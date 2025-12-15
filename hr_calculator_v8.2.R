# ============================================================================
# HR Calculator - 최종 아키텍처 v10.1 (3-Tier Logging)
#
# v10.0 기반 + "성공", "시스템 실패", "통계 실패" 로그 3단계 분리
#
# 1. 성공 로그: "작업 시도 완료" (시스템 오류가 없었던 모든 작업)
#    - 통계 계산 성공 + 통계 계산 스킵(n=0) + 통계 계산 실패(수렴 실패)
#    - 목적: "이어하기(Resumability)" 기능 보장 (가장 중요)
#
# 2. 시스템 실패 로그: "심각한 오류"
#    - DuckDB I/O 오류, 코드 버그(object not found), 배치 쓰기 트랜잭션 실패 등
#    - 목적: 즉시 수정이 필요한 코드/환경 문제 디버깅
#
# 3. 통계 실패 로그: "예상된 오류"
#    - coxph/crr 모델 수렴 실패, n수 부족, Lapack singular 오류 등
#    - 목적: 데이터 특성 프로파일링 (많이 쌓여도 정상)
# ============================================================================
# ============================================================================
# HR Calculator - 최종 아키텍처 v10.0 
# (DuckDB I/O + Job Queue 전략 + 워커별 로그 Queue 분리(v6에서는 성공의 경우는 중앙, 실패는 분리)) + '쓰기 일괄 처리(Write Batching)' 추가 + 트랜잭션 통합(Transactional Integration)
# + 자동 체크포인트 루프 구현(Checkpoint Loop)
# ============================================================================
#
# 이 스크립트는 v5/v7의 RAM 병목(8일 ETA) 문제를 해결하고,
# v4.0의 빠른 DuckDB I/O 속도(3일 ETA)와 v7.0.1의 견고성(이어하기, 로그)을
# 결합하여 서버의 모든 코어를 활용하도록 설계되었습니다.
#
# --- 핵심 아키텍처: 'DuckDB + 분산 버퍼링' 병렬 처리 ---
#
# 1. 'DuckDB I/O' (v4.0의 속도):
#    - RAM 조인 방식 대신, v4.0의 DuckDB 디스크 조인 방식을 사용합니다.
#    - 워커(Worker)는 5.0G~8.1G의 'RES' 메모리를 점유하지 않습니다.
#    - 'RAM 병목'이 사라지고, 요청한 'n_cores' (30~90개)를 모두 활용할 수 있습니다.
#
# 2. 'RAM as Cache' (효율적인 메모리 사용):
#    - 128G RAM은 워커의 'RES'가 아닌, OS의 'buff/cache' (디스크 캐시)로 사용됩니다.
#    - 90개 워커가 outcome_table.parquet에 동시 접근하면, OS가 이 파일을
#      RAM 캐시에 올려두어 DuckDB가 사실상 RAM 속도로 I/O를 수행합니다.
#
# 3. 'Job Queue' 기반 견고성 (v5.0의 안정성):
#    - 중앙 'completed_jobs.duckdb'와 'system_failed_jobs.duckdb'를 통해 '이어하기'를 완벽하게 지원합니다.
#
# 4. '하이브리드 로그' (v5.0의 안정성):
#    - (기존 IO 병목 요소소): 중앙 'completed_jobs.duckdb'에 즉시 기록 (이어하기 보장)
#    - (성공 로그): 워커별 'completed_jobs_PID.duckdb'에 즉시 기록 
#       -> 병렬처리 시작시, 청크들을 모두 모아 중앙 기록 후 청크 모두 삭제 방식 진행 (이어하기 보장 + IO 병목 해결)
#    - (실패 로그): 분산 'system_failed_chunk_PID.duckdb'에 기록 (잠금 충돌 방지)
#    - (결과 버퍼링): 분산 'hr/map_chunk_PID.duckdb'에 기록 (Small File Problem 해결)
#
# 5. 안정적인 'multisession' (v4.0):
#    - RAM 공유(COW)가 필요 없으므로, 안정적인 'multisession'을 사용합니다.
#
# 6.  잦은 디스크 쓰기로 인한 파일 시스템 병목 (22 M/s) 해결 목표
#    - 워커가 batch_size 만큼 결과를 RAM에 모았다가 한 번에 디스크 청크에 기록
#    - 성능(3일 ETA 단축)과 안정성(이어하기, N-1개 유실 허용)의 균형
#
# 7. 트랜잭션 통합(Transactional Integration):
#    - 모든 쓰기 작업을 트랜잭션으로 묶어서, 부분 작업 실패 시 전체 작업 롤백 가능
#    - 디스크 쓰기 횟수를 최소화하여, 파일 시스템 병목 감소
# 8. 자동 체크포인트 루프 구현(Checkpoint Loop):
#    - 최종 함수가 그 함수자체로 메모리에 무언가를 계속 누적시키는 코드는 전혀 없으나, 메모리 캐싱이나 장시간 처리작업으로 메모리 단편화 -> 결국 기존 파라미터들 조절로는 중간에 무조건 함수가 interrupt 됨
#    - 따라서 중간에 체크포인트를 저장하고, 이어하기 시작 시 체크포인트를 읽어와서 이어하기 시작하는 방식으로 해결 -> 예 : "로그 취합 -> 200개 배치 실행 -> 결과 취합 -> 60초 휴식"을 자동으로 반복
# ============================================================================

# conda install -c conda-forge r-tidyverse r-survival r-haven r-broom r-arrow r-tidycmprsk r-data.table r-duckdb

# conda 실패시에만 아래의 것을 시도도
# install.packages("survival")
# install.packages("haven")
# install.packages("dplyr")
# install.packages("tidyverse")
# install.packages("broom")
# install.packages("arrow")
# install.packages("tidycmprsk")
# install.packages("data.table")
# install.packages("duckdb")

# 1. 라이브러리 로드
library(survival)
library(haven)
library(dplyr)
library(tidyverse)
library(broom)
library(arrow)
library(tidycmprsk)
library(glue)      # DuckDB 쿼리를 위해 'glue' 복원
library(future)
library(furrr)
library(future.apply)
library(progressr)
library(data.table)
library(duckdb)
library(DBI)
# library(digest) # 임시 RDS 파일 대신 PID 기반 청크 사용

# ============================================================================
# 1. 데이터 변환 모듈 (Data Conversion Modules) + sas 파일 parquet 화
# ============================================================================

# ============================================================================
# 2. 헬퍼 함수 정의 (Helper Functions) - HR/SHR 분석을 수행하는 함수
# ============================================================================

# HR 분석 함수 (모듈화)
perform_hr_analysis <- function(clean_data, fu, cause_abb, outcome_abb) {
    # Cox 회귀 분석
    fit_coxph <- coxph(Surv(diff, status == 1) ~ case + strata(matched_id), data = clean_data)
    
    res_log_hr <- tidy(fit_coxph)
    res_hr <- tidy(fit_coxph, exponentiate = TRUE, conf.int = TRUE)
    
    # Cox 회귀 결과 정리
    full_coxph_results <- res_log_hr %>%
        select(std.error, statistic, p.value, estimate) %>%
        rename(
            log_hr_values = estimate,
            hr_p_values = p.value,
            log_hr_std = std.error,
            log_hr_z_values = statistic
        ) %>%
        mutate(
            fu = fu,
            cause_abb = cause_abb,
            outcome_abb = outcome_abb,
            hr_values = res_hr$estimate,
            hr_lower_cis = res_hr$conf.low,
            hr_upper_cis = res_hr$conf.high
        ) %>%
        select(
            fu, cause_abb, outcome_abb, hr_values, hr_lower_cis, hr_upper_cis, 
            log_hr_values, hr_p_values, log_hr_std, log_hr_z_values
        )
    
    # [수정] 경쟁위험 분석을 위한 데이터 "수정" (복사 방지)
    clean_data[, status_factor := factor(
            status,
            levels = 0:2, 
            labels = c("censor", "outcome", "death")
        )
    ]
    
    # 경쟁위험 분석
    fit_crr <- crr(Surv(diff, status_factor) ~ case, data = clean_data)
    
    res_log_shr <- tidy(fit_crr)
    res_shr <- tidy(fit_crr, exponentiate = TRUE, conf.int = TRUE)
    
    # 경쟁위험 분석 결과 정리
    full_crr_results <- res_log_shr %>%
        select(std.error, statistic, p.value, estimate) %>%
        rename(
            log_shr_values = estimate,
            shr_p_values = p.value,
            log_shr_std = std.error,
            log_shr_z_values = statistic
        ) %>%
        mutate(
            shr_values = res_shr$estimate,
            shr_lower_cis = res_shr$conf.low,
            shr_upper_cis = res_shr$conf.high
        ) %>%
        select(
            shr_values, shr_lower_cis, shr_upper_cis, log_shr_values, 
            shr_p_values, log_shr_std, log_shr_z_values
        )
    
    # 최종 결과 반환
    return(bind_cols(full_coxph_results, full_crr_results))
}

# ============================================================================
# 3단계: [v9.3 신규] 배치 처리 모듈 (Batch Worker)
# ============================================================================

# N개의 작업을 처리하고 결과를 RAM에 모았다가 한 번에 디스크에 쓰는 함수
process_batch <- function(
    batch_jobs, # data.table: 처리할 작업 목록 (N개 행)
    fu,
    matched_parquet_folder_path,
    outcome_parquet_file_path,
    results_hr_folder_path,
    results_mapping_folder_path,
    db_completed_folder_path, # 성공 로그 청크 폴더
    db_system_failed_folder_path, # 시스템 실패 로그 청크 폴더
    db_stat_failed_folder_path # 통계 실패 로그 청크 폴더
) {
    # 워커별 프로그레스 바 설정 강제 활성화 (future multisession은 별도 프로세스이므로 옵션 전달 필요)
    # handlers는 이미 메인에서 설정했으므로 옵션만 설정 (중복 호출 시 "handlers on the stack" 에러 발생)
    if (!isTRUE(getOption("progressr.enable"))) {
        options(progressr.enable = TRUE)
    }
    
    # 워커별 청크 파일 경로 설정 (v7.txt 방식)
    worker_pid <- Sys.getpid()
    db_hr_chunk_path <- file.path(results_hr_folder_path, sprintf("hr_chunk_%s.duckdb", worker_pid))
    db_map_chunk_path <- file.path(results_mapping_folder_path, sprintf("map_chunk_%s.duckdb", worker_pid))
    db_completed_chunk_path <- file.path(db_completed_folder_path, sprintf("completed_chunk_%s.duckdb", worker_pid))
    db_system_failed_chunk_path <- file.path(db_system_failed_folder_path, sprintf("system_failed_chunk_%s.duckdb", worker_pid))
    db_stat_failed_chunk_path <- file.path(db_stat_failed_folder_path, sprintf("stat_failed_chunk_%s.duckdb", worker_pid))

    # RAM에 결과를 모을 임시 리스트 초기화
    batch_hr_results <- list()
    batch_edge_pids <- list()
    batch_edge_index <- list()
    batch_edge_key <- list()
    batch_completed_jobs <- list()
    batch_system_failed_jobs <- list()
    batch_stat_failed_jobs <- list()

    # 배치 내 각 작업 처리 (기존 process_one_pair 로직 재활용)
    # 워커별 프로그레스 바 제거: 멀티프로세싱 환경에서 여러 워커가 동시에 출력하면 떨림 현상 발생
    # 메인 프로그레스 바(배치 진행 상황)만 표시
    for (i in 1:nrow(batch_jobs)) {
        current_job <- batch_jobs[i, ]
        cause_abb <- current_job$cause_abb
        outcome_abb <- current_job$outcome_abb

        tryCatch({
            # --- 1. DuckDB 쿼리 (v7.txt process_one_pair) ---
            con_duck <- dbConnect(duckdb::duckdb())
            matched_parquet_file_path <- file.path(matched_parquet_folder_path, sprintf("matched_%s.parquet", tolower(cause_abb)))
            query <- glue::glue("
                SELECT m.*, o.recu_fr_dt, o.abb_sick, o.key_seq
                FROM read_parquet('{matched_parquet_file_path}') AS m
                LEFT JOIN (
                    SELECT person_id, recu_fr_dt, abb_sick, key_seq 
                    FROM read_parquet('{outcome_parquet_file_path}') 
                    WHERE abb_sick = '{outcome_abb}'
                ) AS o ON m.person_id = o.person_id
            ")
            clean_data <- as.data.table(dbGetQuery(con_duck, query))
            dbDisconnect(con_duck, shutdown = TRUE)

            # --- 2. 전처리 (v7.txt process_one_pair) ---
            clean_data[, `:=`(index_date=as.IDate(index_date, "%Y%m%d"), death_date=as.IDate(paste0(dth_ym,"15"), "%Y%m%d"), end_date=as.IDate(paste0(2003+fu,"1231"),"%Y%m%d"), event_date=as.IDate(recu_fr_dt,"%Y%m%d"))]
            clean_data[, final_date := fifelse(!is.na(event_date), pmin(event_date, end_date, na.rm = TRUE), pmin(death_date, end_date, na.rm = TRUE))]
            clean_data[, status := fifelse(!is.na(event_date), fifelse(event_date <= final_date, 1, 0), fifelse(!is.na(death_date) & death_date <= final_date, 2, 0))]
            clean_data[, diff := final_date - index_date]
            problem_ids <- clean_data[diff < 0, unique(matched_id)]
            if (length(problem_ids) > 0) clean_data <- clean_data[!matched_id %in% problem_ids]

            # --- 3. HR 분석 ---
            hr_result <- NULL # 결과 초기화
            if(nrow(clean_data) > 0) {
                
                # [v10.1 신규] 안쪽 tryCatch (통계 오류 감지용)
                tryCatch({
                    
                    hr_result <- perform_hr_analysis(clean_data, fu, cause_abb, outcome_abb)
                    
                    # NULL 반환 케이스 처리
                    # [해결] R 스코프 규칙: error 핸들러나 함수 내부에서 부모 스코프 변수를 수정하려면 <<- 사용 필수
                    # <- 는 로컬 할당만 하여 부모 스코프의 batch_stat_failed_jobs가 수정되지 않음
                    if (is.null(hr_result)) {
                        batch_stat_failed_jobs[[length(batch_stat_failed_jobs) + 1]] <<- data.frame(
                            cause_abb = cause_abb, outcome_abb = outcome_abb, fu = fu,
                            error_msg = "perform_hr_analysis returned NULL unexpectedly", timestamp = Sys.time()
                        )
                    }
                    
                }, error = function(stat_e) {
                    # --- [신규] 3. 통계 계산 실패 로그 기록 ---
                    # [해결] R 스코프 규칙: error 핸들러는 새 스코프를 생성하므로 <<- 사용 필수
                    # <- 로 할당 시 로컬 변수만 생성되어 부모 스코프의 batch_stat_failed_jobs가 수정되지 않음
                    batch_stat_failed_jobs[[length(batch_stat_failed_jobs) + 1]] <<- data.frame(
                        cause_abb = cause_abb, outcome_abb = outcome_abb, fu = fu,
                        error_msg = as.character(stat_e$message), timestamp = Sys.time()
                    )
                }) # end inner tryCatch
            } else {
                # --- [수정] n=0 스킵 케이스도 통계 실패 로그에 기록 ---
                # (데이터가 없어서 통계 분석을 수행할 수 없는 경우)
                # [해결] R 스코프 규칙: if-else 블록 내부에서도 부모 스코프 변수 수정 시 <<- 사용
                batch_stat_failed_jobs[[length(batch_stat_failed_jobs) + 1]] <<- data.frame(
                    cause_abb = cause_abb, outcome_abb = outcome_abb, fu = fu,
                    error_msg = "n=0: No matched data available for analysis", timestamp = Sys.time()
                )
            }
            
            # --- 4. 결과 RAM 리스트에 추가 (통계 계산 성공 시) ---
            if (!is.null(hr_result)) {
                batch_hr_results[[length(batch_hr_results) + 1]] <- hr_result
                # Mapping 데이터
                key <- paste(cause_abb, outcome_abb, fu, sep = "_")
                pids <- clean_data[case == 1, .(person_id)]
                idx_key <- clean_data[case == 1, .(index_key_seq)]
                out_key <- clean_data[case == 1 & status == 1, .(key_seq)]
                if (nrow(pids) > 0) batch_edge_pids[[length(batch_edge_pids) + 1]] <- data.frame(key = key, person_id = pids$person_id)
                if (nrow(idx_key) > 0) batch_edge_index[[length(batch_edge_index) + 1]] <- data.frame(key = key, index_key_seq = idx_key$index_key_seq)
                if (nrow(out_key) > 0) batch_edge_key[[length(batch_edge_key) + 1]] <- data.frame(key = key, outcome_key_seq = out_key$key_seq)
            }
            # else 블록 제거: hr_result가 NULL인 경우는 이미 위에서 처리됨
            # - n=0 케이스: 258-265라인의 else 블록에서 처리
            # - 에러 발생: 251-257라인의 error 핸들러에서 처리  
            # - perform_hr_analysis가 NULL 반환: 243-249라인에서 처리
            
            # --- [수정] 1. 성공 로그 RAM 리스트에 추가 ---
            # (시스템 오류가 없었으므로 "완료"로 기록)
            batch_completed_jobs[[length(batch_completed_jobs) + 1]] <- data.frame(cause_abb = cause_abb, outcome_abb = outcome_abb, fu = fu)

        }, error = function(sys_e) {
            # --- [수정] 2. 시스템/R 실패 로그 기록 ---
            batch_system_failed_jobs[[length(batch_system_failed_jobs) + 1]] <- data.frame(
                cause_abb = cause_abb, outcome_abb = outcome_abb, fu = fu,
                error_msg = as.character(sys_e$message), timestamp = Sys.time()
            )
        }) # end outer tryCatch
    } # end for loop

    # --- [v10.2 수정] 통계 실패 로그 누락 검증: 성공 로그와 HR 결과 비교 ---
    # 원래 로직이 모든 통계 실패를 기록해야 하므로, 누락이 발견되면 에러로 중단
    if (length(batch_completed_jobs) > 0) {
        completed_df <- bind_rows(batch_completed_jobs)
        completed_keys <- paste(completed_df$cause_abb, completed_df$outcome_abb, completed_df$fu, sep = "_")
        
        # HR 결과 키 추출 (있을 경우)
        hr_keys <- if (length(batch_hr_results) > 0) {
            sapply(batch_hr_results, function(x) paste(x$cause_abb[1], x$outcome_abb[1], x$fu[1], sep = "_"))
        } else {
            character(0)
        }
        
        # 통계 실패 로그 키 추출 (있을 경우)
        stat_fail_keys <- if (length(batch_stat_failed_jobs) > 0) {
            sapply(batch_stat_failed_jobs, function(x) paste(x$cause_abb, x$outcome_abb, x$fu, sep = "_"))
        } else {
            character(0)
        }
        
        # 성공 로그에는 있지만 HR 결과도 통계 실패 로그도 없는 경우
        missing_keys <- setdiff(completed_keys, c(hr_keys, stat_fail_keys))
        if (length(missing_keys) > 0) {
            # 상세 내역 출력
            cat("\n==========================================================\n")
            cat("ERROR: 성공 로그와 HR 결과/통계 실패 로그 불일치 감지\n")
            cat("==========================================================\n")
            cat(sprintf("발견된 누락 조합 수: %d개\n\n", length(missing_keys)))
            
            cat("상세 내역:\n")
            cat("  성공 로그 총 건수:", length(completed_keys), "\n")
            cat("  HR 결과 총 건수:", length(hr_keys), "\n")
            cat("  통계 실패 로그 총 건수:", length(stat_fail_keys), "\n")
            cat("  누락된 조합 수:", length(missing_keys), "\n\n")
            
            cat("누락된 조합 목록 (처음 20개):\n")
            missing_keys_sorted <- sort(missing_keys)
            for (i in 1:min(20, length(missing_keys_sorted))) {
                cat(sprintf("  %d. %s\n", i, missing_keys_sorted[i]))
            }
            if (length(missing_keys) > 20) {
                cat(sprintf("  ... (총 %d개 중 20개만 표시)\n", length(missing_keys)))
            }
            
            # 누락된 조합을 데이터프레임으로 변환하여 출력
            missing_parts <- strsplit(missing_keys, "_")
            missing_df <- data.frame(
                cause_abb = sapply(missing_parts, function(x) x[1]),
                outcome_abb = sapply(missing_parts, function(x) x[2]),
                fu = as.integer(sapply(missing_parts, function(x) x[3]))
            )
            cat("\n누락된 조합 데이터프레임 (처음 10개):\n")
            print(head(missing_df, 10))
            
            cat("\n==========================================================\n")
            cat("원인: 원래 로직에서 통계 실패가 발생했지만 batch_stat_failed_jobs에\n")
            cat("      기록되지 않았거나, 다른 로직 오류가 있는 것으로 보입니다.\n")
            cat("==========================================================\n")
            flush.console()
            
            stop(sprintf(
                "ERROR: 성공 로그에는 있지만 HR 결과와 통계 실패 로그 모두 없는 조합 %d개 발견. 실행을 중단합니다.",
                length(missing_keys)
            ))
        }
    }

    # --- [v10.1 수정] 일괄 쓰기 (트랜잭션 관리) ---
    con_hr <- if (length(batch_hr_results) > 0) dbConnect(duckdb::duckdb(), dbdir = db_hr_chunk_path, read_only = FALSE) else NULL
    con_map <- if (length(batch_edge_pids) > 0 || length(batch_edge_index) > 0 || length(batch_edge_key) > 0) dbConnect(duckdb::duckdb(), dbdir = db_map_chunk_path, read_only = FALSE) else NULL
    con_comp <- if (length(batch_completed_jobs) > 0) dbConnect(duckdb::duckdb(), dbdir = db_completed_chunk_path, read_only = FALSE) else NULL
    con_system_fail <- if (length(batch_system_failed_jobs) > 0) dbConnect(duckdb::duckdb(), dbdir = db_system_failed_chunk_path, read_only = FALSE) else NULL
    con_stat_fail <- if (length(batch_stat_failed_jobs) > 0) dbConnect(duckdb::duckdb(), dbdir = db_stat_failed_chunk_path, read_only = FALSE) else NULL # [신규]

    on.exit({
        if (!is.null(con_hr)) dbDisconnect(con_hr, shutdown = TRUE)
        if (!is.null(con_map)) dbDisconnect(con_map, shutdown = TRUE)
        if (!is.null(con_comp)) dbDisconnect(con_comp, shutdown = TRUE)
        if (!is.null(con_system_fail)) dbDisconnect(con_system_fail, shutdown = TRUE)
        if (!is.null(con_stat_fail)) dbDisconnect(con_stat_fail, shutdown = TRUE)
    })

    # 트랜잭션 시작
    if (!is.null(con_hr)) dbExecute(con_hr, "BEGIN TRANSACTION;")
    if (!is.null(con_map)) dbExecute(con_map, "BEGIN TRANSACTION;")
    if (!is.null(con_comp)) dbExecute(con_comp, "BEGIN TRANSACTION;")
    if (!is.null(con_system_fail)) dbExecute(con_system_fail, "BEGIN TRANSACTION;")
    if (!is.null(con_stat_fail)) dbExecute(con_stat_fail, "BEGIN TRANSACTION;") # [신규]

    tryCatch({
        # 1. 성공 로그 쓰기
        if (!is.null(con_comp)) {
            dbExecute(con_comp, "CREATE TABLE IF NOT EXISTS jobs (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER)")
            dbWriteTable(con_comp, "jobs", bind_rows(batch_completed_jobs), append = TRUE)
        }
        # 2. 시스템 실패 로그 쓰기
        if (!is.null(con_system_fail)) {
            dbExecute(con_system_fail, "CREATE TABLE IF NOT EXISTS system_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME)")
            dbWriteTable(con_system_fail, "system_failures", bind_rows(batch_system_failed_jobs), append = TRUE)
        }
        # 3. 통계 실패 로그 쓰기
        if (!is.null(con_stat_fail)) {
            if (length(batch_stat_failed_jobs) > 0) {
                dbExecute(con_stat_fail, "CREATE TABLE IF NOT EXISTS stat_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME)")
                dbWriteTable(con_stat_fail, "stat_failures", bind_rows(batch_stat_failed_jobs), append = TRUE)
            }
        } else if (length(batch_stat_failed_jobs) > 0) {
            # 통계 실패가 있는데 연결이 NULL인 경우 경고 및 연결 재생성
            warning(sprintf("통계 실패 %d건이 있지만 연결이 NULL입니다. 연결을 재생성합니다.\n", length(batch_stat_failed_jobs)))
            con_stat_fail <- dbConnect(duckdb::duckdb(), dbdir = db_stat_failed_chunk_path, read_only = FALSE)
            dbExecute(con_stat_fail, "BEGIN TRANSACTION;")
            dbExecute(con_stat_fail, "CREATE TABLE IF NOT EXISTS stat_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME)")
            dbWriteTable(con_stat_fail, "stat_failures", bind_rows(batch_stat_failed_jobs), append = TRUE)
            dbExecute(con_stat_fail, "COMMIT;")
        }
        # 결과물 쓰기 (HR, Map)
        if (!is.null(con_hr)) {
            dbExecute(con_hr, "
                CREATE TABLE IF NOT EXISTS hr_results (
                    fu INTEGER, 
                    cause_abb VARCHAR, 
                    outcome_abb VARCHAR, 
                    hr_values DOUBLE, 
                    hr_lower_cis DOUBLE, 
                    hr_upper_cis DOUBLE, 
                    log_hr_values DOUBLE, 
                    hr_p_values DOUBLE, 
                    log_hr_std DOUBLE, 
                    log_hr_z_values DOUBLE,
                    shr_values DOUBLE, 
                    shr_lower_cis DOUBLE, 
                    shr_upper_cis DOUBLE, 
                    log_shr_values DOUBLE, 
                    shr_p_values DOUBLE, 
                    log_shr_std DOUBLE, 
                    log_shr_z_values DOUBLE
                )
            ")
            dbWriteTable(con_hr, "hr_results", bind_rows(batch_hr_results), append = TRUE)
        }
        if (!is.null(con_map)) {
            dbExecute(con_map, "CREATE TABLE IF NOT EXISTS edge_pids (key VARCHAR, person_id BIGINT);")
            dbExecute(con_map, "CREATE TABLE IF NOT EXISTS edge_index_key_seq (key VARCHAR, index_key_seq BIGINT);")
            dbExecute(con_map, "CREATE TABLE IF NOT EXISTS edge_key_seq (key VARCHAR, outcome_key_seq BIGINT);")
            if (length(batch_edge_pids) > 0) dbWriteTable(con_map, "edge_pids", bind_rows(batch_edge_pids), append = TRUE)
            if (length(batch_edge_index) > 0) dbWriteTable(con_map, "edge_index_key_seq", bind_rows(batch_edge_index), append = TRUE)
            if (length(batch_edge_key) > 0) dbWriteTable(con_map, "edge_key_seq", bind_rows(batch_edge_key), append = TRUE)
        }

        # 모든 쓰기 성공 시 커밋
        if (!is.null(con_hr)) dbExecute(con_hr, "COMMIT;")
        if (!is.null(con_map)) dbExecute(con_map, "COMMIT;")
        if (!is.null(con_comp)) dbExecute(con_comp, "COMMIT;")
        if (!is.null(con_system_fail)) dbExecute(con_system_fail, "COMMIT;")
        if (!is.null(con_stat_fail)) dbExecute(con_stat_fail, "COMMIT;")

    }, error = function(e) {
        # 쓰기 중 오류 발생 시 롤백
        warning("Batch write failed, attempting rollback: ", e$message)
        if (!is.null(con_hr)) try(dbExecute(con_hr, "ROLLBACK;"), silent = TRUE)
        if (!is.null(con_map)) try(dbExecute(con_map, "ROLLBACK;"), silent = TRUE)
        if (!is.null(con_comp)) try(dbExecute(con_comp, "ROLLBACK;"), silent = TRUE)
        if (!is.null(con_system_fail)) try(dbExecute(con_system_fail, "ROLLBACK;"), silent = TRUE)
        if (!is.null(con_stat_fail)) try(dbExecute(con_stat_fail, "ROLLBACK;"), silent = TRUE)
        stop(e) # 에러를 다시 던져서 상위 tryCatch가 잡도록 함
    })

    return(TRUE)
}

# ============================================================================
# 4. 메인 실행 함수 (Main Executor)
# ============================================================================

run_hr_analysis <- function(
    cause_list, 
    outcome_list, 
    fu, 
    n_cores,
    batch_size, # [v8.0 신규] 배치 처리 크기
    max_batches_per_run, # [v10.0 신규] 한 번의 실행에서 최대 배치 수
    matched_parquet_folder_path, 
    outcome_parquet_file_path, # v4.0과 동일하게 '파일 경로'
    results_hr_folder_path,
    results_mapping_folder_path,
    db_completed_file_path, # 중앙 성공 로그 경로
    db_completed_folder_path, # 청크 성공 로그 폴더 경로
    db_system_failed_folder_path, # 청크 시스템 실패 로그 경로,
    db_stat_failed_folder_path # 청크 통계 실패 로그 경로,
    ) {
    cat("\n--- [단계 1] 핵심 병렬 분석 시작 ---\n")
    
    # --- 작업 목록 생성 ---
    instruction_list_all <- tidyr::expand_grid(cause_abb = cause_list, outcome_abb = outcome_list) %>%
        filter(cause_abb != outcome_abb) %>%
        mutate(fu = fu)
    
    con_completed <- dbConnect(duckdb::duckdb(), dbdir = db_completed_file_path, read_only = FALSE)
    dbExecute(con_completed, "CREATE TABLE IF NOT EXISTS jobs (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, PRIMARY KEY (cause_abb, outcome_abb, fu))")
    completed_jobs <- as.data.table(dbGetQuery(con_completed, "SELECT * FROM jobs"))
    dbDisconnect(con_completed, shutdown = TRUE)
    
    dt_instruction_all <- as.data.table(instruction_list_all)
    if (nrow(completed_jobs) > 0) {
         jobs_to_do <- dt_instruction_all[!completed_jobs, on = c("cause_abb", "outcome_abb", "fu")]
    } else {
         jobs_to_do <- dt_instruction_all
    }
    
    cat(sprintf("전체 %d개 중, 기완료 %d개 제외, 총 %d개 작업 시작 (Core: %d)\n", 
        nrow(instruction_list_all), nrow(completed_jobs), nrow(jobs_to_do), n_cores))
    
    if (nrow(jobs_to_do) == 0) {
        cat("모든 작업이 이미 완료되었습니다.\n")
        return(0) # [v10.0 수정] 모든 작업이 이미 완료되었을 경우 0 반환
    }
    # --------------------------------

    # --- 병렬 처리 설정 및 실행 ---
    # --- [v8 수정] 작업을 배치로 분할 ---
    total_jobs_to_do_count <- nrow(jobs_to_do) # [수정] 실제 행 수 계산
    num_batches <- ceiling(total_jobs_to_do_count / batch_size) # [수정] 올바른 변수 사용
    job_indices <- 1:total_jobs_to_do_count # [수정] 올바른 변수 사용
    # split 함수를 사용하여 인덱스를 배치 크기만큼 나눔
    batches_indices <- split(job_indices, ceiling(job_indices / batch_size))

    # [v10.0 신규] 이번 실행에서 처리할 배치만 선택
    num_batches_this_run <- min(num_batches, max_batches_per_run)
    batches_indices_this_run <- batches_indices[1:num_batches_this_run]

    cat(sprintf("--- 이번 실행에서 %d개 배치 중 %d개 처리 시작 ---\n", num_batches, num_batches_this_run))

    # --- 병렬 처리 설정 및 실행 (future_lapply 사용) ---
    plan(multisession, workers = n_cores)
    required_packages <- c("data.table", "duckdb", "DBI", "arrow", "survival", "broom", "tidycmprsk", "dplyr", "glue") # digest 제거

    progressr::with_progress({
        p <- progressr::progressor(steps = num_batches_this_run) # 프로그레스 스텝 = 배치 수

        # future_lapply: 각 배치 인덱스 리스트를 process_batch 함수에 전달
        results <- future_lapply(batches_indices_this_run, function(indices) {
            # 현재 배치에 해당하는 작업 데이터프레임 생성
            current_batch_jobs <- jobs_to_do[indices, ]

            # [v10.1 수정] 바깥쪽 tryCatch (시스템/R 실패 감지용)
            tryCatch({
                
                # 배치 처리 함수 호출 (신규 인자 추가)
                process_batch(
                    batch_jobs = current_batch_jobs,
                    fu = fu,
                    matched_parquet_folder_path = matched_parquet_folder_path,
                    outcome_parquet_file_path = outcome_parquet_file_path,
                    results_hr_folder_path = results_hr_folder_path,
                    results_mapping_folder_path = results_mapping_folder_path,
                    db_completed_folder_path = db_completed_folder_path,
                    db_system_failed_folder_path = db_system_failed_folder_path,
                    db_stat_failed_folder_path = db_stat_failed_folder_path # [신규]
                )
                
            }, error = function(e) {
                # [v10.1 수정] "시스템/R 실패" 또는 "일괄 쓰기 실패"를 2번 로그에 기록
                cat(sprintf("\nCRITICAL BATCH ERROR (first job: %s -> %s): %s\n", 
                    current_batch_jobs[1, ]$cause_abb, 
                    current_batch_jobs[1, ]$outcome_abb, 
                    e$message))
                
                worker_pid <- Sys.getpid()
                db_system_failed_chunk_path <- file.path(db_system_failed_folder_path, sprintf("system_failed_chunk_%s.duckdb", worker_pid))
                
                con_system_failed <- dbConnect(duckdb::duckdb(), dbdir = db_system_failed_chunk_path, read_only = FALSE)
                on.exit(dbDisconnect(con_system_failed, shutdown = TRUE))
                
                dbExecute(con_system_failed, "CREATE TABLE IF NOT EXISTS system_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME)")
                
                # 배치 전체 실패를 첫 번째 작업 기준으로 기록
                dbWriteTable(
                    con_system_failed, "system_failures", 
                    data.frame(
                        cause_abb = current_batch_jobs[1, ]$cause_abb, 
                        outcome_abb = current_batch_jobs[1, ]$outcome_abb, 
                        fu = fu, 
                        error_msg = as.character(e$message),
                        timestamp = Sys.time()
                    ), 
                    append = TRUE
                )
            })
            p()
            return(TRUE)
        }, future.seed = TRUE, future.packages = required_packages)
    })

    plan(sequential)
    # cat("\n--- [단계 1] 핵심 병렬 배치 분석 완료 ---\n")
    cat(sprintf("\n--- [단계 1] %d개 배치 분석 완료 ---\n", num_batches_this_run))

    # [v10.0 신규] 남은 작업 수 반환
    return(total_jobs_to_do_count - length(unlist(batches_indices_this_run)))
}

# ============================================================================
# 5. 데이터 취합 함수 (Data Aggregator)
# ============================================================================
# R 리스트를 Parquet으로 저장하는 재사용 가능한 헬퍼 함수
save_mapping_to_parquet <- function(mapping_list, type, output_dir, fu) {
    if (length(mapping_list) == 0) {
        cat(sprintf("   - '%s' 매핑 데이터가 없어 건너뜁니다.\n", type))
        return()
    }
    
    cat(sprintf("   - '%s' 매핑 저장 중...\n", type))
    
    # 리스트를 key-value 데이터프레임으로 변환
    df <- data.frame(
        key = names(mapping_list),
        stringsAsFactors = FALSE
    )
    df$values <- I(mapping_list) # 리스트 구조를 유지하며 컬럼에 삽입
    
    # Parquet 파일로 저장
    parquet_file <- file.path(output_dir, sprintf("%s_mapping_%d.parquet", type, fu))
    arrow::write_parquet(df, parquet_file)
    
    cat(sprintf("     ✓ 완료: %s\n", basename(parquet_file)))
}

aggregate_results_mappings <- function(
    cause_list,
    fu, 
    matched_parquet_folder_path, 
    results_hr_folder_path,
    results_mapping_folder_path
    ) {
cat("\n--- [단계 2] 최종 '결과물' 취합 시작 ---\n")
        # --- 1. HR 결과 취합 ---
        cat("1. HR 결과(DuckDB 청크) 취합 중...\n")
        hr_chunk_files <- list.files(results_hr_folder_path, pattern="hr_chunk_.*\\.duckdb", full.names=TRUE)

        if (length(hr_chunk_files) > 0) {
            con_agg <- dbConnect(duckdb::duckdb()); on.exit(dbDisconnect(con_agg, shutdown=TRUE), add=TRUE)
            # 각 DuckDB 파일을 ATTACH하여 UNION ALL로 결합
            all_hr_data <- list()
            for (i in 1:length(hr_chunk_files)) {
                tryCatch({
                    dbExecute(con_agg, sprintf("ATTACH '%s' AS db%d (READ_ONLY)", hr_chunk_files[i], i))
                    chunk_data <- dbGetQuery(con_agg, sprintf("SELECT * FROM db%d.hr_results", i))
                    if (nrow(chunk_data) > 0) {
                        all_hr_data[[length(all_hr_data) + 1]] <- chunk_data
                    }
                    dbExecute(con_agg, sprintf("DETACH db%d", i))
                }, error = function(e) {
                    cat(sprintf("경고: HR 청크 파일 읽기 실패 %s: %s\n", hr_chunk_files[i], e$message))
                })
            }
            if (length(all_hr_data) > 0) {
                new_hr_table <- distinct(bind_rows(all_hr_data))
                final_hr_path <- file.path(results_hr_folder_path, sprintf("total_hr_results_%d.parquet", fu))
                
                # 기존 parquet 파일이 있으면 읽어서 병합 (누적 저장)
                if (file.exists(final_hr_path)) {
                    tryCatch({
                        existing_hr_table <- arrow::read_parquet(final_hr_path)
                        final_hr_table <- distinct(bind_rows(existing_hr_table, new_hr_table))
                        existing_count <- nrow(existing_hr_table)
                        new_count <- nrow(new_hr_table)
                        final_count <- nrow(final_hr_table)
                        cat(sprintf("     → 기존: %d 건, 새로 추가: %d 건, 최종: %d 건\n",
                                   existing_count, new_count, final_count))
                    }, error = function(e) {
                        cat(sprintf("     경고: 기존 parquet 파일 읽기 실패, 새 데이터로 덮어씀: %s\n", e$message))
                        final_hr_table <- new_hr_table
                    })
                } else {
                    final_hr_table <- new_hr_table
                }
                
                arrow::write_parquet(final_hr_table, final_hr_path)
                cat(sprintf("     ✓ HR 결과 취합 완료: %s (총 %d 건)\n", basename(final_hr_path), nrow(final_hr_table)))
                rm(all_hr_data, new_hr_table, final_hr_table); gc()
            } else {
                cat("   - 유효한 HR 데이터 없음.\n")
            }
        } else { cat("   - 취합할 HR 청크 파일 없음.\n") }

        # --- 2. Node 매핑 데이터 생성 (v4.0과 동일) ---
        cat("\n2. Node 매핑 데이터 생성 중...\n")
        node_pids_list <- list()
        node_index_key_seq_list <- list()
        
        for (cause_abb in cause_list) {
            key <- paste(cause_abb, fu, sep = "_")
            matched_path <- file.path(matched_parquet_folder_path, sprintf("matched_%s.parquet", tolower(cause_abb)))
            if (file.exists(matched_path)) {
                matched_data <- arrow::read_parquet(matched_path, col_select = c("person_id", "index_key_seq", "case"))
                node_pids_list[[key]] <- matched_data$person_id
                node_index_key_seq_list[[key]] <- matched_data$index_key_seq[matched_data$case == 1]
            }
        }
        save_mapping_to_parquet(node_pids_list, "node_pids", results_mapping_folder_path, fu)
        save_mapping_to_parquet(node_index_key_seq_list, "node_index_key_seq", results_mapping_folder_path, fu)
        rm(node_pids_list, node_index_key_seq_list); gc()

        # --- 3. Edge 매핑 데이터 취합 ---
        cat("\n3. Edge 매핑 데이터(DuckDB 청크) 취합 중...\n")
        map_chunk_files <- list.files(results_mapping_folder_path, pattern="map_chunk_.*\\.duckdb", full.names=TRUE)

        if (length(map_chunk_files) > 0) {
            con_map_agg <- dbConnect(duckdb::duckdb()); on.exit(dbDisconnect(con_map_agg, shutdown=TRUE), add=TRUE)
            # 각 DuckDB 파일을 ATTACH하여 데이터 취합
            all_edge_pids <- list()
            all_edge_index <- list()
            all_edge_key <- list()
            
            for (i in 1:length(map_chunk_files)) {
                tryCatch({
                    dbExecute(con_map_agg, sprintf("ATTACH '%s' AS db%d (READ_ONLY)", map_chunk_files[i], i))
                    
                    # 각 테이블에서 데이터 읽기 (테이블이 없으면 자동으로 에러 처리됨)
                    tryCatch({
                        pids_data <- dbGetQuery(con_map_agg, sprintf("SELECT * FROM db%d.edge_pids", i))
                        if (nrow(pids_data) > 0) all_edge_pids[[length(all_edge_pids) + 1]] <- pids_data
                    }, error = function(e) {}) # 테이블이 없으면 무시
                    
                    tryCatch({
                        index_data <- dbGetQuery(con_map_agg, sprintf("SELECT * FROM db%d.edge_index_key_seq", i))
                        if (nrow(index_data) > 0) all_edge_index[[length(all_edge_index) + 1]] <- index_data
                    }, error = function(e) {}) # 테이블이 없으면 무시
                    
                    tryCatch({
                        key_data <- dbGetQuery(con_map_agg, sprintf("SELECT * FROM db%d.edge_key_seq", i))
                        if (nrow(key_data) > 0) all_edge_key[[length(all_edge_key) + 1]] <- key_data
                    }, error = function(e) {}) # 테이블이 없으면 무시
                    
                    dbExecute(con_map_agg, sprintf("DETACH db%d", i))
                }, error = function(e) {
                    cat(sprintf("경고: 매핑 청크 파일 읽기 실패 %s: %s\n", map_chunk_files[i], e$message))
                })
            }
            
            # 데이터 결합 및 그룹화
            if (length(all_edge_pids) > 0) {
                edge_pids_combined <- distinct(bind_rows(all_edge_pids))
                edge_pids_df <- edge_pids_combined %>%
                    group_by(key) %>%
                    summarise(values = list(person_id), .groups = 'drop')
                edge_pids_list <- setNames(edge_pids_df$values, edge_pids_df$key)
                save_mapping_to_parquet(edge_pids_list, "edge_pids", results_mapping_folder_path, fu)
            }
            
            if (length(all_edge_index) > 0) {
                edge_index_combined <- distinct(bind_rows(all_edge_index))
                edge_index_df <- edge_index_combined %>%
                    group_by(key) %>%
                    summarise(values = list(index_key_seq), .groups = 'drop')
                edge_index_key_seq_list <- setNames(edge_index_df$values, edge_index_df$key)
                save_mapping_to_parquet(edge_index_key_seq_list, "edge_index_key_seq", results_mapping_folder_path, fu)
            }
            
            if (length(all_edge_key) > 0) {
                edge_key_combined <- distinct(bind_rows(all_edge_key))
                edge_key_df <- edge_key_combined %>%
                    group_by(key) %>%
                    summarise(values = list(outcome_key_seq), .groups = 'drop')
                edge_key_seq_list <- setNames(edge_key_df$values, edge_key_df$key)
                save_mapping_to_parquet(edge_key_seq_list, "edge_key_seq", results_mapping_folder_path, fu)
            }
            
            rm(all_edge_pids, all_edge_index, all_edge_key, 
               edge_pids_combined, edge_index_combined, edge_key_combined,
               edge_pids_df, edge_index_df, edge_key_df,
               edge_pids_list, edge_index_key_seq_list, edge_key_seq_list); gc()
        } else { cat("   - 취합할 Edge 청크 파일 없음.\n") }

        # --- 4. HR/Map 임시 청크 파일 삭제 ---
        cat("\n4. HR/Map 임시 청크 파일 삭제 중...\n")
        suppressWarnings(file.remove(hr_chunk_files))
        suppressWarnings(file.remove(map_chunk_files))

        cat("--- [단계 2] 최종 '결과물' 취합 완료 ---\n")
}

# ============================================================================
# 6. 스크립트 실행 (Script Execution)
# ============================================================================

# 질병 코드 목록을 가져오는 유틸리티 함수
get_disease_codes_from_path <- function(matched_parquet_folder_path) {
    codes <- toupper(gsub("matched_(.*)\\.parquet", "\\1", list.files(matched_parquet_folder_path)))
    return(sort(codes))
}

paths <- list(
        matched_sas_folder = "/home/hashjamm/project_data/disease_network/sas_files/hr_project/matched_date/",
        matched_parquet_folder = "/home/hashjamm/project_data/disease_network/matched_date_parquet/",
        outcome_sas_file = "/home/hashjamm/project_data/disease_network/sas_files/hr_project/hr_std_pop10.sas7bdat",
        outcome_parquet_file = "/home/hashjamm/project_data/disease_network/outcome_table.parquet",
        results_hr_folder = "/home/hashjamm/results/disease_network/hr_results_v10/",
        results_mapping_folder = "/home/hashjamm/results/disease_network/hr_mapping_results_v10/",
        db_completed_folder = "/home/hashjamm/results/disease_network/hr_job_queue_db_v10/completed_jobs/",
        db_completed_file = "/home/hashjamm/results/disease_network/hr_job_queue_db_v10/completed_jobs.duckdb",
        db_system_failed_folder = "/home/hashjamm/results/disease_network/hr_job_queue_db_v10/system_failed_jobs/",
        db_system_failed_file = "/home/hashjamm/results/disease_network/hr_job_queue_db_v10/system_failed_jobs.duckdb",
        db_stat_failed_folder = "/home/hashjamm/results/disease_network/hr_job_queue_db_v10/stat_failed_jobs/",
        db_stat_failed_file = "/home/hashjamm/results/disease_network/hr_job_queue_db_v10/stat_failed_jobs.duckdb"
    )

# progressr 핸들러 설정: 터미널 출력 강제 활성화 (tee 리다이렉션과 호환)
progressr::handlers(global = TRUE)
options(progressr.enable = TRUE)  # 프로그레스 바 강제 활성화
handlers(handler_progress(format = "[:bar] :current/:total (:percent) | ETA: :eta"))

# [v7.0 신규] 사전 취합 헬퍼 함수
pre_aggregate_logs <- function(chunk_folder, central_db_path, pattern, table_name, create_sql, silent = FALSE) {
    chunk_files <- list.files(chunk_folder, pattern = pattern, full.names = TRUE)
    
    if (length(chunk_files) > 0) {
        if (!silent) {
            cat(sprintf("--- [사전 취합] 이전 실행의 %s 청크 %d개를 병합합니다... ---\n", table_name, length(chunk_files)))
        }
        
        con_agg <- dbConnect(duckdb::duckdb(), dbdir = central_db_path, read_only = FALSE)
        dbExecute(con_agg, create_sql)
        
        total_inserted <- 0
        successful_files <- 0
        failed_files <- 0
        successfully_processed_files <- character(0)  # 성공한 파일만 삭제하기 위해
        
        for (i in 1:length(chunk_files)) {
            file_basename <- basename(chunk_files[i])
            
            tryCatch({
                dbExecute(con_agg, sprintf("ATTACH '%s' AS db%d (READ_ONLY)", chunk_files[i], i))
                
                before_count <- dbGetQuery(con_agg, sprintf("SELECT COUNT(*) FROM %s", table_name))[1, 1]
                dbExecute(con_agg, sprintf("INSERT INTO %s SELECT * FROM db%d.%s ON CONFLICT DO NOTHING", table_name, i, table_name))
                after_count <- dbGetQuery(con_agg, sprintf("SELECT COUNT(*) FROM %s", table_name))[1, 1]
                
                inserted_count <- after_count - before_count
                total_inserted <- total_inserted + inserted_count
                successful_files <- successful_files + 1
                successfully_processed_files <- c(successfully_processed_files, chunk_files[i])  # 성공한 파일 기록
                
                # 중복 키 경고 출력 (stat_failures 제외)
                chunk_count <- dbGetQuery(con_agg, sprintf("SELECT COUNT(*) FROM db%d.%s", i, table_name))[1, 1]
                if (inserted_count < chunk_count && !silent && table_name != "stat_failures") {
                    cat(sprintf("  경고: 청크 %s - 원본 %d건 중 %d건만 삽입됨 (중복 키: %d건)\n", 
                               file_basename, chunk_count, inserted_count, chunk_count - inserted_count))
                }
                
                dbExecute(con_agg, sprintf("DETACH db%d", i))
            }, error = function(e) {
                failed_files <<- failed_files + 1
                if (!silent) {
                    cat(sprintf("경고: 청크 파일 병합 실패 %s: %s\n", file_basename, e$message))
                    cat(sprintf("      불완전하거나 손상된 파일로 판단되어 삭제합니다.\n"))
                    cat(sprintf("      해당 작업은 중앙 DB에 기록되지 않았으므로 다음 사이클에서 자동 재실행됩니다.\n"))
                }
                warning(sprintf("청크 파일 병합 실패 %s: %s (파일 삭제됨)", file_basename, e$message))
                
                # DETACH 시도 (에러 발생 시에도 정리)
                tryCatch({
                    dbExecute(con_agg, sprintf("DETACH IF EXISTS db%d", i))
                }, error = function(e2) {
                    # DETACH 실패는 무시
                })
                
                # 불완전한 파일 삭제 (다음 사이클에서 작업 재실행으로 복구)
                tryCatch({
                    file.remove(chunk_files[i])
                    if (!silent) {
                        cat(sprintf("      ✓ 파일 삭제 완료: %s\n", file_basename))
                    }
                }, error = function(e3) {
                    if (!silent) {
                        cat(sprintf("      ⚠️  파일 삭제 실패 (수동 삭제 필요): %s\n", file_basename))
                    }
                })
            })
        }
        
        dbDisconnect(con_agg, shutdown = TRUE)
        
        # 성공한 파일만 삭제 (실패한 파일은 위에서 이미 삭제됨)
        if (length(successfully_processed_files) > 0) {
            suppressWarnings(file.remove(successfully_processed_files))
            if (!silent) {
                if (failed_files > 0) {
                    cat(sprintf("  ℹ️  성공한 청크 파일 %d개 삭제, 실패한 청크 파일 %d개 삭제 완료\n",
                               length(successfully_processed_files), failed_files))
                } else {
                    cat(sprintf("  ℹ️  청크 파일 %d개 모두 취합 완료 및 삭제\n", length(successfully_processed_files)))
                }
            }
        } else if (failed_files > 0 && !silent) {
            cat(sprintf("  ℹ️  모든 청크 파일 병합 실패. 불완전한 파일 %d개 삭제 완료.\n", failed_files))
            cat(sprintf("      해당 작업들은 다음 사이클에서 자동 재실행됩니다.\n"))
        }
        
        if (!silent) {
            cat(sprintf("--- [사전 취합] %s 병합 완료 (총 %d건 삽입) ---\n", table_name, total_inserted))
        }
    } else {
        if (!silent) {
            cat(sprintf("--- [사전 취합] %s 청크 파일 없음 (이미 취합되었거나 기록되지 않음) ---\n", table_name))
        }
    }
}

# 메인 실행 함수 (v10.0 - 자동 체크포인트 + 예열 옵션 적용)
main <- function(
    paths = paths, 
    fu, 
    n_cores = 15, 
    batch_size = 500, 
    max_batches_per_run = 200,  # [v10.3] 한 실행당 처리할 배치 수 (이후 프로세스 종료)
    warming_up = FALSE
) {

    total_start_time <- Sys.time()
    
    # --- [신규] 단계 0: OS 디스크 캐시 '수동 예열' (루프 시작 전 1회) ---
    if (warming_up) {
        cat("\n--- OS 디스크 캐시 예열 시작 ---\n")
        tryCatch({
            cat("공통 데이터(outcome_table)를 1회 읽어 캐시에 올립니다...\n")
            temp_data <- arrow::read_parquet(paths$outcome_parquet_file)
            dplyr::collect(temp_data)
            rm(temp_data)
            gc()
            cat("--- 예열 완료 ---\n\n")
        }, error = function(e) {
            cat(sprintf("경고: OS 예열 실패: %s\n", e$message))
        })
    }
    
    # [v10.3 수정] 메모리 파편화 방지를 위해 단일 사이클만 실행 후 프로세스 종료
    # 외부에서 재실행하면 자동으로 이어서 진행됨
    cat("\n==========================================================\n")
    cat(sprintf("--- %s: 새 작업 사이클 시작 ---\n", Sys.time()))
    
    # --- 단계 1: 사전 로그 취합 (이어하기 준비) ---
    pre_aggregate_logs( 
        chunk_folder = paths$db_completed_folder,
        central_db_path = paths$db_completed_file, 
        pattern = "completed_chunk_.*\\.duckdb",
        table_name = "jobs",
        create_sql = "CREATE TABLE IF NOT EXISTS jobs (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, PRIMARY KEY (cause_abb, outcome_abb, fu))"
    )
    pre_aggregate_logs(
        chunk_folder = paths$db_system_failed_folder,
        central_db_path = paths$db_system_failed_file,
        pattern = "system_failed_chunk_.*\\.duckdb",
        table_name = "system_failures",
        create_sql = "CREATE TABLE IF NOT EXISTS system_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME, PRIMARY KEY (cause_abb, outcome_abb, fu, error_msg))"
    )
    # 3. [신규] 통계 실패 로그 취합
    pre_aggregate_logs(
        chunk_folder = paths$db_stat_failed_folder,
        central_db_path = paths$db_stat_failed_file,
        pattern = "stat_failed_chunk_.*\\.duckdb",
        table_name = "stat_failures",
        create_sql = "CREATE TABLE IF NOT EXISTS stat_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME, PRIMARY KEY (cause_abb, outcome_abb, fu, error_msg))"
    )
    # -----------------------------------------

    cat("분석 대상 질병 코드 로드 중...\n")
    disease_codes <- get_disease_codes_from_path(paths$matched_parquet_folder)
    cat(sprintf("   - %d개 유효 코드 확인.\n", length(disease_codes)))

    # --- 단계 2: 핵심 병렬 분석 실행 (N개 배치만) ---
    # run_hr_analysis는 남은 작업 수를 반환
    remaining_jobs <- run_hr_analysis(
        cause_list = disease_codes, 
        outcome_list = disease_codes, 
        fu = fu, 
        n_cores = n_cores, 
        batch_size = batch_size,
        max_batches_per_run = max_batches_per_run, # [v9.4]
        matched_parquet_folder_path = paths$matched_parquet_folder,
        outcome_parquet_file_path = paths$outcome_parquet_file,
        results_hr_folder_path = paths$results_hr_folder,
        results_mapping_folder_path = paths$results_mapping_folder,
        db_completed_file_path = paths$db_completed_file, # '읽기'용
        db_completed_folder_path = paths$db_completed_folder, # '쓰기'용
        db_system_failed_folder_path = paths$db_system_failed_folder, # '쓰기'용
        db_stat_failed_folder_path = paths$db_stat_failed_folder # '쓰기'용
    )

    # --- 단계 3: '결과물' 취합 ---
    aggregate_results_mappings(
        cause_list = disease_codes, 
        fu = fu,
        matched_parquet_folder_path = paths$matched_parquet_folder,
        results_hr_folder_path = paths$results_hr_folder,
        results_mapping_folder_path = paths$results_mapping_folder
    )

    # --- 단계 4: 종료 시 '로그' 취합 (다음 사이클 준비) ---
    cat("\n--- [단계 4] 이번 사이클 로그 취합 시작 ---\n")
    pre_aggregate_logs( # 완료 로그
        chunk_folder = paths$db_completed_folder,
        central_db_path = paths$db_completed_file,
        pattern = "completed_chunk_.*\\.duckdb",
        table_name = "jobs",
        create_sql = "CREATE TABLE IF NOT EXISTS jobs (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, PRIMARY KEY (cause_abb, outcome_abb, fu))"
    )
    pre_aggregate_logs( # 시스템 실패 로그
        chunk_folder = paths$db_system_failed_folder,
        central_db_path = paths$db_system_failed_file,
        pattern = "system_failed_chunk_.*\\.duckdb",
        table_name = "system_failures",
        create_sql = "CREATE TABLE IF NOT EXISTS system_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME, PRIMARY KEY (cause_abb, outcome_abb, fu, error_msg))"
    )
    pre_aggregate_logs( # 통계 실패 로그
        chunk_folder = paths$db_stat_failed_folder,
        central_db_path = paths$db_stat_failed_file,
        pattern = "stat_failed_chunk_.*\\.duckdb",
        table_name = "stat_failures",
        create_sql = "CREATE TABLE IF NOT EXISTS stat_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME, PRIMARY KEY (cause_abb, outcome_abb, fu, error_msg))"
    )
    cat("--- [단계 4] 이번 사이클 로그 취합 완료 ---\n")
    
    # --- [v10.3 수정] 작업 완료 여부 확인 및 프로세스 종료 ---
    if (remaining_jobs == 0) {
        cat("\n--- 모든 배치가 완료되었습니다! ---\n")
        # 모든 작업 완료 시 최종 요약 출력을 위해 return하지 않음
    } else {
        # --- 메모리 파편화 방지를 위한 프로세스 종료 ---
        # max_batches_per_run개 처리 후 완전히 종료하여 메모리 파편화 해소
        # 사용자는 스크립트를 다시 실행하면 자동으로 이어서 진행됨
        cat("\n==========================================================\n")
        cat("--- 메모리 파편화 방지를 위한 프로세스 종료 ---\n")
        cat("==========================================================\n")
        cat(sprintf("이번 사이클에서 %d개 배치 (약 %d개 작업) 처리 완료\n", 
                   max_batches_per_run, max_batches_per_run * batch_size))
        cat(sprintf("남은 작업: %d개\n\n", remaining_jobs))
        cat("체크포인트 정보:\n")
        cat(sprintf("  - 완료된 작업은 중앙 DB에 기록됨\n"))
        cat(sprintf("  - 남은 작업은 다음 실행 시 자동으로 이어서 진행됨\n"))
        cat(sprintf("\n다음 실행 방법:\n"))
        cat(sprintf("  이 스크립트를 다시 실행하면 자동으로 남은 작업을 처리합니다.\n"))
        cat(sprintf("  (동일한 파라미터로 main() 함수 실행)\n\n"))
        cat("==========================================================\n")
        flush.console()
        
        return(invisible(remaining_jobs))  # 함수 종료 (남은 작업 수 반환)
    }
    
    # --- 최종 요약 (모든 작업 완료 시에만 도달) ---
    total_elapsed <- as.numeric(difftime(Sys.time(), total_start_time, units = "hours"))
    cat(sprintf("\n모든 작업 완료! 총 소요 시간: %.2f시간 (%.1f일)\n", total_elapsed, total_elapsed / 24))
    cat(sprintf("HR 결과물 위치: %s\n", file.path(paths$results_hr_folder, sprintf("total_hr_results_%d.parquet", fu))))
    cat(sprintf("매핑 결과물 위치: %s\n", paths$results_mapping_folder))
    cat(sprintf("성공한 작업 로그 (중앙): %s\n", paths$db_completed_file))
    cat(sprintf("시스템 실패 로그 (중앙): %s\n", paths$db_system_failed_file))
    cat(sprintf("통계 실패 로그 (중앙): %s\n", paths$db_stat_failed_file))
}

# --- 메인 실행 ---
result <- main(
    paths = paths, fu = 10, 
    n_cores = 30, # CPU 및 디스크 IO 부하
    batch_size = 1000, # 메모리 부하,
    max_batches_per_run = 100,  # [v10.3] 한 실행당 처리할 배치 수 (이후 프로세스 종료)
    warming_up = FALSE
)

# 로그 불일치 확인
check_log_discrepancy()

# --- 종료 코드 설정 (셸 스크립트용) ---
# result가 0이면 모든 작업 완료, 0보다 크면 남은 작업 있음 (재시작 필요)
if (!is.null(result) && result > 0) {
    quit(save = "no", status = 1)  # 재시작 필요 (셸 스크립트가 계속 실행)
} else {
    quit(save = "no", status = 0)  # 모든 작업 완료
}

#' 중앙 성공 로그 파일 확인
#'
#' @param log_path 'completed_jobs.duckdb' 파일의 전체 경로
#' @param n 반환할 행의 수 (기본 10)
#' @param tail TRUE면 마지막 n개 (최신순), FALSE면 처음 n개 (오래된순)
#'
check_success_log <- function(log_path, n = 10, tail = TRUE) {
    if (!file.exists(log_path)) {
        cat(sprintf("성공 로그 파일이 아직 생성되지 않았습니다: %s\n", log_path))
        return(invisible(NULL))
    }
    
    con <- dbConnect(duckdb::duckdb(), dbdir = log_path, read_only = TRUE)
    on.exit(dbDisconnect(con, shutdown = TRUE)) # 함수 종료 시 항상 연결 해제
    
    if (!"jobs" %in% dbListTables(con)) {
        cat("로그 파일에 'jobs' 테이블이 없습니다.\n")
        return(invisible(NULL))
    }
    
    total_rows <- dbGetQuery(con, "SELECT COUNT(*) FROM jobs")[1, 1]
    
    if (tail) {
        # rowid는 DuckDB의 내부 행 ID. 가장 빠르게 최신순 정렬 가능
        query <- sprintf("SELECT * FROM jobs ORDER BY rowid DESC LIMIT %d", n)
        cat(sprintf("\n--- [성공 로그] 최근 %d개 작업 (총 %s개 완료) ---\n", n, format(total_rows, big.mark = ",")))
    } else {
        query <- sprintf("SELECT * FROM jobs LIMIT %d", n)
        cat(sprintf("\n--- [성공 로그] 최초 %d개 작업 (총 %s개 완료) ---\n", n, format(total_rows, big.mark = ",")))
    }
    
    data <- dbGetQuery(con, query)
    print(data)
    invisible(data)
}

#' 중앙 시스템 실패 로그 파일 확인
#'
#' @param log_path 'system_failed_jobs.duckdb' 파일의 전체 경로
#' @param n 반환할 행의 수 (기본 10)
#' @param tail TRUE면 마지막 n개 (최신순), FALSE면 처음 n개 (오래된순)
#'
check_system_failed_log <- function(log_path, n = 10, tail = TRUE) {
    if (!file.exists(log_path)) {
        cat(sprintf("시스템 실패 로그 파일이 아직 생성되지 않았습니다: %s\n", log_path))
        return(invisible(NULL))
    }
    
    con <- dbConnect(duckdb::duckdb(), dbdir = log_path, read_only = TRUE)
    on.exit(dbDisconnect(con, shutdown = TRUE)) # 함수 종료 시 항상 연결 해제
    
    # [수정] 'failures' 테이블 확인
    if (!"system_failures" %in% dbListTables(con)) {
        cat("로그 파일에 'system_failures' 테이블이 없습니다.\n")
        return(invisible(NULL))
    }
    
    # [수정] 'system_failures' 테이블에서 카운트
    total_rows <- dbGetQuery(con, "SELECT COUNT(*) FROM system_failures")[1, 1]
    
    if (tail) {
        # [수정] rowid 대신 timestamp 기준 정렬
        query <- sprintf("SELECT * FROM system_failures ORDER BY timestamp DESC LIMIT %d", n)
        cat(sprintf("\n--- [시스템 실패 로그] 최근 %d개 오류 (총 %s개 고유 오류) ---\n", n, format(total_rows, big.mark = ",")))
    } else {
        # [수정] rowid 대신 timestamp 기준 정렬
        query <- sprintf("SELECT * FROM system_failures ORDER BY timestamp ASC LIMIT %d", n)
        cat(sprintf("\n--- [시스템 실패 로그] 최초 %d개 오류 (총 %s개 고유 오류) ---\n", n, format(total_rows, big.mark = ",")))
    }
    
    data <- dbGetQuery(con, query)
    print(data)
    invisible(data)
}

#' 중앙 통계 실패 로그 파일 확인
#'
#' @param log_path 'stat_failed_jobs.duckdb' 파일의 전체 경로
#' @param n 반환할 행의 수 (기본 10)
#' @param tail TRUE면 마지막 n개 (최신순), FALSE면 처음 n개 (오래된순)
#'
check_stat_failed_log <- function(log_path, n = 10, tail = TRUE) {
    if (!file.exists(log_path)) {
        cat(sprintf("통계 실패 로그 파일이 아직 생성되지 않았습니다: %s\n", log_path))
        return(invisible(NULL))
    }
    
    con <- dbConnect(duckdb::duckdb(), dbdir = log_path, read_only = TRUE)
    on.exit(dbDisconnect(con, shutdown = TRUE)) # 함수 종료 시 항상 연결 해제
    
    # [수정] 'stat_failures' 테이블 확인
    if (!"stat_failures" %in% dbListTables(con)) {
        cat("로그 파일에 'stat_failures' 테이블이 없습니다.\n")
        return(invisible(NULL))
    }
    
    # [수정] 'stat_failures' 테이블에서 카운트
    total_rows <- dbGetQuery(con, "SELECT COUNT(*) FROM stat_failures")[1, 1]
    
    if (tail) {
        # [수정] rowid 대신 timestamp 기준 정렬
        query <- sprintf("SELECT * FROM stat_failures ORDER BY timestamp DESC LIMIT %d", n)
        cat(sprintf("\n--- [통계 실패 로그] 최근 %d개 오류 (총 %s개 고유 오류) ---\n", n, format(total_rows, big.mark = ",")))
    } else {
        # [수정] rowid 대신 timestamp 기준 정렬
        query <- sprintf("SELECT * FROM stat_failures ORDER BY timestamp ASC LIMIT %d", n)
        cat(sprintf("\n--- [통계 실패 로그] 최초 %d개 오류 (총 %s개 고유 오류) ---\n", n, format(total_rows, big.mark = ",")))
    }
    
    data <- dbGetQuery(con, query)
    print(data)
    invisible(data)
}

#' 개별 워커(PID)의 성공 로그 청크 확인
#'
#' @param log_folder_path completed_jobs 청크 파일들이 저장되는 폴더 경로
#' @param pid 확인할 워커의 PID (숫자 또는 문자열)
#' @param n 반환할 행의 수 (기본 10)
#' @param tail TRUE면 마지막 n개 (최신순), FALSE면 처음 n개 (오래된순)
#'
check_success_chunk <- function(log_folder_path, pid, n = 10, tail = TRUE) {
    # [수정] 파일 이름 패턴 변경
    log_file_path <- file.path(log_folder_path, sprintf("completed_chunk_%s.duckdb", pid))
    
    if (!file.exists(log_file_path)) {
        cat(sprintf("\n--- [성공 로그] PID %s에 대한 성공 로그 청크 없음 ---\n", pid))
        return(invisible(NULL))
    }
    
    con <- dbConnect(duckdb::duckdb(), dbdir = log_file_path, read_only = TRUE)
    on.exit(dbDisconnect(con, shutdown = TRUE))
    
    # [수정] 'jobs' 테이블 확인
    if (!"jobs" %in% dbListTables(con)) {
        cat("로그 파일에 'jobs' 테이블이 없습니다.\n")
        return(invisible(NULL))
    }
    
    # [수정] 'jobs' 테이블에서 카운트
    total_rows <- dbGetQuery(con, "SELECT COUNT(*) FROM jobs")[1, 1]
    
    if (tail) {
        # [수정] timestamp 대신 rowid 기준 정렬
        query <- sprintf("SELECT * FROM jobs ORDER BY rowid DESC LIMIT %d", n)
        cat(sprintf("\n--- [성공 로그] PID %s의 최근 %d개 작업 (총 %d개) ---\n", pid, n, total_rows))
    } else {
        query <- sprintf("SELECT * FROM jobs LIMIT %d", n)
        cat(sprintf("\n--- [성공 로그] PID %s의 최초 %d개 작업 (총 %d개) ---\n", pid, n, total_rows))
    }
    
    data <- dbGetQuery(con, query)
    print(data)
    invisible(data)
}

#' 개별 워커(PID)의 시스템 실패 로그 청크 확인
#'
#' @param log_folder_path system_failed_jobs 청크 파일들이 저장되는 폴더 경로
#' @param pid 확인할 워커의 PID (숫자 또는 문자열)
#' @param n 반환할 행의 수 (기본 10)
#' @param tail TRUE면 마지막 n개 (최신순), FALSE면 처음 n개 (오래된순)
#'
check_system_failed_chunk <- function(log_folder_path, pid, n = 10, tail = TRUE) {
    log_file_path <- file.path(log_folder_path, sprintf("system_failed_chunk_%s.duckdb", pid))
    
    if (!file.exists(log_file_path)) {
        cat(sprintf("\n--- [실패 로그] PID %s에 대한 실패 로그 청크 없음 ---\n", pid))
        return(invisible(NULL))
    }
    
    con <- dbConnect(duckdb::duckdb(), dbdir = log_file_path, read_only = TRUE)
    on.exit(dbDisconnect(con, shutdown = TRUE))
    
    if (!"system_failures" %in% dbListTables(con)) {
        cat("로그 파일에 'system_failures' 테이블이 없습니다.\n")
        return(invisible(NULL))
    }
    
    total_rows <- dbGetQuery(con, "SELECT COUNT(*) FROM system_failures")[1, 1]
    
    if (tail) {
        # timestamp 기준으로 정렬
        query <- sprintf("SELECT * FROM system_failures ORDER BY timestamp DESC LIMIT %d", n)
        cat(sprintf("\n--- [실패 로그] PID %s의 최근 %d개 오류 (총 %d개) ---\n", pid, n, total_rows))
    } else {
        query <- sprintf("SELECT * FROM system_failures ORDER BY timestamp ASC LIMIT %d", n)
        cat(sprintf("\n--- [실패 로그] PID %s의 최초 %d개 오류 (총 %d개) ---\n", pid, n, total_rows))
    }
    
    data <- dbGetQuery(con, query)
    print(data)
    invisible(data)
}

#' 개별 워커(PID)의 통계 실패 로그 청크 확인
#'
#' @param log_folder_path stat_failed_jobs 청크 파일들이 저장되는 폴더 경로
#' @param pid 확인할 워커의 PID (숫자 또는 문자열)
#' @param n 반환할 행의 수 (기본 10)
#' @param tail TRUE면 마지막 n개 (최신순), FALSE면 처음 n개 (오래된순)
#'
check_stat_failed_chunk <- function(log_folder_path, pid, n = 10, tail = TRUE) {
    log_file_path <- file.path(log_folder_path, sprintf("stat_failed_chunk_%s.duckdb", pid))
    
    if (!file.exists(log_file_path)) {
        cat(sprintf("\n--- [통계 실패 로그] PID %s에 대한 통계 실패 로그 청크 없음 ---\n", pid))
        return(invisible(NULL))
    }
    
    con <- dbConnect(duckdb::duckdb(), dbdir = log_file_path, read_only = TRUE)
    on.exit(dbDisconnect(con, shutdown = TRUE))
    
    if (!"stat_failures" %in% dbListTables(con)) {
        cat("로그 파일에 'stat_failures' 테이블이 없습니다.\n")
        return(invisible(NULL))
    }
    
    total_rows <- dbGetQuery(con, "SELECT COUNT(*) FROM stat_failures")[1, 1]
    
    if (tail) {
        # timestamp 기준으로 정렬
        query <- sprintf("SELECT * FROM stat_failures ORDER BY timestamp DESC LIMIT %d", n)
        cat(sprintf("\n--- [통계 실패 로그] PID %s의 최근 %d개 오류 (총 %d개) ---\n", pid, n, total_rows))
    } else {
        query <- sprintf("SELECT * FROM stat_failures ORDER BY timestamp ASC LIMIT %d", n)
        cat(sprintf("\n--- [통계 실패 로그] PID %s의 최초 %d개 오류 (총 %d개) ---\n", pid, n, total_rows))
    }
    
    data <- dbGetQuery(con, query)
    print(data)
    invisible(data)
}

# --- 사용 예시 ---
# check_success_log(paths$db_completed_file)
# check_success_log(paths$db_completed_file, n = 5, tail = FALSE)
# check_system_failed_log(paths$db_system_failed_file)
# check_system_failed_log(paths$db_system_failed_file, n = 5, tail = FALSE)

# (htop에서 오류를 뿜는 R 프로세스의 PID가 97336이라고 가정)
# check_system_failed_chunk(paths$db_system_failed_folder, pid = 97336)
# check_system_failed_chunk(paths$db_system_failed_folder, pid = 97336, n = 3)
# check_success_chunk(paths$db_completed_folder, pid = 97336)
check_success_chunk(paths$db_completed_folder, pid = 9949, n = 5)

check_system_failed_chunk(paths$db_system_failed_folder, pid = 98201)
check_success_log(paths$db_completed_file, n = 5, tail = FALSE)
# --- [성공 로그] 최초 5개 작업 (총 189,166개 완료) ---
#   cause_abb outcome_abb fu
# 1       H62         G52 10
# 2       S92         G02 10
# 3       K28         S30 10
# 4       S49         M70 10
# 5       A87         K20 10

# check_system_failed_log(paths$db_system_failed_file, n = 1000, tail = FALSE)$error_msg %>% unique()

# 실패 로그의 고유 오류 메시지 추출
{con <- dbConnect(duckdb::duckdb(), dbdir = paths$db_system_failed_file, read_only = TRUE); 
 on.exit(dbDisconnect(con, shutdown = TRUE)); 
 dbGetQuery(con, "SELECT DISTINCT error_msg FROM failures")$error_msg}

#' (v10.1+) 로그 불일치 검사 헬퍼 함수
#'
#' @description
#'   별도 파라미터 없이 실행하며, `paths` 객체를 사용하여
#'   (A) "성공 로그"의 총 건수 (중앙 DB + 활성 청크)와
#'   (B) "HR 결과"의 총 행 수 (활성 청크 + 취합된 parquet)를 비교합니다.
#'
#' @details
#'   (A) > (B) 라면, 이는 "정상 스킵" 및 "통계 실패"가 발생했음을 의미합니다.
#'   이때 중앙/청크 통계 실패 로그가 0건이라면, 로그 기록에 버그가 있는 것입니다.
#'   메인 스크립트가 실행되는 동안 *다른 R 세션*에서 실행해야 합니다.
#'   `duckdb`, `DBI`, `glue`, `arrow` 라이브러리가 필요합니다.
#'   
#'   HR 결과는 활성 청크 파일(`hr_chunk_*.duckdb`)과 이미 취합된 파일(`total_hr_results_*.parquet`)
#'   모두를 포함하여 계산합니다. `aggregate_results_mappings`가 실행된 경우 parquet 파일이 생성됩니다.
#'
check_log_discrepancy <- function() {
    
    # --- 필수 객체 및 라이브러리 확인 ---
    if (!exists("paths")) {
        stop("`paths` 객체를 찾을 수 없습니다. R 세션에 'paths' 리스트를 먼저 정의해주세요.")
    }
    libs <- c("duckdb", "DBI", "glue", "arrow")
    if (!all(sapply(libs, requireNamespace, quietly = TRUE))) {
        stop(sprintf("필수 라이브러리가 없습니다. %s 를 로드해주세요.", paste(libs, collapse=", ")))
    }

    cat("==========================================================\n")
    cat("--- [로그 불일치 검사 시작] ---\n")
    cat("(A) 성공 로그 vs (B) HR 결과 로그\n")
    
    total_success_logs <- 0
    total_hr_results <- 0

    # --- 1. (A) 총 "성공 로그" 건수 계산 ---
    cat("\n[1] (A) 총 성공 로그 건수 계산 중...\n")
    tryCatch({
        # 1a. 중앙 DB 건수 (읽기 전용)
        central_success_count <- 0
        con_central <- NULL
        if (file.exists(paths$db_completed_file)) {
            tryCatch({
                con_central <- dbConnect(duckdb::duckdb(), dbdir = paths$db_completed_file, read_only = TRUE)
                if ("jobs" %in% dbListTables(con_central)) {
                    central_success_count <- dbGetQuery(con_central, "SELECT COUNT(*) FROM jobs")[1, 1]
                }
                dbDisconnect(con_central, shutdown = TRUE)
                con_central <- NULL
            }, finally = {
                # 에러 발생 시에도 연결이 반드시 닫히도록 보장
                if (!is.null(con_central)) {
                    try(dbDisconnect(con_central, shutdown = TRUE), silent = TRUE)
                    con_central <- NULL
                }
            })
        }
        
        # 1b. 활성 청크 건수 (읽기 전용)
        success_chunk_files <- list.files(paths$db_completed_folder, pattern="completed_chunk_.*\\.duckdb", full.names=TRUE)
        chunk_success_count <- 0
        if (length(success_chunk_files) > 0) {
            # 각 청크 파일을 개별적으로 열어서 COUNT 합산
            for (chunk_file in success_chunk_files) {
                con_chunk <- NULL
                tryCatch({
                    con_chunk <- dbConnect(duckdb::duckdb(), dbdir = chunk_file, read_only = TRUE)
                    if ("jobs" %in% dbListTables(con_chunk)) {
                        count <- dbGetQuery(con_chunk, "SELECT COUNT(*) FROM jobs")[1, 1]
                        chunk_success_count <- chunk_success_count + count
                    }
                    dbDisconnect(con_chunk, shutdown = TRUE)
                    con_chunk <- NULL
                }, finally = {
                    if (!is.null(con_chunk)) {
                        try(dbDisconnect(con_chunk, shutdown = TRUE), silent = TRUE)
                    }
                })
            }
        }
        
        total_success_logs <- central_success_count + chunk_success_count
        cat(sprintf(" 🟢 (A) 총 성공 로그 (중앙 %s + 청크 %s): %s 건\n", 
                    format(central_success_count, big.mark = ","),
                    format(chunk_success_count, big.mark = ","),
                    format(total_success_logs, big.mark = ",")))

    }, error = function(e) {
        cat(sprintf(" 🟢 (A) 성공 로그 읽기 실패: %s\n", e$message))
    })

    # --- 2. (B) 총 "HR 결과" 행 수 계산 ---
    cat("\n[2] (B) 총 HR 결과 행 수 계산 중...\n")
    tryCatch({
        # 2a. 활성 청크 파일에서 읽기
        hr_chunk_files <- list.files(paths$results_hr_folder, pattern="hr_chunk_.*\\.duckdb", full.names=TRUE)
        hr_chunk_count <- 0
        if (length(hr_chunk_files) > 0) {
            # 각 HR 청크 파일을 개별적으로 열어서 COUNT 합산
            for (hr_chunk_file in hr_chunk_files) {
                con_hr_chunk <- NULL
                tryCatch({
                    con_hr_chunk <- dbConnect(duckdb::duckdb(), dbdir = hr_chunk_file, read_only = TRUE)
                    if ("hr_results" %in% dbListTables(con_hr_chunk)) {
                        count <- dbGetQuery(con_hr_chunk, "SELECT COUNT(*) FROM hr_results")[1, 1]
                        hr_chunk_count <- hr_chunk_count + count
                    }
                    dbDisconnect(con_hr_chunk, shutdown = TRUE)
                    con_hr_chunk <- NULL
                }, finally = {
                    if (!is.null(con_hr_chunk)) {
                        try(dbDisconnect(con_hr_chunk, shutdown = TRUE), silent = TRUE)
                    }
                })
            }
        }
        
        # 2b. 이미 취합된 parquet 파일에서 읽기
        hr_parquet_files <- list.files(paths$results_hr_folder, pattern="total_hr_results_.*\\.parquet", full.names=TRUE)
        hr_parquet_count <- 0
        if (length(hr_parquet_files) > 0) {
            for (hr_parquet_file in hr_parquet_files) {
                tryCatch({
                    # parquet 파일의 행 수 확인 (전체 읽지 않고 메타데이터만)
                    parquet_data <- arrow::open_dataset(hr_parquet_file)
                    count <- nrow(parquet_data)
                    hr_parquet_count <- hr_parquet_count + count
                }, error = function(e) {
                    # 메타데이터 방식이 실패하면 전체 읽기
                    tryCatch({
                        parquet_data <- arrow::read_parquet(hr_parquet_file)
                        hr_parquet_count <<- hr_parquet_count + nrow(parquet_data)
                    }, error = function(e2) {
                        cat(sprintf("경고: HR parquet 파일 읽기 실패 %s: %s\n", hr_parquet_file, e2$message))
                    })
                })
            }
        }
        
        total_hr_results <- hr_chunk_count + hr_parquet_count
        cat(sprintf(" 📁 (B) 총 HR 결과 행 수 (청크 %s + 취합 parquet %s): %s 건\n", 
                   format(hr_chunk_count, big.mark = ","),
                   format(hr_parquet_count, big.mark = ","),
                   format(total_hr_results, big.mark = ",")))
        
    }, error = function(e) {
        cat(sprintf(" 📁 (B) HR 결과 로그 읽기 실패: %s\n", e$message))
    })

    # --- 3. (C) 총 통계 실패 로그 건수 계산 ---
    cat("\n[3] (C) 총 통계 실패 로그 건수 계산 중...\n")
    total_stat_failures <- 0
    tryCatch({
        # 3a. 중앙 DB 건수 (조합 기준으로 카운트 - error_msg는 제외)
        central_stat_fail_count <- 0
        con_central_stat <- NULL
        if (file.exists(paths$db_stat_failed_file)) {
            tryCatch({
                con_central_stat <- dbConnect(duckdb::duckdb(), dbdir = paths$db_stat_failed_file, read_only = TRUE)
                if ("stat_failures" %in% dbListTables(con_central_stat)) {
                    # 조합 기준으로 카운트 (같은 조합에 여러 에러 메시지가 있어도 1건으로 카운트)
                    central_stat_fail_count <- dbGetQuery(con_central_stat, "SELECT COUNT(*) FROM (SELECT DISTINCT cause_abb, outcome_abb, fu FROM stat_failures)")[1, 1]
                }
                dbDisconnect(con_central_stat, shutdown = TRUE)
                con_central_stat <- NULL
            }, finally = {
                if (!is.null(con_central_stat)) {
                    try(dbDisconnect(con_central_stat, shutdown = TRUE), silent = TRUE)
                }
            })
        }
        
        # 3b. 활성 청크 건수 (조합 기준으로 카운트)
        stat_fail_chunk_files <- list.files(paths$db_stat_failed_folder, pattern="stat_failed_chunk_.*\\.duckdb", full.names=TRUE)
        chunk_stat_fail_count <- 0
        if (length(stat_fail_chunk_files) > 0) {
            for (chunk_file in stat_fail_chunk_files) {
                con_chunk <- NULL
                tryCatch({
                    con_chunk <- dbConnect(duckdb::duckdb(), dbdir = chunk_file, read_only = TRUE)
                    if ("stat_failures" %in% dbListTables(con_chunk)) {
                        # 조합 기준으로 카운트 (같은 조합에 여러 에러 메시지가 있어도 1건으로 카운트)
                        count <- dbGetQuery(con_chunk, "SELECT COUNT(*) FROM (SELECT DISTINCT cause_abb, outcome_abb, fu FROM stat_failures)")[1, 1]
                        chunk_stat_fail_count <- chunk_stat_fail_count + count
                    }
                    dbDisconnect(con_chunk, shutdown = TRUE)
                    con_chunk <- NULL
                }, finally = {
                    if (!is.null(con_chunk)) {
                        try(dbDisconnect(con_chunk, shutdown = TRUE), silent = TRUE)
                    }
                })
            }
        }
        
        total_stat_failures <- central_stat_fail_count + chunk_stat_fail_count
        
        cat(sprintf(" 🔴 (C) 총 통계 실패 로그 (중앙 %s + 청크 %s): %s 건\n", 
                    format(central_stat_fail_count, big.mark = ","),
                    format(chunk_stat_fail_count, big.mark = ","),
                    format(total_stat_failures, big.mark = ",")))
        
        # 통계 실패 로그 상세 정보 출력 (디버깅용)
        # 중앙 DB에 있는 실제 행 수 확인 (에러 메시지별로 여러 행일 수 있음)
        if (file.exists(paths$db_stat_failed_file)) {
            con_temp <- NULL
            tryCatch({
                con_temp <- dbConnect(duckdb::duckdb(), dbdir = paths$db_stat_failed_file, read_only = TRUE)
                if ("stat_failures" %in% dbListTables(con_temp)) {
                    total_rows <- dbGetQuery(con_temp, "SELECT COUNT(*) FROM stat_failures")[1, 1]
                    if (total_rows > central_stat_fail_count) {
                        cat(sprintf("     ℹ️  참고: 중앙 DB 실제 행 수: %s 건 (조합 기준: %s 건)\n",
                                   format(total_rows, big.mark = ","),
                                   format(central_stat_fail_count, big.mark = ",")))
                        cat(sprintf("     → 같은 조합에 여러 에러 메시지가 있는 경우: %s 건\n",
                                   format(total_rows - central_stat_fail_count, big.mark = ",")))
                    }
                }
                dbDisconnect(con_temp, shutdown = TRUE)
            }, error = function(e) {
                if (!is.null(con_temp)) try(dbDisconnect(con_temp, shutdown = TRUE), silent = TRUE)
            })
        }
        
    }, error = function(e) {
        cat(sprintf(" 🔴 (C) 통계 실패 로그 읽기 실패: %s\n", e$message))
    })

    # --- 4. (D) 총 시스템 실패 로그 건수 계산 ---
    cat("\n[4] (D) 총 시스템 실패 로그 건수 계산 중...\n")
    total_system_failures <- 0
    tryCatch({
        # 4a. 중앙 DB 건수
        central_system_fail_count <- 0
        con_central_system <- NULL
        if (file.exists(paths$db_system_failed_file)) {
            tryCatch({
                con_central_system <- dbConnect(duckdb::duckdb(), dbdir = paths$db_system_failed_file, read_only = TRUE)
                if ("system_failures" %in% dbListTables(con_central_system)) {
                    central_system_fail_count <- dbGetQuery(con_central_system, "SELECT COUNT(*) FROM system_failures")[1, 1]
                }
                dbDisconnect(con_central_system, shutdown = TRUE)
                con_central_system <- NULL
            }, finally = {
                if (!is.null(con_central_system)) {
                    try(dbDisconnect(con_central_system, shutdown = TRUE), silent = TRUE)
                }
            })
        }
        
        # 4b. 활성 청크 건수
        system_fail_chunk_files <- list.files(paths$db_system_failed_folder, pattern="system_failed_chunk_.*\\.duckdb", full.names=TRUE)
        chunk_system_fail_count <- 0
        if (length(system_fail_chunk_files) > 0) {
            for (chunk_file in system_fail_chunk_files) {
                con_chunk <- NULL
                tryCatch({
                    con_chunk <- dbConnect(duckdb::duckdb(), dbdir = chunk_file, read_only = TRUE)
                    if ("system_failures" %in% dbListTables(con_chunk)) {
                        count <- dbGetQuery(con_chunk, "SELECT COUNT(*) FROM system_failures")[1, 1]
                        chunk_system_fail_count <- chunk_system_fail_count + count
                    }
                    dbDisconnect(con_chunk, shutdown = TRUE)
                    con_chunk <- NULL
                }, finally = {
                    if (!is.null(con_chunk)) {
                        try(dbDisconnect(con_chunk, shutdown = TRUE), silent = TRUE)
                    }
                })
            }
        }
        
        total_system_failures <- central_system_fail_count + chunk_system_fail_count
        cat(sprintf(" ⚠️  (D) 총 시스템 실패 로그 (중앙 %s + 청크 %s): %s 건\n", 
                    format(central_system_fail_count, big.mark = ","),
                    format(chunk_system_fail_count, big.mark = ","),
                    format(total_system_failures, big.mark = ",")))
        
    }, error = function(e) {
        cat(sprintf(" ⚠️  (D) 시스템 실패 로그 읽기 실패: %s\n", e$message))
    })

    # --- 5. 결과 비교 및 진단 ---
    cat("\n--- [5] 최종 진단 ---\n")
    
    # 총 작업 수 계산: 성공 로그(A) + 시스템 실패 로그(D)
    # 참고: A = B + C 이므로, 시도한 작업 수 = (B + C) + D = A + D
    total_attempted_jobs <- total_success_logs + total_system_failures
    discrepancy <- total_success_logs - total_hr_results
    
    cat(sprintf(" (A) 성공 로그 (중앙+청크): %s 건\n", format(total_success_logs, big.mark = ",")))
    cat(sprintf(" (B) HR 결과 (청크 + 취합 parquet): %s 건\n", format(total_hr_results, big.mark = ",")))
    cat(sprintf(" (C) 통계 실패 로그 (중앙+청크): %s 건\n", format(total_stat_failures, big.mark = ",")))
    cat(sprintf(" (D) 시스템 실패 로그 (중앙+청크): %s 건\n", format(total_system_failures, big.mark = ",")))
    cat(sprintf(" (총합) 시도한 작업 수 (A + D): %s 건\n", format(total_attempted_jobs, big.mark = ",")))
    cat(sprintf(" (A) - (B) = 차이: %s 건\n", format(discrepancy, big.mark = ",")))
    
    # 검증 1: 성공 로그와 HR 결과 비교
    if (total_success_logs > 0 && discrepancy > 0) {
        # 차이는 통계 실패 또는 n=0 스킵으로 설명되어야 함
        expected_stat_failures <- discrepancy
        missing_logs <- expected_stat_failures - total_stat_failures
        
        if (missing_logs > 0) {
            cat(sprintf("\n ⚠️  진단 1: 성공 로그 vs HR 결과 불일치!\n"))
            cat(sprintf("     예상 통계 실패/스킵: %s 건\n", format(expected_stat_failures, big.mark = ",")))
            cat(sprintf("     실제 통계 실패 로그: %s 건\n", format(total_stat_failures, big.mark = ",")))
            cat(sprintf("     누락된 로그: %s 건\n", format(missing_logs, big.mark = ",")))
            cat("\n     가능한 원인:\n")
            cat("     1. 통계 실패 로그가 청크 파일에만 기록되고 중앙 DB로 취합되지 않음\n")
            cat("     2. 통계 실패 로그 기록 과정에서 오류 발생 (에러 메시지 확인 필요)\n")
            cat("     3. n=0 스킵 케이스가 통계 실패 로그에 기록되지 않음\n")
        } else if (missing_logs == 0) {
            cat("\n ✅  진단 1: 성공 로그 vs HR 결과 일치 확인!\n")
            cat("     모든 통계 실패/스킵이 통계 실패 로그에 정상적으로 기록되었습니다.\n")
        } else {
            cat(sprintf("\n ⚠️  진단 1: 통계 실패 로그가 예상보다 %s 건 더 많습니다.\n", 
                       format(-missing_logs, big.mark = ",")))
            cat("     이는 로그 기록 로직에 문제가 있거나 중복 기록 가능성이 있습니다.\n")
        }
    } else if (total_success_logs > 0 && discrepancy == 0) {
         cat("\n ✅  진단 1: 성공 로그와 HR 결과 건수가 일치합니다.\n")
         cat("     이는 현재까지 '정상 스킵'이나 '통계 실패'가\n")
         cat("     단 한 건도 발생하지 않았음을 의미합니다 (매우 드문 경우).\n")
    }
    
    # 검증 2: 전체 로그 일관성 확인
    if (total_attempted_jobs > 0) {
        cat(sprintf("\n--- [진단 2] 전체 로그 일관성 확인 ---\n"))
        cat(sprintf("     성공 로그: %s 건 (%.1f%%)\n", 
                   format(total_success_logs, big.mark = ","),
                   100 * total_success_logs / total_attempted_jobs))
        cat(sprintf("     통계 실패: %s 건 (%.1f%%)\n", 
                   format(total_stat_failures, big.mark = ","),
                   100 * total_stat_failures / total_attempted_jobs))
        cat(sprintf("     시스템 실패: %s 건 (%.1f%%)\n", 
                   format(total_system_failures, big.mark = ","),
                   100 * total_system_failures / total_attempted_jobs))
        
        if (total_stat_failures + total_system_failures > 0) {
            failure_rate <- 100 * (total_stat_failures + total_system_failures) / total_attempted_jobs
            cat(sprintf("\n     전체 실패율: %.2f%%\n", failure_rate))
            if (failure_rate > 10) {
                cat("     ⚠️  실패율이 높습니다. 로그를 확인하세요.\n")
            }
        }
        
        # HR 결과가 있는 비율
        hr_success_rate <- if (total_success_logs > 0) {
            100 * total_hr_results / total_success_logs
        } else { 0 }
        cat(sprintf("     HR 결과 생성률: %.1f%% (성공 로그 대비)\n", hr_success_rate))
        
        if (total_stat_failures > 0 && total_system_failures > 0) {
            stat_vs_system_ratio <- total_stat_failures / total_system_failures
            cat(sprintf("     통계 실패 / 시스템 실패 비율: %.2f\n", stat_vs_system_ratio))
            if (stat_vs_system_ratio > 10) {
                cat("     ℹ️  통계 실패가 시스템 실패보다 많습니다 (정상적인 패턴).\n")
            } else if (stat_vs_system_ratio < 0.1) {
                cat("     ⚠️  시스템 실패가 통계 실패보다 많습니다. 시스템 문제를 확인하세요.\n")
            }
        }
    }
    
    if (total_attempted_jobs == 0) {
         cat("\n ℹ️  정보: 아직 비교할 데이터가 없습니다.\n")
    }
    cat("==========================================================\n")
}

check_log_discrepancy()  # 주석 처리: main() 실행 시 파일 충돌 방지
