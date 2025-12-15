# ============================================================================
# [engine_v2 수정 사항] 데이터 소실 방지를 위한 aggregation 방식 복원
# ============================================================================
# 
# engine_v2 문제점:
# 1. <<- 할당 실패: future_lapply 워커 환경에서 <<-가 부모 스코프를 찾지 못해
#    "object 'batch_stat_failed_jobs' not found" 에러 발생
# 2. 부분 커밋 문제: 각 연결이 별도 트랜잭션이어서 일부만 커밋될 수 있음
#    (완료 로그는 커밋되었지만 HR 결과는 롤백됨)
#
# engine_v2 수정 사항:
# [1번 - 필수] <<-를 parent.frame()으로 변경
#   - 171, 181, 190, 219번 줄: error 핸들러에서 부모 스코프 명시적 참조
#   - 워커 환경에서도 안정적으로 동작
#
# [2번 - 필수] 배치 쓰기 순서 변경
#   - HR 결과 먼저 쓰기 → 통계 실패 로그 → 시스템 실패 로그 → 완료 로그(마지막)
#   - 완료 로그는 HR 결과나 통계 실패 로그가 실제로 기록된 경우에만 기록
#   - 부분 커밋 문제 방지
#
# [3번 - 선택] 배치 쓰기 후 검증 로직 추가
#   - 배치 쓰기 후 실제 기록된 내용 확인
#   - 완료 로그에 있지만 HR 결과/통계 실패 로그가 없으면 완료 로그에서 삭제
#   - 추가 안전장치
#
# [4번 - 선택] 완료 로그 기록 조건 강화
#   - 배치 쓰기 단계에서 실제로 HR 결과나 통계 실패 로그가 기록된 경우에만 완료 로그 기록
#   - 완료 로그의 신뢰성 향상
#
# engine_v2 문제점:
# - Edge 매핑 데이터 취합 시 DuckDB COPY 방식 사용
#   → 실패한 청크 파일이 포함되면 전체 취합 실패 가능
#   → 데이터 소실 위험 (청크 파일 삭제 전 검증 없음)
#
# engine_v2 수정 사항:
# [5번 - 필수] Edge 매핑 취합 방식을 engine_v1으로 복원
#   - 각 청크 파일을 하나씩 읽어서 검증 후 취합
#   - 실패한 파일은 건너뛰고 성공한 파일만 취합
#   - 취합 완료 후 청크 파일 삭제 (engine_v2에서는 삭제했으나, engine_v3에서는 삭제하지 않음)
#   - 메모리 문제는 있더라도 데이터 무결성 보장
#
# engine_v2 이후 이슈 (aggregate_results_mappings):
# - 메모리 부족 문제: 수천 개의 청크 파일이 쌓이면 aggregate 시 메모리 부족
# - DuckDB COPY 방식 시도 실패: 청크 파일 삭제로 인한 데이터 소실
# - 현재 방식 한계: 사이클 종료 시에만 aggregate 실행하여 청크 파일이 계속 쌓임
# - 개선 필요: 주기적 aggregate 또는 청크 단위 순차 처리
#   (자세한 내용은 aggregate_results_mappings 함수 주석 참조)
#
# engine_v3 수정 사항:
# [6번 - 필수] Edge 매핑 데이터 취합 로직을 별도 스크립트로 분리
#   - aggregate_edge_mappings.R 스크립트로 분리
#   - 메인 프로세스에서는 청크 파일만 생성하고 aggregate는 별도로 실행
#   - 장점: 메인 프로세스와 aggregate 프로세스 분리로 안정성 향상
#   - 장점: 필요할 때만 aggregate 실행 가능
#   - 장점: 청크 파일만 생성하고 aggregate는 나중에 처리 가능
#
# [7번 - 필수] 청크 파일 삭제 로직 제거
#   - 기존: 취합 완료 후 청크 파일 삭제 (engine_v2 방식)
#   - 변경: 청크 파일은 삭제하지 않음 (데이터 보존)
#   - 이유: 데이터 복구 및 재처리 가능성 보장
#   - 적용: HR 청크 파일, Map 청크 파일, 로그 청크 파일 모두 보존
#
# [8번 - 필수] stat_excluded 케이스 처리 추가
#   - 목적: case=1 & status=1이 없는 경우 HR 분석 및 매핑 생성을 스킵
#   - 처리: stat_excluded 로그에 기록하고 completed 로그에도 기록 (이어쓰기 아키텍처 유지)
#   - 체크 시점: 전처리 완료 후, HR 분석 전 (merge 후 체크)
#   - 기록 방식: stat_failed_jobs, system_failed_jobs와 동일한 청크 파일 → 중앙 DB 취합 방식
#   - 적용 위치:
#     * process_batch: stat_excluded 체크 및 로그 기록 로직 추가
#     * run_hr_analysis: stat_excluded 폴더 경로 파라미터 추가
#     * main: paths 동적 생성 (fu에 따라) 및 stat_excluded 로그 취합 추가
#
# [9번 - 필수] paths 동적 생성
#   - 기존: fu=10 (v10) 고정 경로
#   - 변경: fu 파라미터에 따라 경로 동적 생성 (fu=9, fu=10 등 모두 지원)
#   - 적용: main 함수 내부에서 paths 리스트를 fu 기반으로 생성
#
# [10번 - 필수] aggregate_results_mappings 함수 호출 제거
#   - 기존: HR 결과 취합 및 Node 매핑 데이터 생성을 메인 프로세스에서 처리
#   - 변경: HR 결과 취합 및 Node 매핑 데이터 생성을 별도 스크립트로 분리
#   - 이유:
#     * 취합 실패 시 메인 프로세스에 영향 없음 (프로세스 분리)
#     * 메모리 부담 감소 (HR 분석 직후 메모리 사용량이 높을 때 취합하지 않음)
#     * 재시도 용이 (실패 시 독립적으로 재실행 가능)
#     * Edge 매핑 취합과 동일한 패턴 유지
#   - 구현: 별도 스크립트에 HR 결과 취합 및 Node 매핑 로직 추가 예정
#   - 참고: 청크 파일들은 삭제하지 않으므로 언제든지 별도 스크립트로 취합 가능
#   - 적용 위치: main 함수에서 aggregate_results_mappings 호출 제거 (Line 1177-1190)
#
# ============================================================================

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
# 3단계: 배치 처리 모듈 (Batch Processing Module)
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
    db_stat_failed_folder_path, # 통계 실패 로그 청크 폴더
    db_stat_excluded_folder_path # stat_excluded 로그 청크 폴더 (engine_v3 신규)
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
    db_stat_excluded_chunk_path <- file.path(db_stat_excluded_folder_path, sprintf("stat_excluded_chunk_%s.duckdb", worker_pid)) # [engine_v3 신규]

    # RAM에 결과를 모을 임시 리스트 초기화
    batch_hr_results <- list()
    batch_edge_pids <- list()
    batch_edge_index <- list()
    batch_edge_key <- list()
    batch_completed_jobs <- list()
    batch_system_failed_jobs <- list()
    batch_stat_failed_jobs <- list()
    batch_stat_excluded_jobs <- list() # [engine_v3 신규]

    # 배치 내 각 작업 처리 (기존 process_one_pair 로직 재활용)
    # 배치 내부 프로그레스 바 추가: 각 작업 완료 시마다 진행률 표시
    progressr::with_progress({
        p_job <- progressr::progressor(steps = nrow(batch_jobs))
        
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

                # --- [engine_v3 신규] stat_excluded 체크: case=1 & status=1이 없는 경우 HR 분석 및 매핑 생성 스킵 ---
                if (nrow(clean_data) > 0) {
                    case1_status1_count <- nrow(clean_data[case == 1 & status == 1])
                    if (case1_status1_count == 0) {
                        # stat_excluded 로그 기록
                        batch_stat_excluded_jobs[[length(batch_stat_excluded_jobs) + 1]] <- data.frame(
                            cause_abb = cause_abb, outcome_abb = outcome_abb, fu = fu,
                            reason = "case=1 & status=1: No outcome events in case=1 group", 
                            timestamp = Sys.time()
                        )
                        # completed 로그 기록 (이어쓰기 아키텍처 유지)
                        batch_completed_jobs[[length(batch_completed_jobs) + 1]] <- data.frame(
                            cause_abb = cause_abb, outcome_abb = outcome_abb, fu = fu
                        )
                        # HR 분석 및 매핑 생성 스킵
                        next
                    }
                }

                # --- 3. HR 분석 ---
                hr_result <- NULL # 결과 초기화
                if(nrow(clean_data) > 0) {
                    
                    # [v10.1 신규] 안쪽 tryCatch (통계 오류 감지용)
                    tryCatch({
                        
                        hr_result <- perform_hr_analysis(clean_data, fu, cause_abb, outcome_abb)
                        
                        # NULL 반환 케이스 처리
                        # [engine_v2 수정] <<- 대신 parent.frame() 사용 (워커 환경에서 안정적)
                        if (is.null(hr_result)) {
                            parent_env <- parent.frame()
                            parent_env$batch_stat_failed_jobs[[length(parent_env$batch_stat_failed_jobs) + 1]] <- data.frame(
                                cause_abb = cause_abb, outcome_abb = outcome_abb, fu = fu,
                                error_msg = "perform_hr_analysis returned NULL unexpectedly", timestamp = Sys.time()
                            )
                        }
                        
                    }, error = function(stat_e) {
                        # --- [신규] 3. 통계 계산 실패 로그 기록 ---
                        # [engine_v2 수정] <<- 대신 parent.frame() 사용 (워커 환경에서 안정적)
                        parent_env <- parent.frame()
                        parent_env$batch_stat_failed_jobs[[length(parent_env$batch_stat_failed_jobs) + 1]] <- data.frame(
                            cause_abb = cause_abb, outcome_abb = outcome_abb, fu = fu,
                            error_msg = as.character(stat_e$message), timestamp = Sys.time()
                        )
                    }) # end inner tryCatch
                } else {
                    # --- [수정] n=0 스킵 케이스도 통계 실패 로그에 기록 ---
                    # (데이터가 없어서 통계 분석을 수행할 수 없는 경우)
                    # [engine_v2 수정] if-else 블록은 같은 스코프이므로 <- 사용 가능
                    batch_stat_failed_jobs[[length(batch_stat_failed_jobs) + 1]] <- data.frame(
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
                # [engine_v2 수정] <<- 대신 parent.frame() 사용 (워커 환경에서 안정적)
                parent_env <- parent.frame()
                parent_env$batch_system_failed_jobs[[length(parent_env$batch_system_failed_jobs) + 1]] <- data.frame(
                    cause_abb = cause_abb, outcome_abb = outcome_abb, fu = fu,
                    error_msg = as.character(sys_e$message), timestamp = Sys.time()
                )
            }) # end outer tryCatch
            
            p_job() # 각 작업 완료 시 프로그레스 업데이트
        } # end for loop
    }) # end with_progress

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
        
        # [engine_v3 신규] stat_excluded 로그 키 추출 (있을 경우)
        stat_excluded_keys <- if (length(batch_stat_excluded_jobs) > 0) {
            sapply(batch_stat_excluded_jobs, function(x) paste(x$cause_abb, x$outcome_abb, x$fu, sep = "_"))
        } else {
            character(0)
        }
        
        # 성공 로그에는 있지만 HR 결과도 통계 실패 로그도 stat_excluded 로그도 없는 경우
        missing_keys <- setdiff(completed_keys, c(hr_keys, stat_fail_keys, stat_excluded_keys))
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
            cat("  stat_excluded 로그 총 건수:", length(stat_excluded_keys), "\n")
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
            cat("원인: 완료 로그에는 있지만 HR 결과, 통계 실패 로그, stat_excluded 로그\n")
            cat("      모두 없는 조합이 발견되었습니다. 로직 오류가 있는 것으로 보입니다.\n")
            cat("==========================================================\n")
            flush.console()
            
            stop(sprintf(
                "ERROR: 완료 로그에는 있지만 HR 결과, 통계 실패 로그, stat_excluded 로그 모두 없는 조합 %d개 발견. 실행을 중단합니다.",
                length(missing_keys)
            ))
        }
    }

    # --- [v10.1 수정] 일괄 쓰기 (트랜잭션 관리) ---
    con_hr <- if (length(batch_hr_results) > 0) dbConnect(duckdb::duckdb(), dbdir = db_hr_chunk_path, read_only = FALSE) else NULL
    con_map <- if (length(batch_edge_pids) > 0 || length(batch_edge_index) > 0 || length(batch_edge_key) > 0) dbConnect(duckdb::duckdb(), dbdir = db_map_chunk_path, read_only = FALSE) else NULL
    con_comp <- if (length(batch_completed_jobs) > 0) dbConnect(duckdb::duckdb(), dbdir = db_completed_chunk_path, read_only = FALSE) else NULL
    con_system_fail <- if (length(batch_system_failed_jobs) > 0) dbConnect(duckdb::duckdb(), dbdir = db_system_failed_chunk_path, read_only = FALSE) else NULL
    con_stat_fail <- if (length(batch_stat_failed_jobs) > 0) dbConnect(duckdb::duckdb(), dbdir = db_stat_failed_chunk_path, read_only = FALSE) else NULL
    con_stat_excluded <- if (length(batch_stat_excluded_jobs) > 0) dbConnect(duckdb::duckdb(), dbdir = db_stat_excluded_chunk_path, read_only = FALSE) else NULL # [engine_v3 신규]

    on.exit({
        if (!is.null(con_hr)) dbDisconnect(con_hr, shutdown = TRUE)
        if (!is.null(con_map)) dbDisconnect(con_map, shutdown = TRUE)
        if (!is.null(con_comp)) dbDisconnect(con_comp, shutdown = TRUE)
        if (!is.null(con_system_fail)) dbDisconnect(con_system_fail, shutdown = TRUE)
        if (!is.null(con_stat_fail)) dbDisconnect(con_stat_fail, shutdown = TRUE)
        if (!is.null(con_stat_excluded)) dbDisconnect(con_stat_excluded, shutdown = TRUE) # [engine_v3 신규]
    })

    # 트랜잭션 시작
    if (!is.null(con_hr)) dbExecute(con_hr, "BEGIN TRANSACTION;")
    if (!is.null(con_map)) dbExecute(con_map, "BEGIN TRANSACTION;")
    if (!is.null(con_comp)) dbExecute(con_comp, "BEGIN TRANSACTION;")
    if (!is.null(con_system_fail)) dbExecute(con_system_fail, "BEGIN TRANSACTION;")
    if (!is.null(con_stat_fail)) dbExecute(con_stat_fail, "BEGIN TRANSACTION;")
    if (!is.null(con_stat_excluded)) dbExecute(con_stat_excluded, "BEGIN TRANSACTION;") # [engine_v3 신규]

    tryCatch({
        # [engine_v2 수정] 배치 쓰기 순서 변경: HR 결과 먼저, 완료 로그 마지막
        # 목적: 부분 커밋 문제 방지 (완료 로그가 있으면 HR 결과나 통계 실패 로그가 반드시 존재)
        
        # 1. HR 결과 쓰기 (가장 중요 - 먼저 처리)
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
            dbExecute(con_hr, "COMMIT;")
        }
        
        # 2. 통계 실패 로그 쓰기
        if (!is.null(con_stat_fail)) {
            if (length(batch_stat_failed_jobs) > 0) {
                dbExecute(con_stat_fail, "CREATE TABLE IF NOT EXISTS stat_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME)")
                dbWriteTable(con_stat_fail, "stat_failures", bind_rows(batch_stat_failed_jobs), append = TRUE)
                dbExecute(con_stat_fail, "COMMIT;")
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
        
        # 3. 시스템 실패 로그 쓰기
        if (!is.null(con_system_fail)) {
            dbExecute(con_system_fail, "CREATE TABLE IF NOT EXISTS system_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME)")
            dbWriteTable(con_system_fail, "system_failures", bind_rows(batch_system_failed_jobs), append = TRUE)
            dbExecute(con_system_fail, "COMMIT;")
        }
        
        # 3-1. [engine_v3 신규] stat_excluded 로그 쓰기
        if (!is.null(con_stat_excluded)) {
            if (length(batch_stat_excluded_jobs) > 0) {
                dbExecute(con_stat_excluded, "CREATE TABLE IF NOT EXISTS stat_excluded (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, reason VARCHAR, timestamp DATETIME)")
                dbWriteTable(con_stat_excluded, "stat_excluded", bind_rows(batch_stat_excluded_jobs), append = TRUE)
                dbExecute(con_stat_excluded, "COMMIT;")
            }
        } else if (length(batch_stat_excluded_jobs) > 0) {
            # stat_excluded가 있는데 연결이 NULL인 경우 경고 및 연결 재생성
            warning(sprintf("stat_excluded %d건이 있지만 연결이 NULL입니다. 연결을 재생성합니다.\n", length(batch_stat_excluded_jobs)))
            con_stat_excluded <- dbConnect(duckdb::duckdb(), dbdir = db_stat_excluded_chunk_path, read_only = FALSE)
            dbExecute(con_stat_excluded, "BEGIN TRANSACTION;")
            dbExecute(con_stat_excluded, "CREATE TABLE IF NOT EXISTS stat_excluded (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, reason VARCHAR, timestamp DATETIME)")
            dbWriteTable(con_stat_excluded, "stat_excluded", bind_rows(batch_stat_excluded_jobs), append = TRUE)
            dbExecute(con_stat_excluded, "COMMIT;")
        }
        
        # 4. 완료 로그 쓰기 (마지막 - HR 결과나 통계 실패 로그나 stat_excluded 로그가 실제로 기록된 경우에만)
        # HR 결과가 있거나 통계 실패 로그가 있는 작업만 완료 로그에 기록
        if (!is.null(con_comp)) {
            # 완료 로그에 기록할 작업 필터링
            completed_keys <- if (length(batch_completed_jobs) > 0) {
                completed_df <- bind_rows(batch_completed_jobs)
                paste(completed_df$cause_abb, completed_df$outcome_abb, completed_df$fu, sep = "_")
            } else {
                character(0)
            }
            
            # HR 결과 키 추출
            hr_keys <- if (length(batch_hr_results) > 0) {
                sapply(batch_hr_results, function(x) paste(x$cause_abb[1], x$outcome_abb[1], x$fu[1], sep = "_"))
            } else {
                character(0)
            }
            
            # 통계 실패 로그 키 추출
            stat_fail_keys <- if (length(batch_stat_failed_jobs) > 0) {
                sapply(batch_stat_failed_jobs, function(x) paste(x$cause_abb, x$outcome_abb, x$fu, sep = "_"))
            } else {
                character(0)
            }
            
            # [engine_v3 신규] stat_excluded 로그 키 추출
            stat_excluded_keys <- if (length(batch_stat_excluded_jobs) > 0) {
                sapply(batch_stat_excluded_jobs, function(x) paste(x$cause_abb, x$outcome_abb, x$fu, sep = "_"))
            } else {
                character(0)
            }
            
            # HR 결과나 통계 실패 로그나 stat_excluded 로그가 있는 작업만 완료 로그에 기록
            valid_completed_keys <- intersect(completed_keys, c(hr_keys, stat_fail_keys, stat_excluded_keys))
            
            if (length(valid_completed_keys) > 0) {
                # 키를 다시 분리하여 데이터프레임 생성
                valid_parts <- strsplit(valid_completed_keys, "_")
                valid_completed_jobs <- data.frame(
                    cause_abb = sapply(valid_parts, function(x) x[1]),
                    outcome_abb = sapply(valid_parts, function(x) x[2]),
                    fu = as.integer(sapply(valid_parts, function(x) x[3]))
                )
                
                dbExecute(con_comp, "CREATE TABLE IF NOT EXISTS jobs (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER)")
                dbWriteTable(con_comp, "jobs", valid_completed_jobs, append = TRUE)
                dbExecute(con_comp, "COMMIT;")
            }
        }
        
        # 5. Map 데이터 쓰기
        if (!is.null(con_map)) {
            dbExecute(con_map, "CREATE TABLE IF NOT EXISTS edge_pids (key VARCHAR, person_id BIGINT);")
            dbExecute(con_map, "CREATE TABLE IF NOT EXISTS edge_index_key_seq (key VARCHAR, index_key_seq BIGINT);")
            dbExecute(con_map, "CREATE TABLE IF NOT EXISTS edge_key_seq (key VARCHAR, outcome_key_seq BIGINT);")
            if (length(batch_edge_pids) > 0) dbWriteTable(con_map, "edge_pids", bind_rows(batch_edge_pids), append = TRUE)
            if (length(batch_edge_index) > 0) dbWriteTable(con_map, "edge_index_key_seq", bind_rows(batch_edge_index), append = TRUE)
            if (length(batch_edge_key) > 0) dbWriteTable(con_map, "edge_key_seq", bind_rows(batch_edge_key), append = TRUE)
            dbExecute(con_map, "COMMIT;")
        }

    }, error = function(e) {
        # [engine_v2 수정] 쓰기 중 오류 발생 시 롤백 시도
        # 주의: 각 연결이 이미 개별적으로 커밋되었을 수 있으므로, 롤백이 실패할 수 있음
        # 하지만 일관성을 위해 롤백 시도는 유지
        warning("Batch write failed, attempting rollback: ", e$message)
        if (!is.null(con_hr)) try(dbExecute(con_hr, "ROLLBACK;"), silent = TRUE)
        if (!is.null(con_map)) try(dbExecute(con_map, "ROLLBACK;"), silent = TRUE)
        if (!is.null(con_comp)) try(dbExecute(con_comp, "ROLLBACK;"), silent = TRUE)
        if (!is.null(con_system_fail)) try(dbExecute(con_system_fail, "ROLLBACK;"), silent = TRUE)
        if (!is.null(con_stat_fail)) try(dbExecute(con_stat_fail, "ROLLBACK;"), silent = TRUE)
        if (!is.null(con_stat_excluded)) try(dbExecute(con_stat_excluded, "ROLLBACK;"), silent = TRUE) # [engine_v3 신규]
        stop(e) # 에러를 다시 던져서 상위 tryCatch가 잡도록 함
    })

    # 메모리 정리 (메모리 파편화 방지)
    rm(batch_hr_results, batch_edge_pids, batch_edge_index, batch_edge_key,
       batch_completed_jobs, batch_system_failed_jobs, batch_stat_failed_jobs, batch_stat_excluded_jobs) # [engine_v3 신규]
    gc()

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
    chunks_per_core = 3, # [신규] 코어당 청크 수 (기본값: 3, Spark/Dask 표준)
    matched_parquet_folder_path, 
    outcome_parquet_file_path, # v4.0과 동일하게 '파일 경로'
    results_hr_folder_path,
    results_mapping_folder_path,
    db_completed_file_path, # 중앙 성공 로그 경로
    db_completed_folder_path, # 청크 성공 로그 폴더 경로
    db_system_failed_folder_path, # 청크 시스템 실패 로그 경로,
    db_stat_failed_folder_path, # 청크 통계 실패 로그 경로,
    db_stat_excluded_folder_path # 청크 stat_excluded 로그 경로 (engine_v3 신규)
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
    # --- [수정] 작업을 배치로 분할하고, 각 배치를 n_cores로 나눠서 처리 ---
    total_jobs_to_do_count <- nrow(jobs_to_do)
    num_batches <- ceiling(total_jobs_to_do_count / batch_size)
    job_indices <- 1:total_jobs_to_do_count
    # split 함수를 사용하여 인덱스를 배치 크기만큼 나눔
    batches_indices <- split(job_indices, ceiling(job_indices / batch_size))

    # 모든 배치 처리 (제한 없음)
    num_batches_this_run <- num_batches
    batches_indices_this_run <- batches_indices

    cat(sprintf("--- 총 %d개 배치 처리 시작 (각 배치는 작은 청크로 분할하여 %d개 코어가 동적으로 처리) ---\n", 
        num_batches, n_cores))

    # --- 병렬 처리 설정 ---
    plan(multisession, workers = n_cores, gc = TRUE, earlySignal = TRUE)
    required_packages <- c("data.table", "duckdb", "DBI", "arrow", "survival", "broom", "tidycmprsk", "dplyr", "glue")

    progressr::with_progress({
        p <- progressr::progressor(steps = num_batches_this_run) # 프로그레스 스텝 = 배치 수

        # 각 배치를 순차적으로 처리
        for (batch_idx in 1:length(batches_indices_this_run)) {
            # 현재 배치의 작업 목록
            batch_indices <- batches_indices_this_run[[batch_idx]]
            current_batch_jobs <- jobs_to_do[batch_indices, ]
            
            # 작은 청크 기반 동적 할당 (업계 표준 방식)
            batch_job_count <- nrow(current_batch_jobs)
            
            # 작은 청크 크기 계산: 코어 수의 chunks_per_core배만큼 청크 생성하여 부하 균형 향상
            target_chunks <- n_cores * chunks_per_core  # 목표 청크 수
            chunk_size <- max(1, ceiling(batch_job_count / target_chunks))
            
            # 실제 생성될 청크 수
            num_chunks <- ceiling(batch_job_count / chunk_size)
            
            # 배치 내 작업을 작은 청크로 분할
            chunk_indices_list <- split(1:batch_job_count, ceiling((1:batch_job_count) / chunk_size))
            
            cat(sprintf("    [배치 %d/%d] 작은 청크 기반 할당: %d개 작업 → %d개 청크 (청크당 평균 %d개)\n",
                        batch_idx, length(batches_indices_this_run), batch_job_count, num_chunks, chunk_size))
            
            # 각 청크를 워커에게 동적 병렬 할당 (future가 자동으로 부하 균형 조정)
            results <- future_lapply(chunk_indices_list, function(chunk_indices) {
                # 현재 청크에 해당하는 작업
                chunk_jobs <- current_batch_jobs[chunk_indices, ]

                # [v10.1 수정] 바깥쪽 tryCatch (시스템/R 실패 감지용)
                tryCatch({
                    
                    # 배치 처리 함수 호출 (청크 단위)
                    process_batch(
                        batch_jobs = chunk_jobs,
                        fu = fu,
                        matched_parquet_folder_path = matched_parquet_folder_path,
                        outcome_parquet_file_path = outcome_parquet_file_path,
                        results_hr_folder_path = results_hr_folder_path,
                        results_mapping_folder_path = results_mapping_folder_path,
                        db_completed_folder_path = db_completed_folder_path,
                        db_system_failed_folder_path = db_system_failed_folder_path,
                        db_stat_failed_folder_path = db_stat_failed_folder_path,
                        db_stat_excluded_folder_path = db_stat_excluded_folder_path # [engine_v3 신규]
                    )
                    
                }, error = function(e) {
                    # [v10.1 수정] "시스템/R 실패" 또는 "일괄 쓰기 실패"를 2번 로그에 기록
                    cat(sprintf("\nCRITICAL BATCH ERROR (first job: %s -> %s): %s\n", 
                        chunk_jobs[1, ]$cause_abb, 
                        chunk_jobs[1, ]$outcome_abb, 
                        e$message))
                    
                    worker_pid <- Sys.getpid()
                    db_system_failed_chunk_path <- file.path(db_system_failed_folder_path, sprintf("system_failed_chunk_%s.duckdb", worker_pid))
                    
                    con_system_failed <- dbConnect(duckdb::duckdb(), dbdir = db_system_failed_chunk_path, read_only = FALSE)
                    on.exit(dbDisconnect(con_system_failed, shutdown = TRUE))
                    
                    dbExecute(con_system_failed, "CREATE TABLE IF NOT EXISTS system_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME)")
                    
                    # 청크 실패를 첫 번째 작업 기준으로 기록
                    dbWriteTable(
                        con_system_failed, "system_failures", 
                        data.frame(
                            cause_abb = chunk_jobs[1, ]$cause_abb, 
                            outcome_abb = chunk_jobs[1, ]$outcome_abb, 
                            fu = fu, 
                            error_msg = as.character(e$message),
                            timestamp = Sys.time()
                        ), 
                        append = TRUE
                    )
                })
                return(TRUE)
            }, future.seed = TRUE, future.packages = required_packages)
            
            p() # 배치 완료 시 프로그레스 업데이트
        }
    })

    plan(sequential)
    # cat("\n--- [단계 1] 핵심 병렬 배치 분석 완료 ---\n")
    cat(sprintf("\n--- [단계 1] %d개 배치 분석 완료 ---\n", num_batches_this_run))

    # remaining_jobs 계산은 main 함수에서 사이클 종료 시 취합 후에 수행됨
    return(invisible(TRUE))
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

# ============================================================================
# aggregate_results_mappings 함수 - 이슈 및 개선 이력
# ============================================================================
# 
# [주요 이슈 및 문제점]
# 
# 1. 메모리 부족 문제 (engine_v1 이전)
#    - 문제: 수천 개의 map_chunk 파일이 쌓이면 모든 데이터를 메모리에 올려서
#            bind_rows()로 결합하는 과정에서 메모리 부족 발생
#    - 증상: "cannot allocate vector of size X GB" 에러
#    - 원인: 
#      * 모든 청크 파일의 데이터를 all_edge_pids, all_edge_index, all_edge_key
#        리스트에 저장 (794, 803, 812줄)
#      * bind_rows()로 결합할 때 모든 데이터가 메모리에 동시에 존재
#      * group_by() 및 summarise() 과정에서 추가 메모리 사용
#    - 영향: 대규모 작업 시 aggregate 단계에서 크래시 발생
#
# 2. DuckDB COPY 방식 시도 및 실패 (engine_v2 시도)
#    - 시도: 메모리 문제 해결을 위해 DuckDB의 COPY INTO나 UNION ALL을 사용하여
#            메모리 없이 직접 SQL로 aggregate 시도
#    - 실패 원인:
#      * 실패한 청크 파일이 포함되면 전체 취합 실패
#      * 청크 파일 삭제 전 검증 없이 진행하여 데이터 소실 위험
#      * 일부 청크 파일이 손상되거나 불완전한 상태에서 전체 취합 시도
#    - 결과: 
#      * aggregate 실패 시 청크 파일들이 이미 삭제되어 데이터 복구 불가능
#      * v10_recover_20251110.R 스크립트로 복구 작업 필요
#      * 데이터 소실 위험으로 인해 롤백
#
# 3. 현재 방식의 한계 (engine_v2)
#    - 현재: engine_v1 방식으로 복원 (각 청크를 하나씩 읽어서 검증 후 취합)
#    - 장점:
#      * 실패한 파일은 건너뛰고 성공한 파일만 취합 (데이터 소실 방지)
#      * 청크 파일 삭제 전 검증 가능
#    - 한계:
#      * 여전히 메모리 문제 가능성 (모든 데이터를 메모리에 올림)
#      * 청크 파일이 수천 개 쌓이면 aggregate 시 메모리 부족 가능
#      * 사이클 종료 시에만 aggregate 실행하여 청크 파일이 계속 쌓임
#
# [개선 방안 고려사항 및 제약사항]
#
# 1. 주기적 Aggregate 구현 시 주의사항
#    - 장점: 메모리 부담 분산, 청크 파일 수 제어 가능
#    - 제약사항:
#      * 모든 워커가 파일 시스템을 동시에 변경 중이므로 임의 시점에 aggregate 불가
#      * 반드시 batch가 완전히 마무리된 시점에서만 실행 가능
#      * run_hr_analysis 함수의 배치 처리 완료 후에만 안전하게 실행 가능
#    - 구현 위치: future_lapply의 각 배치 완료 후 또는 배치 사이 사이
#
# 2. 증분 Aggregate의 메모리 문제
#    - 주기적으로 aggregate를 해도 기존 parquet 파일을 R 메모리에 올리는 과정에서
#      메모리 부족 발생 가능
#    - 기존 parquet 파일이 매우 크면 (수 GB 이상) arrow::read_parquet()만으로도
#      메모리 부족 가능
#    - 해결: 기존 parquet을 읽지 않고 청크 단위로만 처리하거나,
#            DuckDB를 통한 스트리밍 방식 고려 필요
#
# 3. 별도 Aggregate 함수 분리 방안
#    - mapping chunk 파일들을 생성만 해두고, aggregate는 별도 함수로 분리
#    - 장점:
#      * 메인 프로세스와 aggregate 프로세스 분리로 안정성 향상
#      * aggregate는 필요할 때만 별도로 실행 가능
#      * 메인 프로세스는 계속 진행하면서 aggregate는 백그라운드로 실행 가능
#    - 단점:
#      * 두 프로세스 간 동기화 필요 (청크 파일 삭제 시점 등)
#      * 구현 복잡도 증가
#
# [결정 사항]
# - Aggregate 로직을 별도 스크립트로 분리하기로 결정 (engine_v3 구현 완료)
# - 방식: 기존 parquet 파일을 읽지 않고 청크 파일만 처리하는 방식 채택
#   * 이유: 기존 parquet을 읽으면 메모리 부족 가능성
#   * Edge 매핑은 key별로 그룹화되어 있어 청크를 모두 처리하면 모든 key 포함됨
#   * 메모리 사용량이 청크 크기에만 의존하여 안정적
# - 구현 완료:
#   * aggregate_edge_mappings.R 스크립트 생성
#   * aggregate_edge_mappings_standalone() 함수 구현
#   * 청크 파일만 읽어서 처리 (기존 parquet 무시)
#   * key 기준으로 중복 제거 처리
#   * 배치 단위 처리 옵션 제공 (chunk_batch_size 파라미터)
#   * 별도 R 스크립트로 실행 가능
#
# [현재 상태]
# - engine_v3: Edge 매핑 데이터 취합 로직을 aggregate_edge_mappings.R로 분리 완료
# - 메인 프로세스(hr_calculator_engine.R)는 청크 파일만 생성
# - Aggregate는 별도 스크립트로 필요할 때 실행 가능
# - 메모리 문제는 있더라도 데이터 소실 방지가 최우선
# - 대규모 작업 시 청크 파일이 쌓여도 aggregate는 별도로 처리 가능
# - 향후 개선: sh 래퍼 스크립트 추가 (자동 재시작 기능 포함)
#
# ============================================================================

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
        # [설계 원칙] 매핑 자료는 덮어쓰기 방식 (이어쓰기 아님)
        # - HR 결과: 작업이 점진적으로 완료되므로 이어쓰기 필요
        # - 매핑 자료 (Node/Edge): 원본/청크 파일에 항상 전체 데이터가 존재하므로
        #   매번 전체를 재생성하는 덮어쓰기 방식이 적절함
        # - Node: matched_*.parquet에서 직접 읽어서 전체 재생성
        # - Edge: 청크 파일에서 읽어서 전체 재생성 (aggregate_edge_mappings.R에서 처리)
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
        # [engine_v3 변경] Edge 매핑 데이터 취합 로직을 별도 스크립트로 분리
        # - aggregate_edge_mappings.R 스크립트에서 처리
        # - 메인 프로세스에서는 청크 파일만 생성하고 aggregate는 별도로 실행
        cat("\n3. Edge 매핑 데이터 취합은 별도 스크립트로 처리됩니다.\n")
        cat("   - 청크 파일은 유지되며, aggregate_edge_mappings.R로 별도 실행 가능합니다.\n")

        # --- 4. HR 임시 청크 파일 ---
        # [engine_v3 수정] 청크 파일 삭제 로직 제거
        # - 기존: 취합 완료 후 청크 파일 삭제
        # - 변경: 청크 파일은 삭제하지 않음 (데이터 보존)
        # - 이유: 데이터 복구 및 재처리 가능성 보장
        # - 참고: 최종 parquet 파일은 삭제하지 않으므로 이어쓰기 기능에 영향 없음
        # Map 청크 파일도 삭제하지 않음 (데이터 보존)

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

# progressr 핸들러 설정: 터미널 출력 비활성화 (Bash 프로그래스 바와 충돌 방지)
progressr::handlers(global = TRUE)
options(progressr.enable = TRUE)  # 프로그레스 바 강제 활성화
# 터미널 출력은 비활성화하여 Bash 프로그래스 바와의 충돌 방지 (로그 파일에는 기록됨)
progressr::handlers(progressr::handler_void())  # 터미널 출력 비활성화

# [v7.0 신규] 사전 취합 헬퍼 함수
# [engine_v3 수정] 로그 청크 파일은 병합 후 삭제 (engine_v2 방식 복원)
# - 로그 청크 파일들(jobs, system_failures, stat_failures, stat_excluded)은 이어쓰기 정보이므로 병합 후 삭제
# - 결과물 청크 파일들(hr_chunk, map_chunk)은 이 함수와 무관하며 별도 스크립트에서 처리
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
                # [설명] 병합 실패 시 청크 파일을 삭제하는 이유:
                # - 청크 파일은 로그 정보(작업 완료 기록)만 담고 있으며, 실제 결과물(HR 결과, 매핑 등)은 별도 파일에 저장됨
                # - 병합 실패 = 중앙 DB에 완료 기록이 없음 = 다음 사이클에서 "미완료"로 간주됨
                # - 청크 파일 삭제 후 다음 사이클에서 작업이 재실행되면, 결과물도 동일하게 생성되어 덮어씌워짐
                # - 따라서 데이터 손실 없이 안전하게 재실행 가능
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
        # [v11.3 수정] suppressWarnings 제거 및 삭제 실패 추적 추가
        # - 기존: suppressWarnings(file.remove(...))로 삭제 실패를 무시
        # - 문제: 파일 삭제 실패를 경고로만 처리하여 실제 실패 여부를 확인할 수 없음
        # - 해결: file.remove()의 반환값(TRUE/FALSE)을 확인하여 삭제 성공/실패 추적
        # - 안전성: 삭제 실패 시 경고 메시지 출력
        # - 참고: 로그 청크 파일만 삭제하므로 데이터 무결성에 영향 없음
        if (length(successfully_processed_files) > 0) {
            # file.remove()의 반환값 확인하여 삭제 성공 여부 추적
            removal_results <- file.remove(successfully_processed_files)
            successful_removals <- sum(removal_results)
            failed_removals <- sum(!removal_results)
            
            if (!silent) {
                if (failed_files > 0) {
                    cat(sprintf("  ℹ️  성공한 청크 파일 %d개 삭제, 실패한 청크 파일 %d개 삭제 완료\n",
                               successful_removals, failed_files))
                } else {
                    cat(sprintf("  ℹ️  청크 파일 %d개 모두 취합 완료 및 삭제\n", successful_removals))
                }
                
                if (failed_removals > 0) {
                    cat(sprintf("  ⚠️  경고: %d개 파일 삭제 실패 (수동 삭제 필요할 수 있음)\n", failed_removals))
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

# 메인 실행 함수 (engine_v1 - 작업 단위 직접 할당 + 자동 체크포인트 + 예열 옵션)
main <- function(
    paths = paths, 
    fu, 
    n_cores = 15, 
    batch_size = 500,
    chunks_per_core = 3, # 코어당 청크 수 (기본값: 3, Spark/Dask 표준, 4-5로 증가 시 부하 균형 향상)
    warming_up = FALSE
) {

    total_start_time <- Sys.time()
    
    # --- [engine_v3 신규] paths를 fu에 따라 동적으로 생성 ---
    paths <- list(
        matched_sas_folder = "/home/hashjamm/project_data/disease_network/sas_files/hr_project/matched_date/",
        matched_parquet_folder = "/home/hashjamm/project_data/disease_network/matched_date_parquet/",
        outcome_sas_file = "/home/hashjamm/project_data/disease_network/sas_files/hr_project/hr_std_pop10.sas7bdat",
        outcome_parquet_file = "/home/hashjamm/project_data/disease_network/outcome_table.parquet",
        results_hr_folder = sprintf("/home/hashjamm/results/disease_network/hr_results_fu%d/", fu),
        results_mapping_folder = sprintf("/home/hashjamm/results/disease_network/hr_mapping_results_fu%d/", fu),
        db_completed_folder = sprintf("/home/hashjamm/results/disease_network/hr_job_queue_db_fu%d/completed_jobs/", fu),
        db_completed_file = sprintf("/home/hashjamm/results/disease_network/hr_job_queue_db_fu%d/completed_jobs.duckdb", fu),
        db_system_failed_folder = sprintf("/home/hashjamm/results/disease_network/hr_job_queue_db_fu%d/system_failed_jobs/", fu),
        db_system_failed_file = sprintf("/home/hashjamm/results/disease_network/hr_job_queue_db_fu%d/system_failed_jobs.duckdb", fu),
        db_stat_failed_folder = sprintf("/home/hashjamm/results/disease_network/hr_job_queue_db_fu%d/stat_failed_jobs/", fu),
        db_stat_failed_file = sprintf("/home/hashjamm/results/disease_network/hr_job_queue_db_fu%d/stat_failed_jobs.duckdb", fu),
        db_stat_excluded_folder = sprintf("/home/hashjamm/results/disease_network/hr_job_queue_db_fu%d/stat_excluded_jobs/", fu),
        db_stat_excluded_file = sprintf("/home/hashjamm/results/disease_network/hr_job_queue_db_fu%d/stat_excluded_jobs.duckdb", fu)
    )
    
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
    # 4. [engine_v3 신규] stat_excluded 로그 취합
    pre_aggregate_logs(
        chunk_folder = paths$db_stat_excluded_folder,
        central_db_path = paths$db_stat_excluded_file,
        pattern = "stat_excluded_chunk_.*\\.duckdb",
        table_name = "stat_excluded",
        create_sql = "CREATE TABLE IF NOT EXISTS stat_excluded (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, reason VARCHAR, timestamp DATETIME, PRIMARY KEY (cause_abb, outcome_abb, fu, reason))"
    )
    # -----------------------------------------

    cat("분석 대상 질병 코드 로드 중...\n")
    disease_codes <- get_disease_codes_from_path(paths$matched_parquet_folder)
    cat(sprintf("   - %d개 유효 코드 확인.\n", length(disease_codes)))

    # --- 단계 2: 핵심 병렬 분석 실행 (모든 배치 처리 시도) ---
    # remaining_jobs는 사이클 종료 시 취합 후에 계산됨 (984줄)
    run_hr_analysis(
        cause_list = disease_codes, 
        outcome_list = disease_codes, 
        fu = fu, 
        n_cores = n_cores, 
        batch_size = batch_size,
        chunks_per_core = chunks_per_core,
        matched_parquet_folder_path = paths$matched_parquet_folder,
        outcome_parquet_file_path = paths$outcome_parquet_file,
        results_hr_folder_path = paths$results_hr_folder,
        results_mapping_folder_path = paths$results_mapping_folder,
        db_completed_file_path = paths$db_completed_file, # '읽기'용
        db_completed_folder_path = paths$db_completed_folder, # '쓰기'용
        db_system_failed_folder_path = paths$db_system_failed_folder, # '쓰기'용
        db_stat_failed_folder_path = paths$db_stat_failed_folder, # '쓰기'용
        db_stat_excluded_folder_path = paths$db_stat_excluded_folder # '쓰기'용 (engine_v3 신규)
    )
    # remaining_jobs 계산은 사이클 종료 시 취합 후에 수행됨

    # --- 단계 3: '결과물' 취합 ---
    # [engine_v3 수정] aggregate_results_mappings 함수 호출 제거
    # - 기존: HR 결과 취합 및 Node 매핑 데이터 생성을 메인 프로세스에서 처리
    # - 변경: HR 결과 취합 및 Node 매핑 데이터 생성을 별도 스크립트로 분리
    # - 이유: 
    #   * 취합 실패 시 메인 프로세스에 영향 없음 (프로세스 분리)
    #   * 메모리 부담 감소 (HR 분석 직후 메모리 사용량이 높을 때 취합하지 않음)
    #   * 재시도 용이 (실패 시 독립적으로 재실행 가능)
    #   * Edge 매핑 취합과 동일한 패턴 유지
    # - 구현: 별도 스크립트에 HR 결과 취합 및 Node 매핑 로직 추가 예정
    # - 참고: 청크 파일들은 삭제하지 않으므로 언제든지 별도 스크립트로 취합 가능
    cat("\n--- [단계 3] 결과물 취합은 별도 스크립트로 처리됩니다 ---\n")
    cat("   - HR 결과 청크 파일: hr_chunk_*.duckdb\n")
    cat("   - Map 데이터 청크 파일: map_chunk_*.duckdb\n")

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
    pre_aggregate_logs( # stat_excluded 로그 (engine_v3 신규)
        chunk_folder = paths$db_stat_excluded_folder,
        central_db_path = paths$db_stat_excluded_file,
        pattern = "stat_excluded_chunk_.*\\.duckdb",
        table_name = "stat_excluded",
        create_sql = "CREATE TABLE IF NOT EXISTS stat_excluded (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, reason VARCHAR, timestamp DATETIME, PRIMARY KEY (cause_abb, outcome_abb, fu, reason))"
    )
    cat("--- [단계 4] 이번 사이클 로그 취합 완료 ---\n")
    
    # --- [수정] 작업 완료 여부 확인 (취합 후 정확한 값 계산) ---
    # instruction_list_all 계산 (remaining_jobs 계산에 필요)
    instruction_list_all <- tidyr::expand_grid(cause_abb = disease_codes, outcome_abb = disease_codes) %>%
        filter(cause_abb != outcome_abb) %>%
        mutate(fu = fu)
    
    # 사이클 종료 시 취합이 완료된 후 남은 작업 수 계산
    con_completed <- dbConnect(duckdb::duckdb(), dbdir = paths$db_completed_file, read_only = TRUE)
    completed_count <- dbGetQuery(con_completed, "SELECT COUNT(*) as cnt FROM jobs")$cnt
    dbDisconnect(con_completed, shutdown = TRUE)
    
    remaining_jobs <- nrow(instruction_list_all) - completed_count
    
    # --- [engine_v1 수정] 작업 완료 여부 확인 및 프로세스 종료 ---
    if (remaining_jobs == 0) {
        cat("\n--- 모든 작업이 완료되었습니다! ---\n")
        # 모든 작업 완료 시 최종 요약 출력을 위해 return하지 않음
    } else {
        # --- 메모리 파편화 방지를 위한 프로세스 종료 ---
        # 모든 배치 처리 시도, 크래시 시 sh 스크립트가 자동으로 재시작하여 이어서 진행
        cat("\n==========================================================\n")
        cat("--- 메모리 파편화 방지를 위한 프로세스 종료 ---\n")
        cat("==========================================================\n")
        cat(sprintf("남은 작업: %d개\n", remaining_jobs))
        cat("(크래시 또는 메모리 문제로 중단된 경우, sh 스크립트가 자동으로 재시작하여 이어서 진행)\n\n")
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
    cat(sprintf("stat_excluded 로그 (중앙): %s\n", paths$db_stat_excluded_file)) # [engine_v3 신규]
}

# --- 메인 실행 ---
# fu 파라미터: 환경 변수에서 읽기 (sh 스크립트에서 전달, 필수)
fu_env <- Sys.getenv("FU")
if (fu_env == "") {
    stop("ERROR: FU 환경 변수가 설정되지 않았습니다. sh 스크립트에서 fu 파라미터를 전달해주세요.")
}
fu <- as.integer(fu_env)
if (is.na(fu) || fu <= 0) {
    stop(sprintf("ERROR: 유효하지 않은 fu 값입니다: %s", fu_env))
}
result <- main(
    paths = paths, fu = fu, 
    n_cores = 60, # CPU 및 디스크 IO 부하
    batch_size = 100000, # 메모리 부하
    chunks_per_core = 50, # 코어당 청크 수 (기본값: 3, Spark/Dask 표준, 4-5로 증가 시 부하 균형 향상)
    warming_up = FALSE
)

# --- 종료 코드 설정 (셸 스크립트용) ---
# result가 0이면 모든 작업 완료, 0보다 크면 남은 작업 있음 (재시작 필요)
if (!is.null(result) && result > 0) {
    quit(save = "no", status = 1)  # 재시작 필요 (셸 스크립트가 계속 실행)
} else {
    quit(save = "no", status = 0)  # 모든 작업 완료
}
 