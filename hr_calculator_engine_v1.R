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
                batch_system_failed_jobs[[length(batch_system_failed_jobs) + 1]] <<- data.frame(
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

    # 메모리 정리 (메모리 파편화 방지)
    rm(batch_hr_results, batch_edge_pids, batch_edge_index, batch_edge_key,
       batch_completed_jobs, batch_system_failed_jobs, batch_stat_failed_jobs)
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
                        db_stat_failed_folder_path = db_stat_failed_folder_path
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

# progressr 핸들러 설정: 터미널 출력 비활성화 (Bash 프로그래스 바와 충돌 방지)
progressr::handlers(global = TRUE)
options(progressr.enable = TRUE)  # 프로그레스 바 강제 활성화
# 터미널 출력은 비활성화하여 Bash 프로그래스 바와의 충돌 방지 (로그 파일에는 기록됨)
progressr::handlers(progressr::handler_void())  # 터미널 출력 비활성화

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

# 메인 실행 함수 (v11.0 - 작업 단위 직접 할당 + 자동 체크포인트 + 예열 옵션)
main <- function(
    paths = paths, 
    fu, 
    n_cores = 15, 
    batch_size = 500,
    chunks_per_core = 3, # 코어당 청크 수 (기본값: 3, Spark/Dask 표준, 4-5로 증가 시 부하 균형 향상)
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
        db_stat_failed_folder_path = paths$db_stat_failed_folder # '쓰기'용
    )
    # remaining_jobs 계산은 사이클 종료 시 취합 후에 수행됨

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
    
    # --- [v11.0 수정] 작업 완료 여부 확인 및 프로세스 종료 ---
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
}

# --- 메인 실행 ---
result <- main(
    paths = paths, fu = 10, 
    n_cores = 45, # CPU 및 디스크 IO 부하
    batch_size = 100000, # 메모리 부하
    chunks_per_core = 8, # 코어당 청크 수 (기본값: 3, Spark/Dask 표준, 4-5로 증가 시 부하 균형 향상)
    warming_up = FALSE
)

# --- 종료 코드 설정 (셸 스크립트용) ---
# result가 0이면 모든 작업 완료, 0보다 크면 남은 작업 있음 (재시작 필요)
if (!is.null(result) && result > 0) {
    quit(save = "no", status = 1)  # 재시작 필요 (셸 스크립트가 계속 실행)
} else {
    quit(save = "no", status = 0)  # 모든 작업 완료
}
 