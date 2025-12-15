# ============================================================================
# hr_rr_mapping_validation_engine_v3.R
# ============================================================================
# 
# 이 파일은 hr_calculator_engine_v4.R을 기반으로 작성되었습니다.
# 
# 배경:
# - 이전 버전(hr_rr_mapping_validation_engine_v2.R)까지는 LLM을 이용하여 hr engine을 계승한 코드였으나
#   계속 자잘한 문제가 발생하기도 했고, 특히 프로그레스 바 문제가 해결되지 않았습니다.
# - 따라서 근본적 해결책으로서 hr_calculator_engine_v4.R을 기반으로 직접 수정하기 위해
#   hr_calculator_engine_v4.R을 복사하여 그 코드로부터 수정하는 방식으로 작성되었습니다.
# 
# 주요 목적:
# - diff < 0 인 matched_id를 가진 edge pids를 얻는 코드
#   (전처리 과정에서 diff = final_date - index_date가 음수인 경우를 필터링하여
#    해당 matched_id에 속하는 edge pids를 추출)
# 
# ============================================================================

# 1. 라이브러리 로드
library(haven)
library(dplyr)
library(tidyverse)
library(arrow)
library(glue)      # DuckDB 쿼리를 위해 'glue' 복원
library(future)
library(furrr)
library(future.apply)
library(progressr)
library(data.table)
library(duckdb)
library(DBI)

# ============================================================================
# 3단계: 배치 처리 모듈 (Batch Processing Module)
# ============================================================================

# N개의 작업을 처리하고 결과를 RAM에 모았다가 한 번에 디스크에 쓰는 함수
process_batch <- function(
    batch_jobs, # data.table: 처리할 작업 목록 (N개 행)
    fu,
    matched_parquet_folder_path,
    outcome_parquet_file_path,
    results_mapping_folder_path,
    db_completed_folder_path, # 성공 로그 청크 폴더
    db_system_failed_folder_path # 시스템 실패 로그 청크 폴더
) {
    # 워커별 프로그레스 바 설정 강제 활성화 (future multisession은 별도 프로세스이므로 옵션 전달 필요)
    # handlers는 이미 메인에서 설정했으므로 옵션만 설정 (중복 호출 시 "handlers on the stack" 에러 발생)
    if (!isTRUE(getOption("progressr.enable"))) {
        options(progressr.enable = TRUE)
    }
    
    # 워커별 청크 파일 경로 설정 (v7.txt 방식)
    worker_pid <- Sys.getpid()
    db_map_chunk_path <- file.path(results_mapping_folder_path, sprintf("map_chunk_%s.duckdb", worker_pid))
    db_completed_chunk_path <- file.path(db_completed_folder_path, sprintf("completed_chunk_%s.duckdb", worker_pid))
    db_system_failed_chunk_path <- file.path(db_system_failed_folder_path, sprintf("system_failed_chunk_%s.duckdb", worker_pid))

    # RAM에 결과를 모을 임시 리스트 초기화
    batch_negative_edge_pids <- list() # diff < 0인 matched_id를 가진 person_id
    batch_completed_jobs <- list()
    batch_system_failed_jobs <- list()

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
                
                # --- 3. negative_edge 매핑 데이터 수집 (diff < 0인 matched_id를 가진 person_id) ---
                problem_ids <- clean_data[diff < 0, unique(matched_id)]
                if (length(problem_ids) > 0) {
                    # diff < 0인 matched_id를 가진 모든 person_id 수집
                    negative_pids <- clean_data[matched_id %in% problem_ids, .(person_id)]
                    key <- paste(cause_abb, outcome_abb, sep = "_")
                    if (nrow(negative_pids) > 0) {
                        batch_negative_edge_pids[[length(batch_negative_edge_pids) + 1]] <- data.frame(key = key, person_id = negative_pids$person_id)
                    }
                }
                
                # --- 4. 완료 로그 RAM 리스트에 추가 ---
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

    # --- [신규] 데이터 일관성 검증: negative_edge_pids와 completed_keys 일치 확인 ---
    if (length(batch_completed_jobs) > 0 && length(batch_negative_edge_pids) > 0) {
        completed_df <- bind_rows(batch_completed_jobs)
        completed_keys <- paste(completed_df$cause_abb, completed_df$outcome_abb, sep = "_")
        
        negative_edge_keys <- unique(unlist(lapply(batch_negative_edge_pids, function(x) x$key)))
        
        # negative_edge_pids가 있지만 completed_keys에 없는 경우 감지
        missing_in_completed <- setdiff(negative_edge_keys, completed_keys)
        if (length(missing_in_completed) > 0) {
            cat("\n==========================================================\n")
            cat("ERROR: negative_edge_pids와 completed_keys 불일치 감지\n")
            cat("==========================================================\n")
            cat(sprintf("발견된 불일치 조합 수: %d개\n\n", length(missing_in_completed)))
            cat("상세 내역:\n")
            cat("  negative_edge_pids 총 건수:", length(negative_edge_keys), "\n")
            cat("  completed_keys 총 건수:", length(completed_keys), "\n")
            cat("  불일치 조합 수:", length(missing_in_completed), "\n\n")
            cat("불일치 조합 목록 (처음 20개):\n")
            missing_sorted <- sort(missing_in_completed)
            for (i in 1:min(20, length(missing_sorted))) {
                cat(sprintf("  %d. %s\n", i, missing_sorted[i]))
            }
            if (length(missing_in_completed) > 20) {
                cat(sprintf("  ... (총 %d개 중 20개만 표시)\n", length(missing_in_completed)))
            }
            cat("\n==========================================================\n")
            cat("원인: negative_edge_pids가 있지만 completed_keys에 없는 조합이 발견되었습니다.\n")
            cat("      로직 오류가 있는 것으로 보입니다.\n")
            cat("==========================================================\n")
            flush.console()
            
            stop(sprintf(
                "ERROR: negative_edge_pids가 있지만 completed_keys에 없는 조합 %d개 발견. 실행을 중단합니다.",
                length(missing_in_completed)
            ))
        }
    }

    # --- 일괄 쓰기 (트랜잭션 관리) ---
    con_map <- if (length(batch_negative_edge_pids) > 0) dbConnect(duckdb::duckdb(), dbdir = db_map_chunk_path, read_only = FALSE) else NULL
    con_comp <- if (length(batch_completed_jobs) > 0) dbConnect(duckdb::duckdb(), dbdir = db_completed_chunk_path, read_only = FALSE) else NULL
    con_system_fail <- if (length(batch_system_failed_jobs) > 0) dbConnect(duckdb::duckdb(), dbdir = db_system_failed_chunk_path, read_only = FALSE) else NULL

    on.exit({
        if (!is.null(con_map)) dbDisconnect(con_map, shutdown = TRUE)
        if (!is.null(con_comp)) dbDisconnect(con_comp, shutdown = TRUE)
        if (!is.null(con_system_fail)) dbDisconnect(con_system_fail, shutdown = TRUE)
    })

    # 트랜잭션 시작
    if (!is.null(con_map)) dbExecute(con_map, "BEGIN TRANSACTION;")
    if (!is.null(con_comp)) dbExecute(con_comp, "BEGIN TRANSACTION;")
    if (!is.null(con_system_fail)) dbExecute(con_system_fail, "BEGIN TRANSACTION;")

    tryCatch({
        # 1. negative_edge 매핑 데이터 쓰기
        if (!is.null(con_map)) {
            dbExecute(con_map, "CREATE TABLE IF NOT EXISTS negative_edge_pids (key VARCHAR, person_id BIGINT);")
            if (length(batch_negative_edge_pids) > 0) {
                dbWriteTable(con_map, "negative_edge_pids", bind_rows(batch_negative_edge_pids), append = TRUE)
            }
            dbExecute(con_map, "COMMIT;")
        }
        
        # 2. 시스템 실패 로그 쓰기
        if (!is.null(con_system_fail)) {
            dbExecute(con_system_fail, "CREATE TABLE IF NOT EXISTS system_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME)")
            dbWriteTable(con_system_fail, "system_failures", bind_rows(batch_system_failed_jobs), append = TRUE)
            dbExecute(con_system_fail, "COMMIT;")
        }
        
        # 3. 완료 로그 쓰기 (마지막 - 모든 완료된 작업 기록)
        # batch_completed_jobs에 있는 모든 작업을 완료 로그에 기록
        # (negative_edge_pids가 있든 없든 모두 기록, system_failures는 batch_completed_jobs에 없으므로 기록 안 됨)
        if (!is.null(con_comp)) {
            # 완료 로그에 기록할 작업 필터링
            completed_keys <- if (length(batch_completed_jobs) > 0) {
                completed_df <- bind_rows(batch_completed_jobs)
                paste(completed_df$cause_abb, completed_df$outcome_abb, sep = "_")
            } else {
                character(0)
            }
            
            # negative_edge_pids 키 추출 (있을 경우)
            negative_edge_keys <- if (length(batch_negative_edge_pids) > 0) {
                unique(unlist(lapply(batch_negative_edge_pids, function(x) x$key)))
            } else {
                character(0)
            }
            
            # 시스템 실패 로그 키 추출 (있을 경우)
            system_fail_keys <- if (length(batch_system_failed_jobs) > 0) {
                sapply(batch_system_failed_jobs, function(x) paste(x$cause_abb, x$outcome_abb, sep = "_"))
            } else {
                character(0)
            }
            
            # 모든 완료된 작업을 완료 로그에 기록
            valid_completed_keys <- completed_keys
            
            if (length(valid_completed_keys) > 0) {
                # 키를 다시 분리하여 데이터프레임 생성
                valid_parts <- strsplit(valid_completed_keys, "_")
                valid_completed_jobs <- data.frame(
                    cause_abb = sapply(valid_parts, function(x) x[1]),
                    outcome_abb = sapply(valid_parts, function(x) x[2]),
                    fu = fu  # fu는 기본값 1 또는 전달된 값 사용
                )
                
                dbExecute(con_comp, "CREATE TABLE IF NOT EXISTS jobs (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER)")
                dbWriteTable(con_comp, "jobs", valid_completed_jobs, append = TRUE)
                dbExecute(con_comp, "COMMIT;")
            }
        }

    }, error = function(e) {
        # 쓰기 중 오류 발생 시 롤백 시도
        # 주의: 각 연결이 이미 개별적으로 커밋되었을 수 있으므로, 롤백이 실패할 수 있음
        # 하지만 일관성을 위해 롤백 시도는 유지
        warning("Batch write failed, attempting rollback: ", e$message)
        if (!is.null(con_map)) try(dbExecute(con_map, "ROLLBACK;"), silent = TRUE)
        if (!is.null(con_comp)) try(dbExecute(con_comp, "ROLLBACK;"), silent = TRUE)
        if (!is.null(con_system_fail)) try(dbExecute(con_system_fail, "ROLLBACK;"), silent = TRUE)
        stop(e) # 에러를 다시 던져서 상위 tryCatch가 잡도록 함
    })

    # 메모리 정리 (메모리 파편화 방지)
    rm(batch_negative_edge_pids, batch_completed_jobs, batch_system_failed_jobs)
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
    results_mapping_folder_path,
    db_completed_file_path, # 중앙 성공 로그 경로
    db_completed_folder_path, # 청크 성공 로그 폴더 경로
    db_system_failed_folder_path # 청크 시스템 실패 로그 경로
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
                        results_mapping_folder_path = results_mapping_folder_path,
                        db_completed_folder_path = db_completed_folder_path,
                        db_system_failed_folder_path = db_system_failed_folder_path
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
# 5. 스크립트 실행 (Script Execution)
# ============================================================================

# 질병 코드 목록을 가져오는 유틸리티 함수
get_disease_codes_from_path <- function(matched_parquet_folder_path) {
    codes <- toupper(gsub("matched_(.*)\\.parquet", "\\1", list.files(matched_parquet_folder_path)))
    return(sort(codes))
}

# progressr 핸들러 설정: 터미널 출력 비활성화 (Bash 프로그래스 바와 충돌 방지)
progressr::handlers(global = TRUE)
options(progressr.enable = TRUE)  # 프로그레스 바 강제 활성화
# 터미널 출력은 비활성화하여 Bash 프로그래스 바와의 충돌 방지 (로그 파일에는 기록됨)
progressr::handlers(progressr::handler_void())  # 터미널 출력 비활성화

# [v7.0 신규] 사전 취합 헬퍼 함수
# [engine_v3 수정] 로그 청크 파일은 병합 후 삭제 (engine_v2 방식 복원)
# - 로그 청크 파일들(jobs, system_failures)은 이어쓰기 정보이므로 병합 후 삭제
# - 결과물 청크 파일들(map_chunk)은 이 함수와 무관하며 별도 스크립트에서 처리
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
                
                # 중복 키 경고 출력
                chunk_count <- dbGetQuery(con_agg, sprintf("SELECT COUNT(*) FROM db%d.%s", i, table_name))[1, 1]
                if (inserted_count < chunk_count && !silent) {
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

# ============================================================================
# 헬퍼 함수: 필요한 디렉토리 생성
# ============================================================================
# 필요한 모든 디렉토리를 생성하는 함수
# - 이미 존재하는 디렉토리는 무시 (덮어쓰지 않음)
# - recursive = TRUE로 부모 디렉토리도 함께 생성
# - showWarnings = FALSE로 경고 메시지 숨김
#
# Args:
#     paths: 경로 정보를 담은 리스트
ensure_required_directories <- function(paths) {
    # 결과 폴더들 (이미 존재하는 경우 생성하지 않음)
    if (!dir.exists(paths$results_mapping_folder)) {
        dir.create(paths$results_mapping_folder, recursive = TRUE, showWarnings = FALSE)
    }
    
    # 로그 청크 폴더들 (이미 존재하는 경우 생성하지 않음)
    if (!dir.exists(paths$db_completed_folder)) {
        dir.create(paths$db_completed_folder, recursive = TRUE, showWarnings = FALSE)
    }
    if (!dir.exists(paths$db_system_failed_folder)) {
        dir.create(paths$db_system_failed_folder, recursive = TRUE, showWarnings = FALSE)
    }
    
    # 중앙 DB 파일의 부모 디렉토리들 (이미 존재하는 경우 생성하지 않음)
    completed_parent_dir <- dirname(paths$db_completed_file)
    if (!dir.exists(completed_parent_dir)) {
        dir.create(completed_parent_dir, recursive = TRUE, showWarnings = FALSE)
    }
    system_failed_parent_dir <- dirname(paths$db_system_failed_file)
    if (!dir.exists(system_failed_parent_dir)) {
        dir.create(system_failed_parent_dir, recursive = TRUE, showWarnings = FALSE)
    }
    
    invisible(NULL)  # 명시적으로 NULL 반환
}

# 메인 실행 함수 (engine_v1 - 작업 단위 직접 할당 + 자동 체크포인트 + 예열 옵션)
main <- function(
    fu = 1, 
    n_cores = 15, 
    batch_size = 500,
    chunks_per_core = 3, # 코어당 청크 수 (기본값: 3, Spark/Dask 표준, 4-5로 증가 시 부하 균형 향상)
    warming_up = FALSE
) {

    total_start_time <- Sys.time()
    
    # --- [engine_v3 신규] paths를 고정 경로로 생성 (fu 독립적) ---
    paths <- list(
        matched_sas_folder = "/home/hashjamm/project_data/disease_network/sas_files/hr_project/matched_date/",
        matched_parquet_folder = "/home/hashjamm/project_data/disease_network/matched_date_parquet/",
        outcome_sas_file = "/home/hashjamm/project_data/disease_network/sas_files/hr_project/hr_std_pop10.sas7bdat",
        outcome_parquet_file = "/home/hashjamm/project_data/disease_network/outcome_table.parquet",
        results_mapping_folder = "/home/hashjamm/results/disease_network/hr_rr_mapping_validation_results/",
        db_completed_folder = "/home/hashjamm/results/disease_network/hr_rr_mapping_validation_job_queue_db/completed_jobs/",
        db_completed_file = "/home/hashjamm/results/disease_network/hr_rr_mapping_validation_job_queue_db/completed_jobs.duckdb",
        db_system_failed_folder = "/home/hashjamm/results/disease_network/hr_rr_mapping_validation_job_queue_db/system_failed_jobs/",
        db_system_failed_file = "/home/hashjamm/results/disease_network/hr_rr_mapping_validation_job_queue_db/system_failed_jobs.duckdb"
    )
    
    # [engine_v4 신규] 필요한 디렉토리 생성 (이미 존재하면 무시)
    ensure_required_directories(paths)
    
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
        results_mapping_folder_path = paths$results_mapping_folder,
        db_completed_file_path = paths$db_completed_file, # '읽기'용
        db_completed_folder_path = paths$db_completed_folder, # '쓰기'용
        db_system_failed_folder_path = paths$db_system_failed_folder # '쓰기'용
    )
    # remaining_jobs 계산은 사이클 종료 시 취합 후에 수행됨

    # --- 단계 3: '결과물' 취합 ---
    # [engine_v3 수정] aggregate_results_mappings 함수 호출 제거
    # - negative_edge 매핑 데이터 취합은 별도 스크립트로 처리
    # - 청크 파일들은 삭제하지 않으므로 언제든지 별도 스크립트로 취합 가능
    cat("\n--- [단계 3] 결과물 취합은 별도 스크립트로 처리됩니다 ---\n")
    cat("   - negative_edge 매핑 데이터 청크 파일: map_chunk_*.duckdb\n")

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
    cat(sprintf("negative_edge 매핑 결과물 위치: %s\n", paths$results_mapping_folder))
    cat(sprintf("성공한 작업 로그 (중앙): %s\n", paths$db_completed_file))
    cat(sprintf("시스템 실패 로그 (중앙): %s\n", paths$db_system_failed_file))
}

# --- 메인 실행 ---
# fu 파라미터: 기본값 1 사용 (validation_engine은 fu 값과 무관하게 동일한 결과 생성)
# 환경 변수에서 읽기 (선택적, 없으면 기본값 1 사용)
fu_env <- Sys.getenv("FU")
if (fu_env != "") {
    fu <- as.integer(fu_env)
    if (is.na(fu) || fu <= 0) {
        warning(sprintf("경고: 유효하지 않은 FU 환경 변수 값입니다: %s. 기본값 1을 사용합니다.", fu_env))
        fu <- 1
    }
} else {
    fu <- 1  # 기본값 사용
}
result <- main(
    fu = fu, 
    n_cores = 60, # CPU 및 디스크 IO 부하 (안정성 우선: 60 → 40)
    batch_size = 50000, # 메모리 부하 (안정성 우선: 100000 → 50000)
    chunks_per_core = 30, # 코어당 청크 수 (안정성 우선: 20 → 10, 기본값: 3, Spark/Dask 표준)
    warming_up = FALSE
)

# --- 종료 코드 설정 (셸 스크립트용) ---
# result가 0이면 모든 작업 완료, 0보다 크면 남은 작업 있음 (재시작 필요)
if (!is.null(result) && result > 0) {
    quit(save = "no", status = 1)  # 재시작 필요 (셸 스크립트가 계속 실행)
} else {
    quit(save = "no", status = 0)  # 모든 작업 완료
}
 