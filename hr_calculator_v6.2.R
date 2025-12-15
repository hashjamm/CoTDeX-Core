# ============================================================================
# HR Calculator - 최종 아키텍처 v6.0 (DuckDB I/O + Job Queue 전략)
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
#    - 중앙 'completed_jobs.duckdb'와 'failed_jobs.duckdb'를 통해 '이어하기'를 완벽하게 지원합니다.
#
# 4. '하이브리드 로그' (v5.0의 안정성):
#    - (성공 로그): 중앙 'completed_jobs.duckdb'에 즉시 기록 (이어하기 보장)
#    - (실패 로그): 분산 'failed_chunk_PID.duckdb'에 기록 (잠금 충돌 방지)
#    - (결과 버퍼링): 분산 'hr/map_chunk_PID.duckdb'에 기록 (Small File Problem 해결)
#
# 5. 안정적인 'multisession' (v4.0):
#    - RAM 공유(COW)가 필요 없으므로, 안정적인 'multisession'을 사용합니다.
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
# 3단계: 핵심 병렬 처리 모듈 (Core Parallel Worker)
# ============================================================================

# 단일 (Cause, Outcome) 쌍을 처리하는, 병렬 작업자(worker)가 실행할 함수
process_one_pair <- function(
    cause_abb, 
    outcome_abb, 
    fu, 
    matched_parquet_folder_path, 
    outcome_parquet_file_path, # v4.0과 동일하게 '파일 경로' 사용
    results_hr_folder_path, # HR 청크 저장용
    results_mapping_folder_path, # Mapping 청크 저장용
    db_completed_file_path # 중앙 성공 로그 경로
    ) {
    
    # data.table 내부 스레딩 비활성화 (future와 충돌 방지)
    setDTthreads(1)

    # 1. DuckDB로 필요한 최소 데이터만 디스크에서 직접 로드 -> 아예 R 메모리 제로로 진행
    con <- dbConnect(duckdb::duckdb())
    on.exit(dbDisconnect(con, shutdown = TRUE)) # 함수 종료 시 항상 DB 연결 해제

    matched_parquet_file_path <- file.path(matched_parquet_folder_path, sprintf("matched_%s.parquet", tolower(cause_abb)))
    
    # Outcome이 발생한 사람과 그렇지 않은 사람을 모두 포함하기 위해 LEFT JOIN 사용
    query <- glue::glue("
        SELECT m.*, o.recu_fr_dt, o.abb_sick, o.key_seq
        FROM read_parquet('{matched_parquet_file_path}') AS m
        LEFT JOIN (
            SELECT person_id, recu_fr_dt, abb_sick, key_seq 
            FROM read_parquet('{outcome_parquet_file_path}') 
            WHERE abb_sick = '{outcome_abb}'
        ) AS o ON m.person_id = o.person_id
    ")
    
    clean_data <- as.data.table(DBI::dbGetQuery(con, query))

    # 2. 데이터 전처리 (시간 계산 등)
    clean_data[, `:=`(
        index_date = as.IDate(index_date, format = "%Y%m%d"),
        death_date = as.IDate(paste0(dth_ym, "15"), format = "%Y%m%d"),
        end_date = as.IDate(paste0(2003 + fu, "1231"), format = "%Y%m%d"),
        event_date = as.IDate(recu_fr_dt, format = "%Y%m%d")
    )]
    
    # final_date 및 status 계산 (정확한 로직 적용)
    clean_data[, final_date := fifelse(
        !is.na(event_date),
        pmin(event_date, end_date, na.rm = TRUE),
        pmin(death_date, end_date, na.rm = TRUE)
    )]
    clean_data[, status := fifelse(
        !is.na(event_date),
        fifelse(event_date <= final_date, 1, 0), 
        fifelse(!is.na(death_date) & death_date <= final_date, 2, 0)
    )]
    
    clean_data[, diff := final_date - index_date]
    
    # diff < 0 인 matched_id 그룹 전체 제거
    problem_ids <- clean_data[diff < 0, unique(matched_id)]
    if (length(problem_ids) > 0) {
        clean_data <- clean_data[!matched_id %in% problem_ids]
    }
    
    # (엣지 케이스: v5에서 가져온 안정성 코드)
    if(nrow(clean_data) == 0) {
        con_completed <- dbConnect(duckdb::duckdb(), dbdir = db_completed_file_path, read_only = FALSE)
        dbWriteTable(
            con_completed, 
            "jobs", 
            data.frame(cause_abb = cause_abb, outcome_abb = outcome_abb, fu = fu), 
            append = TRUE
        )
        dbDisconnect(con_completed, shutdown = TRUE)
        return(TRUE)
    }

    # --- 3. HR 분석 수행 (v6.0 최적화 버전) ---
    hr_result <- perform_hr_analysis(clean_data, fu, cause_abb, outcome_abb)

    # --- 4. Worker-Local Buffering (v5 방식) ---
    # v4.0의 write_parquet/saveRDS 대신 청크 방식 사용
    worker_pid <- Sys.getpid()
    
    # 4-1. HR 결과 버퍼링
    db_hr_path <- file.path(results_hr_folder_path, sprintf("hr_chunk_%s.duckdb", worker_pid))
    con_hr <- dbConnect(duckdb::duckdb(), dbdir = db_hr_path, read_only = FALSE)
    dbWriteTable(con_hr, "hr_results", hr_result, append = TRUE) 
    dbDisconnect(con_hr, shutdown = TRUE)
    
    # 4-2. Edge 매핑 데이터 버퍼링
    key <- paste(cause_abb, outcome_abb, fu, sep = "_")
    edge_slice <- list(
        pids = clean_data[case == 1, .(person_id)],
        index_key_seq = clean_data[case == 1, .(index_key_seq)],
        key_seq = clean_data[case == 1 & status == 1, .(key_seq)]
        )
    
    pids_df <- data.frame(key = key, person_id = unlist(edge_slice$pids, use.names = FALSE))
    idx_df <- data.frame(key = key, index_key_seq = unlist(edge_slice$index_key_seq, use.names = FALSE))
    key_df <- data.frame(key = key, outcome_key_seq = unlist(edge_slice$key_seq, use.names = FALSE))
    
    db_map_path <- file.path(results_mapping_folder_path, sprintf("map_chunk_%s.duckdb", worker_pid))
    con_map <- dbConnect(duckdb::duckdb(), dbdir = db_map_path, read_only = FALSE)
    dbWriteTable(con_map, "edge_pids", pids_df, append = TRUE)
    dbWriteTable(con_map, "edge_index_key_seq", idx_df, append = TRUE)
    dbWriteTable(con_map, "edge_key_seq", key_df, append = TRUE)
    dbDisconnect(con_map, shutdown = TRUE)
    
    # --- 5. Job Queue 상태 '중앙' 업데이트 (v5.0 방식) ---
    con_completed <- dbConnect(duckdb::duckdb(), dbdir = db_completed_file_path, read_only = FALSE)
    dbWriteTable(
        con_completed, 
        "jobs", 
        data.frame(cause_abb = cause_abb, outcome_abb = outcome_abb, fu = fu), 
        append = TRUE
    )
    dbDisconnect(con_completed, shutdown = TRUE)
    
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
    matched_parquet_folder_path, 
    outcome_parquet_file_path, # v4.0과 동일하게 '파일 경로'
    results_hr_folder_path,
    results_mapping_folder_path,
    db_completed_file_path, # 중앙 성공 로그 경로
    db_failed_folder_path # 실패 로그 경로
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
        return()
    }
    # --------------------------------

    # --- 병렬 처리 설정 및 실행 ---
    plan(multisession, workers = n_cores)
    required_packages <- c("data.table", "duckdb", "DBI", "arrow", "survival", "broom", "tidycmprsk", "dplyr", "glue", "digest")
    
    progressr::with_progress({
        p <- progressr::progressor(steps = nrow(jobs_to_do))
        
        future_walk(1:nrow(jobs_to_do), function(i) {
            current_cause <- jobs_to_do$cause_abb[i]
            current_outcome <- jobs_to_do$outcome_abb[i]
            
            tryCatch({
                process_one_pair(
                    current_cause, current_outcome, fu,
                    matched_parquet_folder_path,
                    outcome_parquet_file_path,
                    results_hr_folder_path,
                    results_mapping_folder_path,
                    db_completed_file_path
                )
            }, error = function(e) {
                # [v6.0] '실패 로그' 분산 저장
                cat(sprintf("\nERROR in %s -> %s: %s\n", current_cause, current_outcome, e$message))
                
                worker_pid <- Sys.getpid()
                db_failed_chunk_path <- file.path(db_failed_folder_path, sprintf("failed_chunk_%s.duckdb", worker_pid))

                con_failed <- dbConnect(duckdb::duckdb(), dbdir = db_failed_chunk_path, read_only = FALSE)
                dbExecute(con_failed, "CREATE TABLE IF NOT EXISTS failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME DEFAULT NOW())")
                dbWriteTable(
                    con_failed, 
                    "failures", 
                    data.frame(
                        cause_abb = current_cause, 
                        outcome_abb = current_outcome, 
                        fu = fu, 
                        error_msg = as.character(e$message)
                    ), 
                    append = TRUE
                )
                dbDisconnect(con_failed, shutdown = TRUE)
            })
            p()
        }, .options = furrr_options(seed = TRUE, packages = required_packages))
    })
    
    plan(sequential)
    cat("\n--- [단계 1] 핵심 병렬 분석 완료 ---\n")
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

aggregate_mappings <- function(
    cause_list,
    fu, 
    matched_parquet_folder_path, 
    results_hr_folder_path,
    results_mapping_folder_path
    ) {
        cat("\n--- [단계 2] 최종 데이터 취합 시작 ---\n")
        
        # --- 1. HR 결과 취합 (청크 파일 병합) ---
        cat("1. HR 결과(DuckDB 청크) 취합 중...\n")
        hr_chunk_files <- list.files(results_hr_folder_path, pattern = "hr_chunk_.*\\.duckdb", full.names = TRUE)
        
        if (length(hr_chunk_files) > 0) {
            con_agg <- dbConnect(duckdb::duckdb())
            
            # 첫 번째 테이블을 기준으로 main 테이블 생성
            dbExecute(con_agg, sprintf("ATTACH '%s' AS db1 (READ_ONLY)", hr_chunk_files[1]))
            dbExecute(con_agg, "CREATE TABLE combined_hr AS SELECT * FROM db1.hr_results")
            dbExecute(con_agg, "DETACH db1")
            
            # 나머지 파일들 INSERT
            if (length(hr_chunk_files) > 1) {
                for (i in 2:length(hr_chunk_files)) {
                    dbExecute(con_agg, sprintf("ATTACH '%s' AS db%d (READ_ONLY)", hr_chunk_files[i], i))
                    dbExecute(con_agg, sprintf("INSERT INTO combined_hr SELECT * FROM db%d.hr_results", i))
                    dbExecute(con_agg, sprintf("DETACH db%d", i))
                }
            }
            
            # 중복 제거 (혹시 모를 재시작 오류 대비)
            cat("   - 중복 제거 중...\n")
            final_hr_table <- dbGetQuery(con_agg, "SELECT DISTINCT * FROM combined_hr")
            
            # 최종 Parquet으로 저장
            final_hr_path <- file.path(results_hr_folder, sprintf("total_hr_results_%d.parquet", fu))
            arrow::write_parquet(final_hr_table, final_hr_path)
            
            dbDisconnect(con_agg, shutdown = TRUE)
            cat(sprintf("     ✓ HR 결과 취합 완료: %s\n", basename(final_hr_path)))
        } else {
            cat("   - 취합할 HR 청크 파일이 없습니다.\n")
        }

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

        # --- 3. Edge 매핑 데이터 취합 (청크 파일 병합) ---
        cat("\n3. Edge 매핑 데이터(DuckDB 청크) 취합 중...\n")
        map_chunk_files <- list.files(results_mapping_folder, pattern = "map_chunk_.*\\.duckdb", full.names = TRUE)

        if (length(map_chunk_files) > 0) {
            cat(sprintf("   - %d개의 Edge 데이터 청크를 취합합니다.\n", length(map_chunk_files)))
            con_map_agg <- dbConnect(duckdb::duckdb())
            
            # 3개 테이블(pids, index, key)에 대해 ATTACH 및 INSERT 수행
            # (테이블이 비어있을 수 있으므로 유연하게 처리)
            dbExecute(con_map_agg, "CREATE TABLE combined_pids (key VARCHAR, person_id BIGINT)")
            dbExecute(con_map_agg, "CREATE TABLE combined_index (key VARCHAR, index_key_seq BIGINT)")
            dbExecute(con_map_agg, "CREATE TABLE combined_key (key VARCHAR, outcome_key_seq BIGINT)")

            for (i in 1:length(map_chunk_files)) {
                dbExecute(con_map_agg, sprintf("ATTACH '%s' AS db%d (READ_ONLY)", map_chunk_files[i], i))
                
                # 각 테이블이 존재하는지 확인 후 INSERT
                tables_in_chunk <- dbListTables(con_map_agg)
                if ("edge_pids" %in% tables_in_chunk) {
                    dbExecute(con_map_agg, sprintf("INSERT INTO combined_pids SELECT * FROM db%d.edge_pids", i))
                }
                if ("edge_index_key_seq" %in% tables_in_chunk) {
                    dbExecute(con_map_agg, sprintf("INSERT INTO combined_index SELECT * FROM db%d.edge_index_key_seq", i))
                }
                if ("edge_key_seq" %in% tables_in_chunk) {
                    dbExecute(con_map_agg, sprintf("INSERT INTO combined_key SELECT * FROM db%d.edge_key_seq", i))
                }
                
                dbExecute(con_map_agg, sprintf("DETACH db%d", i))
            }

            # DuckDB의 LIST() 집계 함수로 (key, long_vector) 맵핑 생성
            cat("   - Long format을 List format으로 변환 중...\n")
            edge_pids_df <- dbGetQuery(con_map_agg, "SELECT key, LIST(person_id) as values FROM (SELECT DISTINCT * FROM combined_pids) GROUP BY key")
            edge_index_df <- dbGetQuery(con_map_agg, "SELECT key, LIST(index_key_seq) as values FROM (SELECT DISTINCT * FROM combined_index) GROUP BY key")
            edge_key_df <- dbGetQuery(con_map_agg, "SELECT key, LIST(outcome_key_seq) as values FROM (SELECT DISTINCT * FROM combined_key) GROUP BY key")
            
            dbDisconnect(con_map_agg, shutdown = TRUE)

            # 데이터프레임(key, values)을 명명된 리스트(named list)로 변환
            edge_pids_list <- setNames(edge_pids_df$values, edge_pids_df$key)
            edge_index_key_seq_list <- setNames(edge_index_df$values, edge_index_df$key)
            edge_key_seq_list <- setNames(edge_key_df$values, edge_key_df$key)

            # 헬퍼 함수를 사용해 Parquet으로 저장
            save_mapping_to_parquet(edge_pids_list, "edge_pids", results_mapping_folder_path, fu)
            save_mapping_to_parquet(edge_index_key_seq_list, "edge_index_key_seq", results_mapping_folder_path, fu)
            save_mapping_to_parquet(edge_key_seq_list, "edge_key_seq", results_mapping_folder_path, fu)
            
            rm(edge_pids_list, edge_index_key_seq_list, edge_key_seq_list, edge_pids_df, edge_index_df, edge_key_df); gc()
            
        } else {
            cat("   - 취합할 Edge 청크 파일이 없습니다.\n")
        }
        
        # --- 4. [v7] 임시 청크 파일 삭제 ---
        cat("\n4. 임시 청크 파일 삭제 중...\n")
        suppressWarnings(file.remove(hr_chunk_files))
        suppressWarnings(file.remove(map_chunk_files))
        
        cat("--- [단계 2] 최종 데이터 취합 완료 ---\n")
}

# ============================================================================
# 6. 스크립트 실행 (Script Execution)
# ============================================================================

# 질병 코드 목록을 가져오는 유틸리티 함수
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
        results_hr_folder = "/home/hashjamm/results/disease_network/hr_results_v5/",
        results_mapping_folder = "/home/hashjamm/results/disease_network/hr_mapping_results_v5/",
        db_completed_file = "/home/hashjamm/results/disease_network/hr_job_queue_db/completed_jobs.duckdb",
        db_failed_folder = "/home/hashjamm/results/disease_network/hr_job_queue_db/failed_jobs/"
    )

handlers(handler_progress(format = "[:bar] :current/:total (:percent) | ETA: :eta"))

# 메인 실행 함수
main <- function(paths = paths, fu, n_cores = 90) {

    total_start_time <- Sys.time()
    
    # --- [추가] OS 디스크 캐시 '수동 예열' 단계 ---
    cat("\n--- OS 디스크 캐시 예열 시작 ---\n")
    cat("공통 데이터(outcome_table)를 1회 읽어 캐시에 올립니다...\n")
    temp_data <- read_parquet(paths$outcome_parquet_file)
    dplyr::collect(temp_data)
    rm(temp_data)
    gc()
    cat("--- 예열 완료 ---\n\n")
    # -----------------------------------------

    # 1. 분석 대상 질병 코드(1187개) 먼저 로드
    cat("분석 대상 질병 코드(Cause/Outcome) 로드 중...\n")
    disease_codes <- get_disease_codes_from_path(file.path(paths$matched_parquet_folder))
    cat(sprintf("   - %d개 유효 코드 확인.\n", length(disease_codes)))

    # --- 실행 순서 ---
    
    # 1. 핵심 병렬 분석 실행
    run_hr_analysis(
        disease_codes, disease_codes, fu, n_cores,
        matched_parquet_folder_path = paths$matched_parquet_folder,
        outcome_parquet_file_path = paths$outcome_parquet_file,
        results_hr_folder_path = paths$results_hr_folder,
        results_mapping_folder_path = paths$results_mapping_folder,
        db_completed_file_path = paths$db_completed_file,
        db_failed_folder_path = paths$db_failed_folder
    )
    
    # 2. 최종 데이터 취합
    aggregate_results(
        disease_codes, fu,
        matched_parquet_folder_path = paths$matched_parquet_folder,
        results_hr_folder_path = paths$results_hr_folder,
        results_mapping_folder_path = paths$results_mapping_folder
    )
    
    # --- 최종 요약 ---
    total_elapsed <- as.numeric(difftime(Sys.time(), total_start_time, units = "hours"))
    cat(sprintf("\n모든 작업 완료! 총 소요 시간: %.2f시간 (%.1f일)\n", total_elapsed, total_elapsed/24))
    cat(sprintf("HR 결과물 위치: %s\n", file.path(paths$results_hr_folder, sprintf("total_hr_results_%d.parquet", fu))))
    cat(sprintf("매핑 결과물 위치: %s\n", paths$results_mapping_folder))
    cat(sprintf("실패한 작업 로그: %s\n", paths$db_failed_folder))
}

main(paths = paths, fu = 10, n_cores = 45)

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

#' 개별 워커(PID)의 실패 로그 청크 확인
#'
#' @param log_folder_path failed_jobs 청크 파일들이 저장되는 폴더 경로
#' @param pid 확인할 워커의 PID (숫자 또는 문자열)
#' @param n 반환할 행의 수 (기본 10)
#' @param tail TRUE면 마지막 n개 (최신순), FALSE면 처음 n개 (오래된순)
#'
check_failed_chunk <- function(log_folder_path, pid, n = 10, tail = TRUE) {
    log_file_path <- file.path(log_folder_path, sprintf("failed_chunk_%s.duckdb", pid))
    
    if (!file.exists(log_file_path)) {
        cat(sprintf("\n--- [실패 로그] PID %s에 대한 실패 로그 청크 없음 ---\n", pid))
        return(invisible(NULL))
    }
    
    con <- dbConnect(duckdb::duckdb(), dbdir = log_file_path, read_only = TRUE)
    on.exit(dbDisconnect(con, shutdown = TRUE))
    
    if (!"failures" %in% dbListTables(con)) {
        cat("로그 파일에 'failures' 테이블이 없습니다.\n")
        return(invisible(NULL))
    }
    
    total_rows <- dbGetQuery(con, "SELECT COUNT(*) FROM failures")[1, 1]
    
    if (tail) {
        # timestamp 기준으로 정렬
        query <- sprintf("SELECT * FROM failures ORDER BY timestamp DESC LIMIT %d", n)
        cat(sprintf("\n--- [실패 로그] PID %s의 최근 %d개 오류 (총 %d개) ---\n", pid, n, total_rows))
    } else {
        query <- sprintf("SELECT * FROM failures ORDER BY timestamp ASC LIMIT %d", n)
        cat(sprintf("\n--- [실패 로그] PID %s의 최초 %d개 오류 (총 %d개) ---\n", pid, n, total_rows))
    }
    
    data <- dbGetQuery(con, query)
    print(data)
    invisible(data)
}

# --- 사용 예시 ---
# check_success_log(paths$db_completed_file)
# check_success_log(paths$db_completed_file, n = 5, tail = FALSE)

# (htop에서 오류를 뿜는 R 프로세스의 PID가 97336이라고 가정)
# check_failed_chunk(paths$db_failed_folder, pid = 97336)
# check_failed_chunk(paths$db_failed_folder, pid = 97336, n = 3)

check_failed_chunk(paths$db_failed_folder, pid = 33336)
