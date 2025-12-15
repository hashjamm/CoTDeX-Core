# ============================================================================
# HR Calculator - 현대적 분산 컴퓨팅 패러다임 (Push-down Processing)
# ============================================================================
# 
# 혁신적 메모리 제로 전략:
# - 거대한 중간 데이터(base_data)를 메모리에 아예 생성하지 않음
# - 각 병렬 작업자가 필요한 최소한의 데이터만 디스크에서 직접 처리
# - 메인 프로세스는 작업 목록(Instruction List)만 생성, 데이터 로드 없음
# - 예상 메모리 사용: < 2GB (기존 126GB에서 98% 절감!)
#
# 현대적 데이터 처리 아키텍처:
# 1. 데이터 형식: Parquet (모든 .sas7bdat → .parquet 변환)
#    - 열(column) 기반 저장으로 필요한 열만 빠르게 읽기
#    - 뛰어난 압축률과 DuckDB와의 최고 궁합
#
# 2. 처리 엔진: DuckDB (인-프로세스 분석 데이터베이스)
#    - R 메모리로 데이터 로드 대신 SQL 쿼리로 디스크에서 직접 처리
#    - 수백 GB Parquet 파일도 메모리 부하 없이 초고속 쿼리
#
# 3. 실행 구조: Push-down 병렬 처리
#    - 기존: [데이터 로드 → 거대 객체 생성] → [병렬 처리]
#    - 혁신: [병렬 처리] → [각자 데이터 로드]
#    - 각 워커가 ('J00', 'A01') 명령어만 받아 최소 데이터로 처리
#
# 성능 목표:
# - 메모리 사용량: < 2GB (98% 절감)
# - 처리 시간: ~1.5일 (1,407,782 조합)
# - 확장성: 무제한 코어 활용 가능
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

library(survival)
library(haven)
library(dplyr)
library(tidyverse)
library(broom)
library(arrow)
library(tidycmprsk)
library(glue)
library(future)
library(furrr)
library(progressr)
library(data.table)  # 메모리 효율적인 데이터 처리
library(duckdb)      # 디스크 기반 쿼리 엔진
library(hash)
library(jsonlite)

# --- 경로 설정 ---
# paths <- list(
#     matched_sas_folder = "/home/hashjamm/project_data/disease_network/sas_files/hr_project/matched_date/",
#     matched_parquet_folder = "/home/hashjamm/project_data/disease_network/matched_date_parquet/",
#     outcome_sas_file = "/home/hashjamm/project_data/disease_network/sas_files/hr_project/hr_std_pop10.sas7bdat",
#     outcome_parquet_file = "/home/hashjamm/project_data/disease_network/outcome_table.parquet",
#     results_hr_folder = "/home/hashjamm/results/disease_network/hr_results_final/",
#     results_mapping_folder = "/home/hashjamm/results/disease_network/hr_mapping_results_final/",
#     temp_slices_folder = file.path(tempdir(), "edge_slices")
# )

# ============================================================================
# 1. 데이터 변환 모듈 (Data Conversion Modules) + sas 파일 parquet 화
# ============================================================================

# 헬퍼 함수: 데이터 테이블의 모든 컬럼명을 소문자로 변경
to_columns_lower <- function(dt) {
    # data.table의 setnames를 사용하여 효율적으로 이름 변경
    data.table::setnames(dt, tolower(names(dt)))
    return(dt)
}

# 범용 SAS → Parquet 변환 함수 (핵심 변환 로직)
convert_sas_to_parquet <- function(sas_file_path, parquet_file_path, verbose = TRUE, to_columns_lower = FALSE) {
    # SAS 파일 존재 여부 확인
    if (!file.exists(sas_file_path)) {
        return(list(
            success = FALSE,
            error = sprintf("❌ SAS 파일이 존재하지 않습니다: %s", sas_file_path)
        ))
    }
    
    tryCatch({
        if (verbose) {
            cat(sprintf("🔄 변환 중: %s → %s\n", basename(sas_file_path), basename(parquet_file_path)))
        }
        
        # SAS 파일 로드
        sas_data <- read_sas(sas_file_path)

        if (to_columns_lower) {
            sas_data <- to_columns_lower(sas_data)
        }

        original_size <- object.size(sas_data) / 1024^2  # MB로 변경
        
        # Parquet으로 저장 (디렉토리는 이미 존재함)
        write_parquet(sas_data, parquet_file_path)
        
        # 메모리 해제
        rm(sas_data)
        gc(verbose = FALSE)
        
        # 파일 크기 비교
        parquet_size <- file.size(parquet_file_path) / 1024^2  # MB로 변경
        size_saved <- original_size - parquet_size
        
        if (verbose) {
            cat(sprintf("    ✓ 완료: %.1f MB → %.1f MB (%.1f%% 절약)\n", 
                       original_size, parquet_size, (size_saved/original_size)*100))
        }
        
        return(list(
            success = TRUE,
            skipped = FALSE,
            original_size = original_size,
            parquet_size = parquet_size,
            size_saved = size_saved,
            compression_ratio = (size_saved/original_size)*100
        ))
        
    }, error = function(e) {
        return(list(
            success = FALSE,
            error = e$message
        ))
    })
}

# # outcome_table을 Parquet으로 변환
# convert_sas_to_parquet(paths$outcome_sas_file, paths$outcome_parquet_file, to_columns_lower = TRUE)

# # matched_date 파일들을 일괄 Parquet 변환
# sas_files <- list.files(paths$matched_sas_folder, pattern = "\\.sas7bdat$", full.names = FALSE)

# for (matched_file in sas_files) {
#     sas_file_path <- file.path(paths$matched_sas_folder, matched_file)
#     parquet_file <- gsub("\\.sas7bdat$", "\\.parquet", matched_file)
#     parquet_file_path <- file.path(paths$matched_parquet_folder, parquet_file)
    
#     # 범용 변환 함수 사용
#     convert_sas_to_parquet(sas_file_path, parquet_file_path, verbose = TRUE, to_columns_lower = TRUE)
# }

# ============================================================================
# 데이터 확인: outcome_table.parquet과 matched_date_parquet 파일 상위 10개 row 확인
# ============================================================================

# # outcome_table.parquet 파일 상위 10개 row 확인
# cat("\n=== outcome_table.parquet 상위 10개 row ===\n")
# outcome_data <- read_parquet(parquet_outcome_file_path)
# print(head(outcome_data, 10))

# # matched_date_parquet 폴더의 첫 번째 파일 상위 10개 row 확인
# matched_parquet_files <- list.files(parquet_matched_folder_path, pattern = "\\.parquet$", full.names = TRUE)
# if (length(matched_parquet_files) > 0) {
#     cat("\n=== matched_date_parquet 폴더 첫 번째 파일 상위 10개 row ===\n")
#     cat(sprintf("파일명: %s\n", basename(matched_parquet_files[1])))
#     matched_data <- read_parquet(matched_parquet_files[1])
#     print(head(matched_data, 10))
# } else {
#     cat("\n❌ matched_date_parquet 폴더에 parquet 파일이 없습니다.\n")
# }

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
    
    # 경쟁위험 분석을 위한 데이터 준비
    clean_data_crr <- clean_data %>% mutate(
        status_factor = factor(
            status,
            levels = 0:2, 
            labels = c("censor", "outcome", "death")
        )
    )
    
    # 경쟁위험 분석
    fit_crr <- crr(Surv(diff, status_factor) ~ case, data = clean_data_crr)
    
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
    outcome_parquet_file_path, 
    results_hr_folder_path, 
    temp_slices_folder_path
    ) {
    
    # data.table 내부 스레딩 비활성화 (future와 충돌 방지)
    setDTthreads(1)

    # 1. DuckDB로 필요한 최소 데이터만 디스크에서 직접 로드 -> 아예 R 메모리 제로로 진행
    con <- dbConnect(duckdb::duckdb())
    on.exit(dbDisconnect(con, shutdown = TRUE)) # 함수 종료 시 항상 DB 연결 해제
    
    # duckdb_register 사용시 메모리 초과 발생
    # 따라서, duckdb에서 직접 파일 경로를 사용하여 데이터를 로드하는 방식으로 수정

    # duckdb_register(
    #     con,
    #     "matched_pop_table",
    #     arrow::read_parquet(file.path(matched_parquet_folder_path, sprintf("matched_%s.parquet", tolower(cause_abb))))
    #  ) 

    matched_parquet_file_path <- file.path(matched_parquet_folder_path, sprintf("matched_%s.parquet", tolower(cause_abb)))
    
    # Outcome이 발생한 사람과 그렇지 않은 사람을 모두 포함하기 위해 LEFT JOIN 사용
    query <- glue::glue("
        SELECT m.*, o.recu_fr_dt, o.abb_sick, o.key_seq AS outcome_key_seq
        FROM read_parquet('{matched_parquet_file_path}') AS m
        LEFT JOIN (
            SELECT person_id, recu_fr_dt, abb_sick, key_seq 
            FROM read_parquet('{outcome_parquet_file_path}') 
            WHERE abb_sick = '{outcome_abb}'
        ) AS o ON m.person_id = o.person_id
    ")
    
    clean_data <- as.data.table(dbGetQuery(con, query))

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
    
    # 3. HR 분석 수행 및 최종 결과 저장 (Scatter)
    hr_result <- perform_hr_analysis(clean_data, fu, cause_abb, outcome_abb)
    filename_hr <- sprintf("hr_%s_%s_%d.parquet", cause_abb, outcome_abb, fu)
    write_parquet(hr_result, file.path(results_hr_folder_path, filename_hr))

    # 4. Edge 매핑 데이터 조각 생성 및 임시 파일로 저장 (Scatter)
    key <- paste(cause_abb, outcome_abb, fu, sep = "_")
    
    # diff < 0 제거 후 남은 'case' 그룹에 대해서만 정보 수집
    edge_slice <- list(
        pids = clean_data[case == 1, .(person_id)],
        index_key_seq = clean_data[case == 1, .(index_key_seq)],
        key_seq = clean_data[case == 1 & status == 1, .(outcome_key_seq)]
    )
    
    # 고유한 임시 파일 이름 생성 및 저장
    slice_filename <- sprintf("edge_slice_%s.rds", digest::digest(key))
    saveRDS(list(key = key, data = edge_slice), file.path(temp_slices_folder_path, slice_filename))
    
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
    outcome_parquet_file_path, 
    results_hr_folder_path, 
    temp_slices_folder_path
    ) {
    cat("\n--- [단계 1] 핵심 병렬 분석 시작 ---\n")
    
    # --- 작업 목록 생성 ---
    instruction_list <- tidyr::expand_grid(cause_abb = cause_list, outcome_abb = outcome_list) %>%
        filter(cause_abb != outcome_abb)
    cat(sprintf("총 %d개 조합 분석 시작 (Core: %d)\n", nrow(instruction_list), n_cores))
    
    # --- 병렬 처리 설정 및 실행 ---
    plan(multisession, workers = n_cores)
    required_packages <- c("data.table", "duckdb", "arrow", "survival", "broom", "tidycmprsk", "dplyr", "glue", "digest")
    
    progressr::with_progress({
        p <- progressr::progressor(steps = nrow(instruction_list))
        
        future_walk(1:nrow(instruction_list), function(i) {
            current_cause <- instruction_list$cause_abb[i]
            current_outcome <- instruction_list$outcome_abb[i]
            
            tryCatch({
                process_one_pair(
                    current_cause, current_outcome, fu,
                    matched_parquet_folder_path,
                    outcome_parquet_file_path,
                    results_hr_folder_path,
                    temp_slices_folder_path
                )
            }, error = function(e) {
                cat(sprintf("\nERROR in %s -> %s: %s\n", current_cause, current_outcome, e$message))
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

aggregate_mappings <- function(
    cause_list,
    fu, 
    matched_parquet_folder_path, 
    results_mapping_folder_path,
    temp_slices_folder_path
    ) {
    cat("\n--- [단계 2] 최종 매핑 데이터 취합 시작 ---\n")
    # --- Node 매핑 데이터 생성 ---
    cat("1. Node 매핑 데이터 생성 중...\n")
    node_pids_list <- list()
    node_index_key_seq_list <- list()
    
    for (cause_abb in cause_list) {
        key <- paste(cause_abb, fu, sep = "_")
        matched_path <- file.path(matched_parquet_folder_path, sprintf("matched_%s.parquet", tolower(cause_abb)))
        if (file.exists(matched_path)) {
            matched_data <- read_parquet(matched_path, col_select = c("person_id", "index_key_seq", "case"))
            node_pids_list[[key]] <- matched_data$person_id
            node_index_key_seq_list[[key]] <- matched_data$index_key_seq[matched_data$case == 1]
        }
    }
    save_mapping_to_parquet(node_pids_list, "node_pids", results_mapping_folder_path, fu)
    save_mapping_to_parquet(node_index_key_seq_list, "node_index_key_seq", results_mapping_folder_path, fu)
    rm(node_pids_list, node_index_key_seq_list); gc()

    # --- Edge 매핑 데이터 취합 ---
    cat("\n2. Edge 매핑 데이터 취합 중...\n")
    edge_pids_list <- list()
    edge_index_key_seq_list <- list()
    edge_key_seq_list <- list()
    
    slice_files <- list.files(temp_slices_folder_path, full.names = TRUE, pattern = "\\.rds$")
    cat(sprintf("%d개의 Edge 데이터 조각을 취합합니다.\n", length(slice_files)))
    
    if (length(slice_files) > 0) {
        progressr::with_progress({
            p <- progressr::progressor(steps = length(slice_files))
            for (slice_file in slice_files) {
                slice <- readRDS(slice_file)
                key <- slice$key
                edge_pids_list[[key]] <- unlist(slice$data$pids, use.names = FALSE)
                edge_index_key_seq_list[[key]] <- unlist(slice$data$index_key_seq, use.names = FALSE)
                edge_key_seq_list[[key]] <- unlist(slice$data$key_seq, use.names = FALSE)
                p()
            }
        })
    }
    
    save_mapping_to_parquet(edge_pids_list, "edge_pids", results_mapping_folder_path, fu)
    save_mapping_to_parquet(edge_index_key_seq_list, "edge_index_key_seq", results_mapping_folder_path, fu)
    save_mapping_to_parquet(edge_key_seq_list, "edge_key_seq", results_mapping_folder_path, fu)
    rm(edge_pids_list, edge_index_key_seq_list, edge_key_seq_list); gc()
    
    cat("--- [단계 2] 최종 매핑 데이터 취합 완료 ---\n")
}

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
        results_hr_folder = "/home/hashjamm/results/disease_network/hr_results_final/",
        results_mapping_folder = "/home/hashjamm/results/disease_network/hr_mapping_results_final/",
        temp_slices_folder = file.path(tempdir(), "edge_slices")
    )

handlers(handler_progress(format = "[:bar] :current/:total (:percent) | ETA: :eta"))

# 메인 실행 함수
main <- function(paths = paths, fu, n_cores = 90) {
    
    total_start_time <- Sys.time()
    
    # --- 실행 순서 ---
    
    # 1. 전체 질병 코드 리스트 가져오기
    disease_codes <- get_disease_codes_from_path(file.path(paths$matched_parquet_folder))
    
    # 2. 핵심 병렬 분석 실행
    run_hr_analysis(
        disease_codes, disease_codes, fu, n_cores,
        matched_parquet_folder_path = paths$matched_parquet_folder,
        outcome_parquet_file_path = paths$outcome_parquet_file,
        results_hr_folder_path = paths$results_hr_folder,
        temp_slices_folder_path = paths$temp_slices_folder
    )
    
    # 3. 최종 데이터 취합
    aggregate_mappings(
        disease_codes, fu,
        matched_parquet_folder_path = paths$matched_parquet_folder,
        results_mapping_folder_path = paths$results_mapping_folder,
        temp_slices_folder_path = paths$temp_slices_folder
    )
    
    # --- 최종 요약 ---
    total_elapsed <- as.numeric(difftime(Sys.time(), total_start_time, units = "hours"))
    cat(sprintf("\n모든 작업 완료! 총 소요 시간: %.2f시간 (%.1f일)\n", total_elapsed, total_elapsed/24))
    cat(sprintf("HR 결과물 위치: %s\n", paths$results_hr_folder))
    cat(sprintf("매핑 결과물 위치: %s\n", paths$results_mapping_folder))
}

main(paths = paths, fu = 10, n_cores = 30)
