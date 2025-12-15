# ============================================================================
# HR Calculator - Cause-first Parallelization 방식
# ============================================================================
# 
# 메모리 초최적화 전략:
# - Cause별 순차 처리, Outcome별 병렬 처리
# - 각 cause에 대해 base_data 1개만 메모리에 유지 (fork로 96 워커 공유)
# - matched_pop 중복 로드 제거
# - 예상 메모리 사용: 11-13 GB (기존 126GB에서 90% 절감!)
#
# 성능 최적화:
# - DuckDB: outcome_table 디스크 쿼리 (메모리 절약)
# - data.table: in-place 연산 (속도 향상)
# - multicore fork: base_data COW 공유
# - 96 코어 풀 활용 가능
# - 예상 처리 시간: ~2.5일 (1,407,782 조합)
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

# ============================================================================
# outcome_table을 Parquet로 변환 (한 번만 실행)
# ============================================================================
outcome_parquet_path <- "/home/hashjamm/project_data/disease_network/outcome_table.parquet"

if (!file.exists(outcome_parquet_path)) {
    cat("=== outcome_table Parquet 변환 시작 ===\n")
    cat("SAS 파일 로드 중...\n")
    outcome_table_raw <- read_sas("/home/hashjamm/project_data/disease_network/sas_files/hr_project/hr_std_pop10.sas7bdat")
    cat(sprintf("원본 크기: %.2f GiB\n", object.size(outcome_table_raw) / 1024^3))
    
    cat(sprintf("Parquet 변환 중: %s\n", outcome_parquet_path))
    dir.create(dirname(outcome_parquet_path), showWarnings = FALSE, recursive = TRUE)
    write_parquet(outcome_table_raw, outcome_parquet_path)
    
    # 메모리 해제
    rm(outcome_table_raw)
    gc()
    
    cat(sprintf("✓ Parquet 변환 완료: %s\n", outcome_parquet_path))
    cat(sprintf("  파일 크기: %.2f GiB\n", file.size(outcome_parquet_path) / 1024^3))
} else {
    cat(sprintf("✓ Parquet 파일 존재: %s\n", outcome_parquet_path))
    cat(sprintf("  파일 크기: %.2f GiB\n", file.size(outcome_parquet_path) / 1024^3))
}

prepare_base_matched_data_for_cause <- function(cause_abb, fu) {
    cat(sprintf("   [1/4] matched_pop 로드: matched_%s.sas7bdat\n", tolower(cause_abb)))
    matched_pop <- read_sas(glue("/home/hashjamm/project_data/disease_network/sas_files/hr_project/matched_date/matched_", tolower(cause_abb), ".sas7bdat"))
    setDT(matched_pop)
    
    cat(sprintf("   [2/4] matched_pop 전처리 (%s명)\n", format(nrow(matched_pop), big.mark = ",")))
    matched_pop[, `:=`(
        person_id = PERSON_ID,
        index_date = as.IDate(index_date, format = "%Y%m%d"),
        death_date = as.IDate(paste0(DTH_YM, "15"), format = "%Y%m%d"),
        dth_code1 = DTH_CODE1,
        dth_code2 = DTH_CODE2,
        end_date = as.IDate(paste0(2003 + fu, "1231"), format = "%Y%m%d")
    )]
    matched_pop[, c("PERSON_ID", "DTH_YM", "DTH_CODE1", "DTH_CODE2") := NULL]
    
    cat(sprintf("   [3/4] outcome_table 쿼리 (모든 outcome)\n"))
    
    # --- [변경] 솔루션 A: DuckDB 쿼리 최적화 ---
    # 거대한 IN (...) 문자열 대신, person_id를 DuckDB에 직접 등록하고 INNER JOIN 사용
    duckdb_con <- dbConnect(duckdb::duckdb())
    
    # R의 person_id 벡터를 DuckDB가 인식할 수 있도록 등록
    person_ids_dt <- data.table(PERSON_ID = matched_pop$person_id)
    duckdb::duckdb_register(duckdb_con, "current_person_ids", person_ids_dt)
    
    outcome_parquet_path <- "/home/hashjamm/project_data/disease_network/outcome_table.parquet"
    query <- sprintf("
        SELECT t1.*
        FROM read_parquet('%s') AS t1
        INNER JOIN current_person_ids AS t2 ON t1.PERSON_ID = t2.PERSON_ID
    ", outcome_parquet_path)
    
    all_outcomes <- as.data.table(dbGetQuery(duckdb_con, query))
    
    # 임시 테이블 해제 및 연결 종료
    duckdb::duckdb_unregister(duckdb_con, "current_person_ids")
    dbDisconnect(duckdb_con)
    # --- 솔루션 A 변경 완료 ---

    cat(sprintf("   [4/4] 데이터 병합 및 전처리 (outcome 행: %s)\n", format(nrow(all_outcomes), big.mark = ",")))
    base_data <- merge(matched_pop, all_outcomes, by = "person_id", all.x = FALSE, allow.cartesian = TRUE)

    if (nrow(all_outcomes) > 0) {
        base_data[, `:=`( event_date = as.IDate(recu_fr_dt, format = "%Y%m%d") )]
        base_data[, c("recu_fr_dt") := NULL]
    }
    
    rm(all_outcomes)
    gc(verbose = FALSE)
    
    cat(sprintf("   ✓ base_data 생성 완료 (총 %s행, 크기: %.2f GiB)\n", 
                format(nrow(base_data), big.mark = ","),
                object.size(base_data) / 1024^3))
    
    return(list(base_data = base_data, matched_pop = matched_pop))
}

# base_data <- base_data_matched_pop$base_data
# matched_pop <- base_data_matched_pop$matched_pop

# 2. base_data에서 특정 outcome만 필터링하여 clean_data 생성
extract_clean_data_for_outcome <- function(base_data, matched_pop, outcome_abb) {
    # 특정 outcome만 필터링 (COW를 위해 base_data 수정 방지)
    # - abb_sick == outcome_abb인 행 (outcome 발생)
    # - abb_sick가 NA인 행 (outcome 미발생)
    # 같은 person_id가 여러 outcome 가지면 중복 행이 있으므로 처리 필요
    
    # COW를 위해 인덱스만 사용 (base_data 수정 방지)
    outcome_indices <- which(base_data$abb_sick == outcome_abb)
    
    if (length(outcome_indices) == 0) {
        # 해당 outcome이 없는 경우 빈 데이터 반환
        one_outcome_data <- data.frame()
    } else {
        # 인덱스로 접근 (base_data 수정 없음)
        one_outcome_data <- base_data[outcome_indices, ]
    }

    # data.frame을 data.table로 변환 (처리용)
    one_outcome_data <- as.data.table(one_outcome_data)
    matched_pop <- as.data.table(matched_pop)

    if (nrow(one_outcome_data) > 0 && anyDuplicated(one_outcome_data$person_id) > 0) {
        stop("오류: one_outcome_data에서 person_id 중복 발견!")
    }

    if (nrow(one_outcome_data) > 0) {
        one_outcome_pids <- unique(one_outcome_data$person_id)
        missing_pids_matched_pop <- matched_pop[!person_id %in% one_outcome_pids]
        
        clean_data <- rbindlist(list(one_outcome_data, missing_pids_matched_pop), fill = TRUE)
        setcolorder(clean_data, names(one_outcome_data))
    } else {
        # outcome이 없는 경우 matched_pop에 누락된 컬럼 추가
        clean_data <- copy(matched_pop)
        clean_data[, `:=`(
            event_date = as.IDate(NA),
            abb_sick = NA_character_
        )]
    }

    if (nrow(clean_data) != nrow(matched_pop)) {
        stop("오류: clean_data의 행 수가 matched_pop의 행 수 합과 다릅니다!")
    }

    # final_date 계산
    clean_data[, final_date := fifelse(
        !is.na(event_date),
        pmin(event_date, end_date, na.rm = TRUE),
        pmin(death_date, end_date, na.rm = TRUE)
    )]

    # status 계산
    clean_data[, status := fifelse(
        !is.na(event_date),
        fifelse(event_date <= final_date, 1, 0), # event_date가 final_date와 같거나 빨라야 이벤트
        fifelse(!is.na(death_date) & death_date <= final_date, 2, 0)
    )]
    
    # diff 계산
    clean_data[, diff := final_date - index_date]
    
    # diff < 0인 matched_id들 찾기
    problem_ids <- clean_data[diff < 0, unique(matched_id)]
    
    if (length(problem_ids) > 0) {
        # diff < 0인 matched_id들 제거
        clean_data <- clean_data[!matched_id %in% problem_ids]
        
        # 제거 후 검증: 각 matched_id당 6명인지 확인
        remaining_counts <- clean_data[, .N, by = matched_id]
        problem_remaining <- remaining_counts[N != 6]
        
        if (nrow(problem_remaining) > 0) {
            stop(sprintf("❌ %s: 검증 실패 - matched_id %s에 %d명만 있음 (예상 6명)", 
                       outcome_abb, problem_remaining$matched_id[1], problem_remaining$N[1]))
        }
    }
    
    # 결과 출력
    removed_count <- length(problem_ids) * 6
    remaining_count <- nrow(clean_data)
    cat(sprintf("    ✓ %s: 검증 통과 (제거 %d명, 남은 %d명)\n", 
                outcome_abb, removed_count, remaining_count))
    
    return(clean_data)
}

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

# 매핑 저장소 초기화 (메모리 누수 방지)
init_mapping_storage <- function() {
    # 기존 해시 객체들을 명시적으로 해제
    if (exists("node_index_key_seq_mapping")) {
        rm(node_index_key_seq_mapping, envir = .GlobalEnv)
    }
    if (exists("node_pids_mapping")) {
        rm(node_pids_mapping, envir = .GlobalEnv)
    }
    if (exists("edge_index_key_seq_mapping")) {
        rm(edge_index_key_seq_mapping, envir = .GlobalEnv)
    }
    if (exists("edge_key_seq_mapping")) {
        rm(edge_key_seq_mapping, envir = .GlobalEnv)
    }
    if (exists("edge_pids_mapping")) {
        rm(edge_pids_mapping, envir = .GlobalEnv)
    }
    
    # 강제 가비지 컬렉션
    gc(verbose = FALSE)
    
    # 새로운 빈 해시 객체들 생성
    node_index_key_seq_mapping <<- hash()
    node_pids_mapping <<- hash()
    edge_index_key_seq_mapping <<- hash()
    edge_key_seq_mapping <<- hash()
    edge_pids_mapping <<- hash()
}


# matched_pop 처리 함수 (node) (매핑 반환)
process_node_indicators <- function(cause, matched_pop, fu) {
    key <- paste(cause, fu, sep = "_")

    # index_key_seq 수집
    if (is.null(node_index_key_seq_mapping[[key]])) {
        node_index_key_seq_mapping[[key]] <<- matched_pop$index_key_seq
    } else {
        node_index_key_seq_mapping[[key]] <<- c(
            node_index_key_seq_mapping[[key]], 
            matched_pop$index_key_seq
        )
    }

    # pids 수집
    if (is.null(node_pids_mapping[[key]])) {
        node_pids_mapping[[key]] <<- matched_pop$person_id
    } else {
        node_pids_mapping[[key]] <<- c(
            node_pids_mapping[[key]], 
            matched_pop$person_id
            )
    }
}

# clean_data 처리 함수 (edge) (매핑들 반환)
# [변경] process_edge_indicators 함수는 이제 파일 저장을 직접 담당하지 않음
process_edge_indicators <- function(cause, outcome, clean_data, fu) {
    key <- paste(cause, outcome, fu, sep = "_")
    mapping_data <- list(
        edge_index_key_seq = list(key = key, data = clean_data$index_key_seq),
        edge_key_seq = list(key = key, data = clean_data$key_seq),
        edge_pids = list(key = key, data = clean_data$person_id)
    )
    return(mapping_data)
}

# ============================================================================
# Cause-first Parallelization: 메모리 효율 극대화
# ============================================================================

# cause 하나에 대한 모든 outcome 처리
# ============================================================================
# 병렬 처리 및 결과 취합 함수 (가장 큰 변경이 있는 부분)
# ============================================================================

process_one_cause_all_outcomes <- function(cause_abb, outcome_list, fu,
                                           n_cores = 96,
                                           output_dir = "/home/hashjamm/results/disease_network/hr_results") {
    
    cause_start_time <- Sys.time()
    
    cat(sprintf("\n════════════════════════════════════════════════════════════\n"))
    cat(sprintf("Cause: %s (모든 outcome 처리)\n", cause_abb))
    cat(sprintf("════════════════════════════════════════════════════════════\n"))
    
    base_data_matched_pop <- prepare_base_matched_data_for_cause(cause_abb, fu)
    base_data <- base_data_matched_pop$base_data
    matched_pop <- base_data_matched_pop$matched_pop

    process_node_indicators(cause_abb, matched_pop, fu)
    
    valid_outcomes <- outcome_list[outcome_list != cause_abb]
    
    base_data <- as.data.frame(base_data)
    matched_pop <- as.data.frame(matched_pop)
    
    # --- [변경] 솔루션 B: 임시 파일 저장을 위한 디렉토리 생성 ---
    temp_mapping_dir <- file.path(tempdir(), "mapping_temp", cause_abb)
    if (dir.exists(temp_mapping_dir)) {
        unlink(temp_mapping_dir, recursive = TRUE)
    }
    dir.create(temp_mapping_dir, recursive = TRUE)
    cat(sprintf("\n   임시 매핑 디렉토리: %s\n", temp_mapping_dir))
    # --- 솔루션 B 변경 완료 ---

    cat(sprintf("\n   병렬 처리 시작: %d개 outcome (%d 코어)\n", length(valid_outcomes), n_cores))
    
    # plan(multicore, workers = n_cores)
    plan(multisession, workers = n_cores) # <- multicore를 multisession으로 변경
    options(future.globals.maxSize = Inf)
    
    # --- [변경] 솔루션 B: future_map이 데이터 대신 '파일 경로'를 반환하도록 수정 ---

    # worker에 필요한 모든 패키지 목록을 명시
    required_packages <- c("data.table", "survival", "broom", "tidycmprsk", "dplyr")

    results <- future_map(valid_outcomes, function(outcome_abb) {
        tryCatch({
            clean_data <- extract_clean_data_for_outcome(base_data, matched_pop, outcome_abb)
            
            # 매핑 데이터 생성
            mapping_data <- process_edge_indicators(cause_abb, outcome_abb, clean_data, fu)
            
            # [변경] 매핑 데이터를 임시 파일로 저장
            temp_rds_path <- file.path(temp_mapping_dir, sprintf("map_%s_%s.rds", cause_abb, outcome_abb))
            saveRDS(mapping_data, temp_rds_path)
            
            # HR 분석
            hr_result <- perform_hr_analysis(clean_data, fu, cause_abb, outcome_abb)
    
            filename_hr <- sprintf("hr_%s_%s_%d.parquet", cause_abb, outcome_abb, fu)
            write_parquet(hr_result, file.path(output_dir, filename_hr))
            
            rm(clean_data, hr_result, mapping_data)
            
            # [변경] 실제 데이터 대신 파일 경로 반환
            list(
                success = TRUE,
                cause = cause_abb,
                outcome = outcome_abb,
                hr_file = filename_hr,
                mapping_file = temp_rds_path # 데이터 대신 파일 경로
            )
        }, error = function(e) {
            list(
                success = FALSE,
                cause = cause_abb,
                outcome = outcome_abb,
                error = e$message
            )
        })
    }, .options = furrr_options(seed = TRUE, packages = required_packages))
    # --- 솔루션 B 변경 완료 ---
    
    plan(sequential)
    
    rm(base_data, matched_pop)
    gc(verbose = FALSE)
    
    # --- [변경] 솔루션 B+C: '모아서 한번에 처리' 패턴으로 메모리 최적화 ---
    cat(sprintf("\n   매핑 데이터 수집 및 전역 해시 저장 중...\n"))

    # 1. 임시 리스트에 데이터 조각들 수집 (Collect)
    temp_edge_index_list <- list()
    temp_edge_key_list <- list()
    temp_edge_pids_list <- list()

    for (result in results) {
        if (result$success && !is.null(result$mapping_file)) {
            mapping_data <- readRDS(result$mapping_file)
            key <- mapping_data$edge_pids$key # 모든 매핑 데이터의 key는 동일함

            # 각 매핑 타입별로 임시 리스트에 데이터 조각(vector)을 추가
            if (is.null(temp_edge_index_list[[key]])) temp_edge_index_list[[key]] <- list()
            temp_edge_index_list[[key]][[length(temp_edge_index_list[[key]]) + 1]] <- mapping_data$edge_index_key_seq$data
            
            if (is.null(temp_edge_key_list[[key]])) temp_edge_key_list[[key]] <- list()
            temp_edge_key_list[[key]][[length(temp_edge_key_list[[key]]) + 1]] <- mapping_data$edge_key_seq$data
            
            if (is.null(temp_edge_pids_list[[key]])) temp_edge_pids_list[[key]] <- list()
            temp_edge_pids_list[[key]][[length(temp_edge_pids_list[[key]]) + 1]] <- mapping_data$edge_pids$data

            file.remove(result$mapping_file)
        }
    }

    # 2. 수집된 리스트를 한 번에 합쳐서 전역 해시 객체에 할당 (Combine)
    for (key in names(temp_edge_index_list)) {
        combined_vector <- unlist(temp_edge_index_list[[key]], use.names = FALSE)
        edge_index_key_seq_mapping[[key]] <<- c(edge_index_key_seq_mapping[[key]], combined_vector)
    }
    for (key in names(temp_edge_key_list)) {
        combined_vector <- unlist(temp_edge_key_list[[key]], use.names = FALSE)
        edge_key_seq_mapping[[key]] <<- c(edge_key_seq_mapping[[key]], combined_vector)
    }
    for (key in names(temp_edge_pids_list)) {
        combined_vector <- unlist(temp_edge_pids_list[[key]], use.names = FALSE)
        edge_pids_mapping[[key]] <<- c(edge_pids_mapping[[key]], combined_vector)
    }

    # 3. 임시 객체 및 디렉토리 정리
    rm(temp_edge_index_list, temp_edge_key_list, temp_edge_pids_list)
    unlink(temp_mapping_dir, recursive = TRUE)
    cat(sprintf("   ✓ 임시 매핑 파일 정리 및 데이터 병합 완료\n"))
    # --- 최종 최적화 완료 ---
    
    # 결과 요약
    success_count <- sum(sapply(results, function(x) x$success))
    failed_count <- sum(sapply(results, function(x) !x$success))
    cause_elapsed <- as.numeric(difftime(Sys.time(), cause_start_time, units = "mins"))
    
    cat(sprintf("\n   ✓ Cause %s 완료\n", cause_abb))
    cat(sprintf("     - 성공: %d개, 실패: %d개\n", success_count, failed_count))
    cat(sprintf("     - 소요 시간: %.1f분\n", cause_elapsed))
    
    return(results)
}

# 전체 cause 처리 (Cause-first 방식)
combine_hr_results_cause_first <- function(cause_list, outcome_list, fu = 10,
                                           n_cores = 96,
                                           output_dir = "/home/hashjamm/results/disease_network/hr_results",
                                           mapping_results_dir = "/home/hashjamm/results/disease_network/mapping_results") {
    
    # 출력 디렉토리 생성
    dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
    
    cat(sprintf("════════════════════════════════════════════════════════════\n"))
    cat(sprintf("Cause-first Parallelization 시작\n"))
    cat(sprintf("════════════════════════════════════════════════════════════\n"))
    cat(sprintf("총 Cause: %d개\n", length(cause_list)))
    cat(sprintf("총 Outcome: %d개 (각 cause당)\n", length(outcome_list)))
    cat(sprintf("코어 수: %d개\n", n_cores))
    cat(sprintf("출력 디렉토리: %s\n", output_dir))
    cat(sprintf("매핑 결과 디렉토리: %s\n", mapping_results_dir))
    cat(sprintf("예상 총 조합: %d개 (cause == outcome 제외)\n", length(cause_list) * (length(outcome_list) - 1)))
    
    total_start_time <- Sys.time()
    
    total_success <- 0
    total_failed <- 0
    
    init_mapping_storage()
    gc()
    
    # Cause별 순차 처리
    for (i in seq_along(cause_list)) {
        cause_abb <- cause_list[i]
        
        cat(sprintf("\n[Cause %d/%d] %s\n", i, length(cause_list), cause_abb))
        
        # 하나의 cause에 대한 모든 outcome 처리
        cause_results <- process_one_cause_all_outcomes(
            cause_abb, outcome_list, fu, n_cores, output_dir
        )
        
        # 결과 집계
        success_count <- sum(sapply(cause_results, function(x) x$success))
        failed_count <- sum(sapply(cause_results, function(x) !x$success))
        
        total_success <- total_success + success_count
        total_failed <- total_failed + failed_count
        
        # 실패 로그 저장 제거 (불필요)
        
        # 해당 cause의 매핑 결과 저장
        save_cause_mappings_to_json(cause_abb, fu, mapping_results_dir)
        
        # 다음 cause를 위해 매핑 저장소 초기화 (메모리 정리)
        init_mapping_storage()
        gc()
        
        # 진행 상황 출력
        progress_pct <- i / length(cause_list) * 100
        elapsed_hours <- as.numeric(difftime(Sys.time(), total_start_time, units = "hours"))
        estimated_total_hours <- elapsed_hours / (i / length(cause_list))
        remaining_hours <- estimated_total_hours - elapsed_hours
        
        cat(sprintf("\n  ⏱ 전체 진행률: %.1f%% (%d/%d cause)\n", 
                   progress_pct, i, length(cause_list)))
        cat(sprintf("    - 경과 시간: %.2f시간 (%.1f분)\n", elapsed_hours, elapsed_hours * 60))
        cat(sprintf("    - 예상 남은 시간: %.2f시간 (%.1f분)\n", remaining_hours, remaining_hours * 60))
        cat(sprintf("    - 예상 총 시간: %.2f시간 (%.1f일)\n", estimated_total_hours, estimated_total_hours / 24))
        cat(sprintf("    - 누적 성공: %d개, 누적 실패: %d개\n", total_success, total_failed))
    }
    
    # 전체 완료
    total_elapsed <- as.numeric(difftime(Sys.time(), total_start_time, units = "hours"))
    
    cat(sprintf("\n════════════════════════════════════════════════════════════\n"))
    cat(sprintf("전체 처리 완료!\n"))
    cat(sprintf("════════════════════════════════════════════════════════════\n"))
    cat(sprintf("총 소요 시간: %.2f시간 (%.1f일)\n", total_elapsed, total_elapsed / 24))
    cat(sprintf("성공: %d개\n", total_success))
    cat(sprintf("실패: %d개\n", total_failed))
    cat(sprintf("\n저장 위치: %s\n", output_dir))
    cat(sprintf("  - hr_*_*_%d.parquet\n", fu))
    cat(sprintf("  - count_*_*_%d.parquet\n", fu))
    
    # 실패 로그 저장 제거 (불필요)
    
    # 매핑 결과는 각 cause별로 이미 저장됨
    cat(sprintf("\n매핑 결과는 각 cause별로 저장됨: %s\n", mapping_results_dir))
    
    return(list(
        success_count = total_success,
        failed_count = total_failed,
        output_dir = output_dir,
        elapsed_hours = total_elapsed
    ))
}


# 매핑 결과를 JSON 파일로 저장 (전체)
save_mappings_to_json <- function(output_dir, fu) {
    cat(sprintf("\n=== 매핑 결과 저장 중 ===\n"))
    
    # node_index_key_seq_mapping 저장
    node_index_file <- file.path(output_dir, sprintf("node_index_key_seq_mapping_%d.json", fu))
    node_index_result <- list()
    for (key in keys(node_index_key_seq_mapping)) {
        node_index_result[[key]] <- node_index_key_seq_mapping[[key]]
    }
    jsonlite::write_json(node_index_result, node_index_file, pretty = TRUE)
    
    # node_pids_mapping 저장
    node_pids_file <- file.path(output_dir, sprintf("node_pids_mapping_%d.json", fu))
    node_pids_result <- list()
    for (key in keys(node_pids_mapping)) {
        node_pids_result[[key]] <- node_pids_mapping[[key]]
    }
    jsonlite::write_json(node_pids_result, node_pids_file, pretty = TRUE)
    
    # edge_index_key_seq_mapping 저장
    edge_index_file <- file.path(output_dir, sprintf("edge_index_key_seq_mapping_%d.json", fu))
    edge_index_result <- list()
    for (key in keys(edge_index_key_seq_mapping)) {
        edge_index_result[[key]] <- edge_index_key_seq_mapping[[key]]
    }
    jsonlite::write_json(edge_index_result, edge_index_file, pretty = TRUE)
    
    # edge_key_seq_mapping 저장
    edge_key_file <- file.path(output_dir, sprintf("edge_key_seq_mapping_%d.json", fu))
    edge_key_result <- list()
    for (key in keys(edge_key_seq_mapping)) {
        edge_key_result[[key]] <- edge_key_seq_mapping[[key]]
    }
    jsonlite::write_json(edge_key_result, edge_key_file, pretty = TRUE)
    
    # edge_pids_mapping 저장
    edge_pids_file <- file.path(output_dir, sprintf("edge_pids_mapping_%d.json", fu))
    edge_pids_result <- list()
    for (key in keys(edge_pids_mapping)) {
        edge_pids_result[[key]] <- edge_pids_mapping[[key]]
    }
    jsonlite::write_json(edge_pids_result, edge_pids_file, pretty = TRUE)
    
    cat(sprintf("매핑 결과 저장 완료:\n"))
    cat(sprintf("  - %s\n", node_index_file))
    cat(sprintf("  - %s\n", node_pids_file))
    cat(sprintf("  - %s\n", edge_index_file))
    cat(sprintf("  - %s\n", edge_key_file))
    cat(sprintf("  - %s\n", edge_pids_file))
    
    # 매핑 해시 메모리 해제
    rm(node_index_key_seq_mapping, node_pids_mapping, 
       edge_index_key_seq_mapping, edge_key_seq_mapping, edge_pids_mapping)
    gc(verbose = FALSE)
}

# 빈 문자열을 제거하는 헬퍼 함수
filter_empty_strings <- function(data_list) {
    filtered_list <- list()
    for (key in names(data_list)) {
        # 빈 문자열이 아닌 값들만 필터링
        filtered_values <- data_list[[key]][data_list[[key]] != ""]
        if (length(filtered_values) > 0) {
            filtered_list[[key]] <- filtered_values
        }
    }
    return(filtered_list)
}

# 특정 cause의 매핑 결과를 mapping_results 폴더에 저장
save_cause_mappings_to_json <- function(cause_abb, fu, mapping_results_dir = "/home/hashjamm/results/disease_network/mapping_results") {
    cat(sprintf("\n=== Cause %s 매핑 결과 저장 중 ===\n", cause_abb))
    
    # node_index_key_seq_mapping 저장
    node_index_file <- file.path(mapping_results_dir, sprintf("node_index_key_seq_mapping_%s_%d.json", cause_abb, fu))
    node_index_result <- list()
    for (key in keys(node_index_key_seq_mapping)) {
        if (grepl(paste0("^", cause_abb, "_"), key)) {
            node_index_result[[key]] <- node_index_key_seq_mapping[[key]]
        }
    }
    # 빈 문자열 필터링
    node_index_result <- filter_empty_strings(node_index_result)
    if (length(node_index_result) > 0) {
        # JSON 대신 Parquet로 저장 (용량 절약)
        parquet_file <- gsub("\\.json$", "\\.parquet", node_index_file)
        # 올바른 변환 방식 사용
        df <- data.frame(
            key = names(node_index_result),
            values = I(node_index_result),
            stringsAsFactors = FALSE
        )
        arrow::write_parquet(df, parquet_file)
        cat(sprintf("  - %s (Parquet)\n", basename(parquet_file)))
    }
    
    # node_pids_mapping 저장
    node_pids_file <- file.path(mapping_results_dir, sprintf("node_pids_mapping_%s_%d.json", cause_abb, fu))
    node_pids_result <- list()
    for (key in keys(node_pids_mapping)) {
        if (grepl(paste0("^", cause_abb, "_"), key)) {
            node_pids_result[[key]] <- node_pids_mapping[[key]]
        }
    }
    # 빈 문자열 필터링
    node_pids_result <- filter_empty_strings(node_pids_result)
    if (length(node_pids_result) > 0) {
        # JSON 대신 Parquet로 저장
        parquet_file <- gsub("\\.json$", "\\.parquet", node_pids_file)
        # 올바른 변환 방식 사용
        df <- data.frame(
            key = names(node_pids_result),
            values = I(node_pids_result),
            stringsAsFactors = FALSE
        )
        arrow::write_parquet(df, parquet_file)
        cat(sprintf("  - %s (Parquet)\n", basename(parquet_file)))
    }
    
    # edge_index_key_seq_mapping 저장
    edge_index_file <- file.path(mapping_results_dir, sprintf("edge_index_key_seq_mapping_%s_%d.json", cause_abb, fu))
    edge_index_result <- list()
    for (key in keys(edge_index_key_seq_mapping)) {
        if (grepl(paste0("^", cause_abb, "_.*_", fu, "$"), key)) {
            edge_index_result[[key]] <- edge_index_key_seq_mapping[[key]]
        }
    }
    # 빈 문자열 필터링
    edge_index_result <- filter_empty_strings(edge_index_result)
    if (length(edge_index_result) > 0) {
        # JSON 대신 Parquet로 저장
        parquet_file <- gsub("\\.json$", "\\.parquet", edge_index_file)
        # 올바른 변환 방식 사용
        df <- data.frame(
            key = names(edge_index_result),
            values = I(edge_index_result),
            stringsAsFactors = FALSE
        )
        arrow::write_parquet(df, parquet_file)
        cat(sprintf("  - %s (Parquet)\n", basename(parquet_file)))
    }
    
    # edge_key_seq_mapping 저장
    edge_key_file <- file.path(mapping_results_dir, sprintf("edge_key_seq_mapping_%s_%d.json", cause_abb, fu))
    edge_key_result <- list()
    for (key in keys(edge_key_seq_mapping)) {
        if (grepl(paste0("^", cause_abb, "_.*_", fu, "$"), key)) {
            edge_key_result[[key]] <- edge_key_seq_mapping[[key]]
        }
    }
    # 빈 문자열 필터링
    edge_key_result <- filter_empty_strings(edge_key_result)
    if (length(edge_key_result) > 0) {
        # JSON 대신 Parquet로 저장
        parquet_file <- gsub("\\.json$", "\\.parquet", edge_key_file)
        # 올바른 변환 방식 사용
        df <- data.frame(
            key = names(edge_key_result),
            values = I(edge_key_result),
            stringsAsFactors = FALSE
        )
        arrow::write_parquet(df, parquet_file)
        cat(sprintf("  - %s (Parquet)\n", basename(parquet_file)))
    }
    
    # edge_pids_mapping 저장
    edge_pids_file <- file.path(mapping_results_dir, sprintf("edge_pids_mapping_%s_%d.json", cause_abb, fu))
    edge_pids_result <- list()
    for (key in keys(edge_pids_mapping)) {
        if (grepl(paste0("^", cause_abb, "_.*_", fu, "$"), key)) {
            edge_pids_result[[key]] <- edge_pids_mapping[[key]]
        }
    }
    # 빈 문자열 필터링
    edge_pids_result <- filter_empty_strings(edge_pids_result)
    if (length(edge_pids_result) > 0) {
        # JSON 대신 Parquet로 저장
        parquet_file <- gsub("\\.json$", "\\.parquet", edge_pids_file)
        # 올바른 변환 방식 사용
        df <- data.frame(
            key = names(edge_pids_result),
            values = I(edge_pids_result),
            stringsAsFactors = FALSE
        )
        arrow::write_parquet(df, parquet_file)
        cat(sprintf("  - %s (Parquet)\n", basename(parquet_file)))
    }
    
    cat(sprintf("Cause %s 매핑 결과 저장 완료\n", cause_abb))
}

# matched_date 경로의 모든 질병 코드 추출
get_disease_codes_from_path <- function(data_path = "/home/hashjamm/project_data/disease_network/sas_files/hr_project/matched_date/") {
    
    # 모든 sas7bdat 파일 목록 가져오기
    sas_files <- list.files(data_path, pattern = "\\.sas7bdat$", full.names = FALSE)
    
    # 파일명에서 질병 코드 추출 (matched_xxx.sas7bdat -> xxx)
    disease_codes <- gsub("matched_(.*)\\.sas7bdat", "\\1", sas_files)
    
    # 대문자로 변환 (j03 -> J03)
    disease_codes <- toupper(disease_codes)
    
    # 정렬
    disease_codes <- sort(disease_codes)
    
    cat(sprintf("=== 발견된 질병 코드 (%d개) ===\n", length(disease_codes)))
    cat(sprintf("경로: %s\n\n", data_path))
    
    # 코드 출력 (10개씩 묶어서)
    for (i in seq(1, length(disease_codes), by = 10)) {
        end_idx <- min(i + 9, length(disease_codes))
        cat(sprintf("%s\n", paste(disease_codes[i:end_idx], collapse = ", ")))
    }
    
    return(disease_codes)
}

disease_codes <- get_disease_codes_from_path()

# 가장 용량이 큰 matched 파일에 대하여 어느정도 컴퓨터가 버티는지 테스트 (사실상 작업 메모리 부하가 가장 심한 작업 하나를 테스트)
# find /home/hashjamm/project_data/disease_network/sas_files/hr_project/matched_date/ -name "*.sas7bdat" -exec ls -lh {} \; | sort -k5 -hr | head -10 
which(disease_codes == "J03")

# summary <- combine_hr_results_cause_first(
#     cause_list = disease_codes[481:481],
#     outcome_list = disease_codes,
#     fu = 10, 
#     n_cores = 50, # 108G/126G -> 사실상 최대 메모리 사용 가능 코어 개수는 55개 정도로 파악됨.
#     output_dir = "/home/hashjamm/results/disease_network/hr_results"
# )

clear_output_directory("/home/hashjamm/results/disease_network/hr_results")
clear_output_directory("/home/hashjamm/results/disease_network/mapping_results")

which(disease_codes == "H10") # 357
which(disease_codes == "A09") # 9
which(disease_codes == "B30") # 57
which(disease_codes == "B36") # 61 -> 이거 나중에 확인은 해봐야함

which(disease_codes == "H00") # 350
which(disease_codes == "H03") # 353
which(disease_codes == "H06") # 356
which(disease_codes == "H10") # 357
which(disease_codes == "H51") # 387
which(disease_codes == "H65") # 397
which(disease_codes == "I99") # 477
which(disease_codes == "J00") # 478
which(disease_codes == "J02") # 480 -> 여기부터 해야함

which(disease_codes == "E11") # 208

plan(sequential)
gc()

outcome_parquet_path <- "/home/hashjamm/project_data/disease_network/outcome_table.parquet"

summary <- combine_hr_results_cause_first(
    cause_list = disease_codes[208:208],
    outcome_list = disease_codes,
    fu = 5, 
    n_cores = 90,
    output_dir = "/home/hashjamm/results/disease_network/hr_results"
)

summary <- combine_hr_results_cause_first(
    cause_list = disease_codes[479:500],
    outcome_list = disease_codes,
    fu = 10, 
    n_cores = 25,
    output_dir = "/home/hashjamm/results/disease_network/hr_results"
)

# ============================================================================
# Parquet 파일 구조 비교 (H10 vs A01)
# H10 이전까지는 json으로 저장했다가 이걸 parquet로 변환했음.
# H10 부터는 아예 parquet로 저장했음. 그래서 그 형태가 같은지 비교함
# values 내부의 값들이 numeric(json -> parquet)인지 integer(H10 -> parquet)인지 정도의 차이 
# Django 에서는 어차피 동일하게 읽혀서 무방함
# ============================================================================
cat(sprintf("\n=== Parquet 파일 구조 비교 시작 ===\n"))

# 비교할 파일들 설정
mapping_dir <- "/home/hashjamm/results/disease_network/mapping_results"
h10_files <- list.files(mapping_dir, pattern = ".*H10_10\\.parquet$", full.names = TRUE)
a01_files <- list.files(mapping_dir, pattern = ".*A01_10\\.parquet$", full.names = TRUE)

cat(sprintf("H10 파일: %d개\n", length(h10_files)))
cat(sprintf("A01 파일: %d개\n", length(a01_files)))

# 매핑 타입별로 비교
mapping_types <- c("node_index_key_seq_mapping", "node_pids_mapping", 
                  "edge_index_key_seq_mapping", "edge_key_seq_mapping", "edge_pids_mapping")

all_structures_match <- TRUE

for (mapping_type in mapping_types) {
    cat(sprintf("\n--- %s 비교 ---\n", mapping_type))
    
    h10_file <- h10_files[grepl(mapping_type, h10_files)]
    a01_file <- a01_files[grepl(mapping_type, a01_files)]
    
    if (length(h10_file) > 0 && length(a01_file) > 0) {
        tryCatch({
            # 파일 읽기
            df_h10 <- arrow::read_parquet(h10_file[1])
            df_a01 <- arrow::read_parquet(a01_file[1])
            
            # H10 파일 정보 출력
            cat(sprintf("\n[H10 파일] %s\n", basename(h10_file[1])))
            cat(sprintf("  행 수: %d\n", nrow(df_h10)))
            cat(sprintf("  컬럼 수: %d\n", ncol(df_h10)))
            cat(sprintf("  컬럼명: [%s]\n", paste(colnames(df_h10), collapse = ", ")))
            
            # H10 스키마 출력
            schema_h10 <- df_h10 %>% arrow::as_arrow_table() %>% .$schema
            cat(sprintf("  스키마: %s\n", paste(schema_h10$names, collapse = ", ")))
            cat(sprintf("  스키마 타입: %s\n", paste(schema_h10$types, collapse = ", ")))
            
            # H10 values 구조 출력
            if ("values" %in% colnames(df_h10) && nrow(df_h10) > 0) {
                h10_values_type <- class(df_h10$values[[1]])
                h10_values_length <- length(df_h10$values[[1]])
                cat(sprintf("  values 타입: %s\n", paste(h10_values_type, collapse = "/")))
                cat(sprintf("  values 길이: %d\n", h10_values_length))
                cat(sprintf("  values 처음 3개: [%s]\n", paste(head(df_h10$values[[1]], 3), collapse = ", ")))
            }
            
            # A01 파일 정보 출력
            cat(sprintf("\n[A01 파일] %s\n", basename(a01_file[1])))
            cat(sprintf("  행 수: %d\n", nrow(df_a01)))
            cat(sprintf("  컬럼 수: %d\n", ncol(df_a01)))
            cat(sprintf("  컬럼명: [%s]\n", paste(colnames(df_a01), collapse = ", ")))
            
            # A01 스키마 출력
            schema_a01 <- df_a01 %>% arrow::as_arrow_table() %>% .$schema
            cat(sprintf("  스키마: %s\n", paste(schema_a01$names, collapse = ", ")))
            cat(sprintf("  스키마 타입: %s\n", paste(schema_a01$types, collapse = ", ")))
            
            # A01 values 구조 출력
            if ("values" %in% colnames(df_a01) && nrow(df_a01) > 0) {
                a01_values_type <- class(df_a01$values[[1]])
                a01_values_length <- length(df_a01$values[[1]])
                cat(sprintf("  values 타입: %s\n", paste(a01_values_type, collapse = "/")))
                cat(sprintf("  values 길이: %d\n", a01_values_length))
                cat(sprintf("  values 처음 3개: [%s]\n", paste(head(df_a01$values[[1]], 3), collapse = ", ")))
            }
            
            # 비교 결과 요약
            cols_match <- (ncol(df_h10) == ncol(df_a01)) && identical(colnames(df_h10), colnames(df_a01))
            schema_match <- identical(schema_h10$names, schema_a01$names) && identical(schema_h10$types, schema_a01$types)
            values_structure_match <- TRUE
            if ("values" %in% colnames(df_h10) && "values" %in% colnames(df_a01)) {
                if (nrow(df_h10) > 0 && nrow(df_a01) > 0) {
                    h10_values_type <- class(df_h10$values[[1]])
                    a01_values_type <- class(df_a01$values[[1]])
                    values_structure_match <- identical(h10_values_type, a01_values_type)
                }
            }
            
            cat(sprintf("\n[비교 결과]\n"))
            cat(sprintf("  컬럼 구조: %s\n", if(cols_match) "일치" else "불일치"))
            cat(sprintf("  스키마: %s\n", if(schema_match) "일치" else "불일치"))
            cat(sprintf("  values 리스트 구조: %s\n", if(values_structure_match) "일치" else "불일치"))
            
            if (!structure_match) all_structures_match <- FALSE
            
        }, error = function(e) {
            cat(sprintf("❌ 비교 실패: %s\n", e$message))
            all_structures_match <- FALSE
        })
    } else {
        cat(sprintf("⚠️  파일을 찾을 수 없습니다\n"))
        all_structures_match <- FALSE
    }
}
