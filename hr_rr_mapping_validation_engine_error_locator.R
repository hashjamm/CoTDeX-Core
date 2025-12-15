#!/usr/bin/env Rscript
# ============================================================================
# HR Engine Validation 에러 발생 지점 정확한 위치 파악용 디버그 스크립트
# ============================================================================
# 
# [목적]
# - validation_engine에서 에러가 발생한 정확한 코드 위치 파악
# - 로그 파일에서 에러 발생한 100개 질병쌍 추출하여 해당 조합만 처리
# - clean_data로부터 diff 설정 전후로 indicator 삽입하여 에러 발생 지점 확인
#
# ============================================================================

# ============================================================================
# 1. 라이브러리 로드
# ============================================================================

library(data.table)
library(arrow)
library(dplyr)
library(tidyr)
library(duckdb)
library(DBI)
library(glue)
library(future)
library(furrr)
library(future.apply)
library(progressr)

# ============================================================================
# 2. 설정 변수 및 경로
# ============================================================================

# [파라미터] 추적 기간 (follow-up)
fu <- 1

# [경로] 입력 데이터 파일
matched_parquet_folder_path <- "/home/hashjamm/project_data/disease_network/matched_date_parquet/"
outcome_parquet_file_path <- "/home/hashjamm/project_data/disease_network/outcome_table.parquet"

# [병렬 처리 설정] (디버깅용으로 작게 설정)
n_cores <- 4
batch_size <- 50
chunks_per_core <- 2

# [경로] 저장 디렉토리 (임시 디렉토리 사용)
output_dir <- "/home/hashjamm/results/disease_network/hr_rr_mapping_validation_results_error_locator"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# [경로] 완료 작업 추적용 DuckDB
db_completed_file_path <- file.path(output_dir, "validation_completed_jobs.duckdb")
db_completed_folder_path <- file.path(output_dir, "validation_completed_jobs")
dir.create(db_completed_folder_path, recursive = TRUE, showWarnings = FALSE)

# [경로] 결과물 청크 파일 폴더
results_chunk_folder_path <- file.path(output_dir, "validation_chunks")
dir.create(results_chunk_folder_path, recursive = TRUE, showWarnings = FALSE)

# [경로] 로그 파일 경로
log_file_path <- "/home/hashjamm/results/disease_network/hr_rr_mapping_validation_manager_history.log"

# ============================================================================
# 3. 함수 정의
# ============================================================================

# ============================================================================
# 함수: 로그 파일에서 에러 조합 추출
# ============================================================================

extract_error_combinations <- function(log_file_path, max_count = 100) {
    if (!file.exists(log_file_path)) {
        stop(sprintf("로그 파일을 찾을 수 없습니다: %s", log_file_path))
    }
    
    cat("로그 파일 읽는 중...\n")
    flush.console()
    
    # 시스템 grep 사용 (가장 빠름)
    if (Sys.which("grep") != "") {
        cat("grep으로 에러 라인 필터링 중...\n")
        flush.console()
        grep_cmd <- sprintf("grep '경고:.*처리 중 오류 발생' '%s'", log_file_path)
        error_lines <- system(grep_cmd, intern = TRUE)
        cat(sprintf("발견된 에러 라인 수: %d개\n", length(error_lines)))
        flush.console()
    } else {
        # R에서 읽기
        cat("로그 파일 전체 읽는 중... (시간이 걸릴 수 있습니다)\n")
        flush.console()
        log_lines <- readLines(log_file_path)
        error_lines <- log_lines[grepl("경고:.*처리 중 오류 발생", log_lines)]
        cat(sprintf("발견된 에러 라인 수: %d개\n", length(error_lines)))
        flush.console()
    }
    
    if (length(error_lines) == 0) {
        cat("에러 라인을 찾을 수 없습니다.\n")
        flush.console()
        return(data.frame(cause_abb = character(0), outcome_abb = character(0), stringsAsFactors = FALSE))
    }
    
    # 정규식 매칭
    error_pattern <- "경고: ([A-Z0-9]+) -> ([A-Z0-9]+) 처리 중 오류 발생"
    cat("에러 조합 추출 중...\n")
    flush.console()
    matches <- regmatches(error_lines, regexec(error_pattern, error_lines))
    
    # 유효한 매칭만 추출
    valid_matches <- matches[sapply(matches, function(x) length(x) >= 3)]
    
    if (length(valid_matches) == 0) {
        cat("유효한 에러 조합을 찾을 수 없습니다.\n")
        flush.console()
        return(data.frame(cause_abb = character(0), outcome_abb = character(0), stringsAsFactors = FALSE))
    }
    
    # data.table로 효율적으로 변환
    cause_abb_vec <- sapply(valid_matches, function(x) x[2])
    outcome_abb_vec <- sapply(valid_matches, function(x) x[3])
    
    error_combinations <- data.table(
        cause_abb = cause_abb_vec,
        outcome_abb = outcome_abb_vec
    )
    
    # 중복 제거
    error_combinations <- unique(error_combinations)
    
    # 최대 개수만큼만 선택
    if (nrow(error_combinations) > max_count) {
        error_combinations <- error_combinations[1:max_count, ]
    }
    
    cat(sprintf("추출 완료: %d개 조합 (중복 제거 후)\n", nrow(error_combinations)))
    flush.console()
    
    return(as.data.frame(error_combinations))
}

# ============================================================================
# 함수: 하나의 cause-outcome 조합 처리 (에러 위치 파악용)
# ============================================================================

process_one_combination <- function(
    cause_abb,
    outcome_abb,
    fu,
    matched_parquet_folder_path,
    outcome_parquet_file_path
) {
    result_list <- list()
    result_list[["all_diff_negative_info"]] <- list()
    
    flush.console()
    cat(sprintf("[ERROR_LOCATOR] === %s -> %s 처리 시작 ===\n", cause_abb, outcome_abb))
    flush.console()
    
    tryCatch({
        # [INDICATOR 1] 데이터 로드 시작
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 1] 데이터 로드 시작\n"))
        flush.console()
        
        con_duck <- dbConnect(duckdb::duckdb())
        matched_parquet_file_path <- file.path(matched_parquet_folder_path, 
                                               sprintf("matched_%s.parquet", tolower(cause_abb)))
        
        if (!file.exists(matched_parquet_file_path)) {
            dbDisconnect(con_duck, shutdown = TRUE)
            return(NULL)
        }
        
        query <- glue::glue(
            "SELECT m.*, o.recu_fr_dt, o.abb_sick, o.key_seq\n",
            "FROM read_parquet('{matched_parquet_file_path}') AS m\n",
            "LEFT JOIN (\n",
            "    SELECT person_id, recu_fr_dt, abb_sick, key_seq\n",
            "    FROM read_parquet('{outcome_parquet_file_path}')\n",
            "    WHERE abb_sick = '{outcome_abb}'\n",
            ") AS o ON m.person_id = o.person_id"
        )
        clean_data <- as.data.table(dbGetQuery(con_duck, query))
        dbDisconnect(con_duck, shutdown = TRUE)
        
        # [INDICATOR 2] 데이터 로드 완료
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 2] 데이터 로드 완료: %d행\n", nrow(clean_data)))
        flush.console()
        
        # [INDICATOR 3] 날짜 변환 시작
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 3] 날짜 변환 시작\n"))
        flush.console()
        
        clean_data[, `:=`(
            index_date = as.IDate(index_date, "%Y%m%d"),
            death_date = as.IDate(paste0(dth_ym, "15"), "%Y%m%d"),
            end_date = as.IDate(paste0(2003 + fu, "1231"), "%Y%m%d"),
            event_date = as.IDate(recu_fr_dt, "%Y%m%d")
        )]
        
        # [INDICATOR 4] 날짜 변환 완료
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 4] 날짜 변환 완료\n"))
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 4-1] 날짜 변환 후 NA 상태: index_date=%d, death_date=%d, end_date=%d, event_date=%d\n",
                   sum(is.na(clean_data$index_date)), 
                   sum(is.na(clean_data$death_date)),
                   sum(is.na(clean_data$end_date)),
                   sum(is.na(clean_data$event_date))))
        flush.console()
        
        # [INDICATOR 5] final_date 계산 시작
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 5] final_date 계산 시작\n"))
        flush.console()
        
        clean_data[, final_date := fifelse(
            !is.na(event_date),
            pmin(event_date, end_date, na.rm = TRUE),
            pmin(death_date, end_date, na.rm = TRUE)
        )]
        
        # [INDICATOR 6] final_date 계산 완료
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 6] final_date 계산 완료: final_date NA 개수=%d / %d\n", 
                   sum(is.na(clean_data$final_date)), nrow(clean_data)))
        flush.console()
        
        # [INDICATOR 7] status 계산 시작
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 7] status 계산 시작\n"))
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 7-1] status 계산 전 NA 상태: event_date=%d, final_date=%d, death_date=%d\n",
                   sum(is.na(clean_data$event_date)),
                   sum(is.na(clean_data$final_date)),
                   sum(is.na(clean_data$death_date))))
        flush.console()
        
        # [INDICATOR 7-2] event_date <= final_date 비교 테스트 (에러 발생 가능 지점)
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 7-2] event_date <= final_date 비교 테스트 시작\n"))
        flush.console()
        
        event_not_na_mask <- !is.na(clean_data$event_date)
        if (sum(event_not_na_mask) > 0) {
            test_comparison <- tryCatch({
                clean_data$event_date[event_not_na_mask] <= clean_data$final_date[event_not_na_mask]
            }, error = function(e) {
                flush.console()
                cat(sprintf("[ERROR_LOCATOR] [INDICATOR 7-2] 에러 발생: %s\n", e$message))
                flush.console()
                cat(sprintf("[ERROR_LOCATOR] [INDICATOR 7-2] 에러 발생 시점 데이터 샘플 (처음 5개):\n"))
                flush.console()
                sample_idx <- which(event_not_na_mask)[1:min(5, sum(event_not_na_mask))]
                for (idx in sample_idx) {
                    cat(sprintf("  [행 %d] event_date=%s, final_date=%s, event_date NA=%s, final_date NA=%s\n",
                               idx,
                               ifelse(is.na(clean_data$event_date[idx]), "NA", as.character(clean_data$event_date[idx])),
                               ifelse(is.na(clean_data$final_date[idx]), "NA", as.character(clean_data$final_date[idx])),
                               is.na(clean_data$event_date[idx]),
                               is.na(clean_data$final_date[idx])))
                    flush.console()
                }
                return(NULL)
            })
            
            if (!is.null(test_comparison)) {
                flush.console()
                cat(sprintf("[ERROR_LOCATOR] [INDICATOR 7-2] 비교 결과: TRUE=%d, FALSE=%d, NA=%d\n",
                           sum(test_comparison, na.rm=TRUE),
                           sum(!test_comparison, na.rm=TRUE),
                           sum(is.na(test_comparison))))
                flush.console()
            }
        }
        
        # [INDICATOR 7-3] death_date <= final_date 비교 테스트 (에러 발생 가능 지점)
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 7-3] death_date <= final_date 비교 테스트 시작\n"))
        flush.console()
        
        death_not_na_mask <- !is.na(clean_data$death_date)
        if (sum(death_not_na_mask) > 0) {
            test_comparison2 <- tryCatch({
                clean_data$death_date[death_not_na_mask] <= clean_data$final_date[death_not_na_mask]
            }, error = function(e) {
                flush.console()
                cat(sprintf("[ERROR_LOCATOR] [INDICATOR 7-3] 에러 발생: %s\n", e$message))
                flush.console()
                cat(sprintf("[ERROR_LOCATOR] [INDICATOR 7-3] 에러 발생 시점 데이터 샘플 (처음 5개):\n"))
                flush.console()
                sample_idx <- which(death_not_na_mask)[1:min(5, sum(death_not_na_mask))]
                for (idx in sample_idx) {
                    cat(sprintf("  [행 %d] death_date=%s, final_date=%s, death_date NA=%s, final_date NA=%s\n",
                               idx,
                               ifelse(is.na(clean_data$death_date[idx]), "NA", as.character(clean_data$death_date[idx])),
                               ifelse(is.na(clean_data$final_date[idx]), "NA", as.character(clean_data$final_date[idx])),
                               is.na(clean_data$death_date[idx]),
                               is.na(clean_data$final_date[idx])))
                    flush.console()
                }
                return(NULL)
            })
            
            if (!is.null(test_comparison2)) {
                flush.console()
                cat(sprintf("[ERROR_LOCATOR] [INDICATOR 7-3] 비교 결과: TRUE=%d, FALSE=%d, NA=%d\n",
                           sum(test_comparison2, na.rm=TRUE),
                           sum(!test_comparison2, na.rm=TRUE),
                           sum(is.na(test_comparison2))))
                flush.console()
            }
        }
        
        # [INDICATOR 7-4] 실제 status 계산 (원본 엔진과 동일)
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 7-4] 실제 status 계산 시작\n"))
        flush.console()
        
        clean_data[, status := fifelse(
            !is.na(event_date),
            fifelse(event_date <= final_date, 1, 0),
            fifelse(!is.na(death_date) & death_date <= final_date, 2, 0)
        )]
        
        # [INDICATOR 8] status 계산 완료
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 8] status 계산 완료: status NA 개수=%d / %d\n", 
                   sum(is.na(clean_data$status)), nrow(clean_data)))
        flush.console()
        
        # [INDICATOR 9] diff 계산 시작 (에러 발생 가능 지점)
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 9] diff 계산 시작 (에러 발생 가능 지점)\n"))
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 9-1] diff 계산 전 NA 상태: final_date=%d, index_date=%d\n",
                   sum(is.na(clean_data$final_date)),
                   sum(is.na(clean_data$index_date))))
        flush.console()
        
        # [INDICATOR 9-2] diff 계산 테스트
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 9-2] diff 계산 테스트 시작\n"))
        flush.console()
        
        test_diff <- tryCatch({
            clean_data$final_date - clean_data$index_date
        }, error = function(e) {
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 9-2] diff 계산 에러 발생: %s\n", e$message))
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 9-2] 에러 발생 시점 데이터 샘플 (처음 5개):\n"))
            flush.console()
            sample_idx <- 1:min(5, nrow(clean_data))
            for (idx in sample_idx) {
                cat(sprintf("  [행 %d] final_date=%s, index_date=%s, final_date NA=%s, index_date NA=%s\n",
                           idx,
                           ifelse(is.na(clean_data$final_date[idx]), "NA", as.character(clean_data$final_date[idx])),
                           ifelse(is.na(clean_data$index_date[idx]), "NA", as.character(clean_data$index_date[idx])),
                           is.na(clean_data$final_date[idx]),
                           is.na(clean_data$index_date[idx])))
                flush.console()
            }
            return(NULL)
        })
        
        if (!is.null(test_diff)) {
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 9-2] diff 계산 테스트 결과: NA 개수=%d\n", sum(is.na(test_diff))))
            flush.console()
        }
        
        clean_data[, diff := final_date - index_date]
        
        # [INDICATOR 10] diff 계산 완료
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 10] diff 계산 완료: diff NA 개수=%d / %d\n", 
                   sum(is.na(clean_data$diff)), nrow(clean_data)))
        flush.console()
        
        # [INDICATOR 11] diff < 0 필터링 시작
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 11] diff < 0 필터링 시작\n"))
        flush.console()
        
        # [INDICATOR 11-1] diff < 0 비교 테스트 (에러 발생 가능 지점)
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 11-1] diff < 0 비교 테스트 시작\n"))
        flush.console()
        
        test_diff_negative <- tryCatch({
            clean_data$diff < 0
        }, error = function(e) {
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 11-1] diff < 0 비교 에러 발생: %s\n", e$message))
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 11-1] 에러 발생 시점 데이터 샘플 (처음 5개):\n"))
            flush.console()
            sample_idx <- 1:min(5, nrow(clean_data))
            for (idx in sample_idx) {
                cat(sprintf("  [행 %d] diff=%s, diff NA=%s\n",
                           idx,
                           ifelse(is.na(clean_data$diff[idx]), "NA", as.character(clean_data$diff[idx])),
                           is.na(clean_data$diff[idx])))
                flush.console()
            }
            return(NULL)
        })
        
        if (!is.null(test_diff_negative)) {
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 11-1] diff < 0 비교 결과: TRUE=%d, FALSE=%d, NA=%d\n",
                       sum(test_diff_negative, na.rm=TRUE),
                       sum(!test_diff_negative, na.rm=TRUE),
                       sum(is.na(test_diff_negative))))
            flush.console()
        }
        
        problem_ids <- clean_data[diff < 0, unique(matched_id)]
        
        # [INDICATOR 12] diff < 0 필터링 완료
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 12] diff < 0 필터링 완료: problem_ids 개수 = %d\n", length(problem_ids)))
        flush.console()
        
        if (length(problem_ids) > 0) {
            # [INDICATOR 13] diff_negative_data 추출 시작
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 13] diff_negative_data 추출 시작\n"))
            flush.console()
            
            diff_negative_data <- clean_data[matched_id %in% problem_ids]
            
            # [INDICATOR 14] diff_negative_data 추출 완료
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 14] diff_negative_data 추출 완료: %d행\n", nrow(diff_negative_data)))
            flush.console()
            
            # [INDICATOR 15] key 생성 시작
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 15] key 생성 시작\n"))
            flush.console()
            
            key <- paste(cause_abb, outcome_abb, sep = "_")
            
            # [INDICATOR 16] key 생성 완료
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 16] key 생성 완료: %s\n", key))
            flush.console()
            
            # [INDICATOR 17] data.frame 생성 시작
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 17] data.frame 생성 시작\n"))
            flush.console()
            
            info_df <- data.frame(
                person_id = diff_negative_data$person_id,
                matched_id = diff_negative_data$matched_id,
                case = diff_negative_data$case,
                status = diff_negative_data$status,
                diff = diff_negative_data$diff,
                index_date = diff_negative_data$index_date,
                final_date = diff_negative_data$final_date,
                event_date = diff_negative_data$event_date,
                stringsAsFactors = FALSE
            )
            
            # [INDICATOR 18] data.frame 생성 완료
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 18] data.frame 생성 완료: %d행\n", nrow(info_df)))
            flush.console()
            
            # [INDICATOR 19] key 컬럼 추가 시작
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 19] key 컬럼 추가 시작\n"))
            flush.console()
            
            info_df$key <- key
            
            # [INDICATOR 20] key 컬럼 추가 완료
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 20] key 컬럼 추가 완료\n"))
            flush.console()
            
            # [INDICATOR 21] result_list에 추가 시작
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 21] result_list에 추가 시작\n"))
            flush.console()
            
            result_list[["all_diff_negative_info"]] <- info_df
            
            # [INDICATOR 22] result_list에 추가 완료
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 22] result_list에 추가 완료\n"))
            flush.console()
        } else {
            # [INDICATOR 13] diff < 0인 경우 없음
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [INDICATOR 13] diff < 0인 경우 없음\n"))
            flush.console()
        }
        
        # [INDICATOR 23] 메모리 정리 시작
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 23] 메모리 정리 시작\n"))
        flush.console()
        
        rm(clean_data, diff_negative_data, problem_ids)
        gc()
        
        # [INDICATOR 24] 메모리 정리 완료
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] [INDICATOR 24] 메모리 정리 완료\n"))
        flush.console()
        
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] === %s -> %s 처리 완료 ===\n", cause_abb, outcome_abb))
        flush.console()
        return(result_list)
        
    }, error = function(e) {
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] === %s -> %s 처리 중 에러 발생 ===\n", cause_abb, outcome_abb))
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] 에러 메시지: %s\n", e$message))
        flush.console()
        cat(sprintf("[ERROR_LOCATOR] 에러 클래스: %s\n", class(e)[1]))
        flush.console()
        if (!is.null(e$call)) {
            cat(sprintf("[ERROR_LOCATOR] 에러 호출: %s\n", deparse(e$call)))
            flush.console()
        }
        # 마지막으로 출력된 indicator 확인 가능하도록
        cat(sprintf("[ERROR_LOCATOR] 마지막 indicator 이후에 에러 발생 (위의 indicator 번호 확인)\n"))
        flush.console()
        tryCatch({
            if (exists("con_duck")) dbDisconnect(con_duck, shutdown = TRUE)
        }, error = function(e2) {})
        return(NULL)
    })
}

# ============================================================================
# 함수: 배치 처리
# ============================================================================

process_batch <- function(
    batch_jobs,  # data.table: 처리할 작업 목록
    fu,
    matched_parquet_folder_path,
    outcome_parquet_file_path,
    results_chunk_folder_path,
    db_completed_folder_path
) {
    # 워커별 청크 파일 경로 설정 (HR engine과 동일)
    worker_pid <- Sys.getpid()
    db_chunk_path <- file.path(results_chunk_folder_path, sprintf("validation_chunk_%s.duckdb", worker_pid))
    db_completed_chunk_path <- file.path(db_completed_folder_path, sprintf("completed_chunk_%s.duckdb", worker_pid))
    
    # RAM에 결과를 모을 임시 리스트 초기화 (HR engine과 동일)
    batch_diff_negative_info <- list()
    batch_completed_jobs <- list()
    
    # 배치 내 각 작업 처리 (HR engine과 동일한 방식)
    for (i in 1:nrow(batch_jobs)) {
        current_job <- batch_jobs[i, ]
        cause_abb <- current_job$cause_abb
        outcome_abb <- current_job$outcome_abb
        
        tryCatch({
            # [INDICATOR] process_one_combination 호출 시작
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [BATCH] %s -> %s: process_one_combination 호출 시작\n", 
                       cause_abb, outcome_abb))
            flush.console()
            
            # 작업 처리 (각 작업마다 메모리 내 DuckDB로 Parquet 읽기)
            result <- process_one_combination(
                cause_abb = cause_abb,
                outcome_abb = outcome_abb,
                fu = fu,
                matched_parquet_folder_path = matched_parquet_folder_path,
                outcome_parquet_file_path = outcome_parquet_file_path
            )
            
            # [INDICATOR] process_one_combination 호출 완료
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [BATCH] %s -> %s: process_one_combination 호출 완료\n", 
                       cause_abb, outcome_abb))
            flush.console()
            
            # [INDICATOR] result 검증 시작
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [BATCH] %s -> %s: result 검증 시작\n", 
                       cause_abb, outcome_abb))
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [BATCH] result is.null: %s\n", is.null(result)))
            flush.console()
            if (!is.null(result)) {
                cat(sprintf("[ERROR_LOCATOR] [BATCH] result names: %s\n", 
                           paste(names(result), collapse = ", ")))
                flush.console()
                if ("all_diff_negative_info" %in% names(result)) {
                    cat(sprintf("[ERROR_LOCATOR] [BATCH] result[['all_diff_negative_info']] 타입: %s\n", 
                               class(result[["all_diff_negative_info"]])[1]))
                    flush.console()
                    cat(sprintf("[ERROR_LOCATOR] [BATCH] result[['all_diff_negative_info']] 길이: %d\n", 
                               length(result[["all_diff_negative_info"]])))
                    flush.console()
                    if (is.data.frame(result[["all_diff_negative_info"]])) {
                        cat(sprintf("[ERROR_LOCATOR] [BATCH] result[['all_diff_negative_info']] nrow: %d\n", 
                                   nrow(result[["all_diff_negative_info"]])))
                        flush.console()
                    }
                }
            }
            
            # RAM 리스트에 결과 추가 (HR engine과 동일)
            # [INDICATOR] 결과 추가 시작
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [BATCH] %s -> %s: 결과 추가 시작\n", 
                       cause_abb, outcome_abb))
            flush.console()
            
            if (!is.null(result) && 
                "all_diff_negative_info" %in% names(result)) {
                
                # [INDICATOR] all_diff_negative_info 존재 확인
                flush.console()
                cat(sprintf("[ERROR_LOCATOR] [BATCH] %s -> %s: all_diff_negative_info 존재 확인\n", 
                           cause_abb, outcome_abb))
                flush.console()
                
                # data.frame인지 확인 후 nrow() 호출
                if (is.data.frame(result[["all_diff_negative_info"]]) && 
                    nrow(result[["all_diff_negative_info"]]) > 0) {
                    batch_diff_negative_info[[length(batch_diff_negative_info) + 1]] <- result[["all_diff_negative_info"]]
                    flush.console()
                    cat(sprintf("[ERROR_LOCATOR] [BATCH] %s -> %s: 결과 추가 완료 (%d행)\n", 
                               cause_abb, outcome_abb, nrow(result[["all_diff_negative_info"]])))
                    flush.console()
                } else {
                    flush.console()
                    cat(sprintf("[ERROR_LOCATOR] [BATCH] %s -> %s: 결과 추가 스킵 (data.frame이 아니거나 행이 0개)\n", 
                               cause_abb, outcome_abb))
                    flush.console()
                }
            } else {
                flush.console()
                cat(sprintf("[ERROR_LOCATOR] [BATCH] %s -> %s: 결과 추가 스킵 (result가 NULL이거나 all_diff_negative_info 없음)\n", 
                           cause_abb, outcome_abb))
                flush.console()
            }
            
            # 완료 기록 (결과 유무와 무관하게 기록)
            # [INDICATOR] 완료 기록 시작
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [BATCH] %s -> %s: 완료 기록 시작\n", 
                       cause_abb, outcome_abb))
            flush.console()
            
            batch_completed_jobs[[length(batch_completed_jobs) + 1]] <- data.frame(
                cause_abb = cause_abb,
                outcome_abb = outcome_abb,
                fu = fu,
                stringsAsFactors = FALSE
            )
            
            # [INDICATOR] 완료 기록 완료
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [BATCH] %s -> %s: 완료 기록 완료\n", 
                       cause_abb, outcome_abb))
            flush.console()
            
        }, error = function(e) {
            # 오류 발생 시에도 완료 기록 (재시도 방지)
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [BATCH] %s -> %s: 에러 발생 (tryCatch 내부)\n", 
                       cause_abb, outcome_abb))
            flush.console()
            cat(sprintf("경고: %s -> %s 처리 중 오류 발생: %s\n", 
                       cause_abb, outcome_abb, e$message))
            flush.console()
            cat(sprintf("[ERROR_LOCATOR] [BATCH] 에러 스택 트레이스:\n"))
            flush.console()
            print(traceback())
            flush.console()
        })
    }
    
    # --- 일괄 쓰기 (트랜잭션 관리, HR engine과 동일) ---
    # 필요한 경우에만 파일 기반 DuckDB 연결 생성
    con_chunk <- if (length(batch_diff_negative_info) > 0) dbConnect(duckdb::duckdb(), dbdir = db_chunk_path, read_only = FALSE) else NULL
    con_completed <- if (length(batch_completed_jobs) > 0) dbConnect(duckdb::duckdb(), dbdir = db_completed_chunk_path, read_only = FALSE) else NULL
    
    on.exit({
        if (!is.null(con_chunk)) dbDisconnect(con_chunk, shutdown = TRUE)
        if (!is.null(con_completed)) dbDisconnect(con_completed, shutdown = TRUE)
    })
    
    # 트랜잭션 시작
    if (!is.null(con_chunk)) dbExecute(con_chunk, "BEGIN TRANSACTION;")
    if (!is.null(con_completed)) dbExecute(con_completed, "BEGIN TRANSACTION;")
    
    tryCatch({
        # 1. diff_negative_info 결과 쓰기 (가장 중요 - 먼저 처리)
        if (!is.null(con_chunk)) {
            dbExecute(con_chunk, "CREATE TABLE IF NOT EXISTS all_diff_negative_info (
                key VARCHAR,
                person_id VARCHAR,
                matched_id VARCHAR,
                \"case\" INTEGER,
                status INTEGER,
                diff INTEGER,
                index_date DATE,
                final_date DATE,
                event_date DATE
            )")
            dbWriteTable(con_chunk, "all_diff_negative_info", bind_rows(batch_diff_negative_info), append = TRUE)
            dbExecute(con_chunk, "COMMIT;")
        }
        
        # 2. 완료 로그 쓰기 (마지막 - 모든 작업 기록, 이어쓰기 아키텍처 유지)
        if (!is.null(con_completed)) {
            completed_df <- bind_rows(batch_completed_jobs)
            dbExecute(con_completed, "CREATE TABLE IF NOT EXISTS jobs (
                cause_abb VARCHAR,
                outcome_abb VARCHAR,
                fu INTEGER,
                PRIMARY KEY (cause_abb, outcome_abb, fu)
            )")
            dbWriteTable(con_completed, "jobs", completed_df, append = TRUE)
            dbExecute(con_completed, "COMMIT;")
        }
        
    }, error = function(e) {
        # 쓰기 중 오류 발생 시 롤백 시도 (HR engine과 동일)
        warning("Batch write failed, attempting rollback: ", e$message)
        if (!is.null(con_chunk)) try(dbExecute(con_chunk, "ROLLBACK;"), silent = TRUE)
        if (!is.null(con_completed)) try(dbExecute(con_completed, "ROLLBACK;"), silent = TRUE)
        stop(e) # 에러를 다시 던져서 상위 tryCatch가 잡도록 함
    })
    
    # 메모리 정리 (메모리 파편화 방지, HR engine과 동일)
    rm(batch_diff_negative_info, batch_completed_jobs)
    gc()
    
    return(TRUE)
}

# ============================================================================
# 메인 실행 함수
# ============================================================================

main <- function() {
    # 로그 파일에서 에러 조합 추출
    cat("로그 파일에서 에러 조합 추출 중...\n")
    flush.console()
    error_combinations <- extract_error_combinations(log_file_path, max_count = 100)
    cat(sprintf("추출된 에러 조합 수: %d개\n", nrow(error_combinations)))
    flush.console()
    
    if (nrow(error_combinations) == 0) {
        cat("에러 조합을 찾을 수 없습니다. 로그 파일을 확인하세요.\n")
        return()
    }
    
    # 작업 목록 생성 (에러 발생한 조합만)
    instruction_list_all <- error_combinations
    instruction_list_all$fu <- fu
    
    # data.table로 변환 (원본 엔진과 동일)
    jobs_to_do <- as.data.table(instruction_list_all)
    
    cat(sprintf("처리할 작업 수: %d개\n", nrow(jobs_to_do)))
    flush.console()
    
    if (nrow(jobs_to_do) == 0) {
        cat("처리할 작업이 없습니다.\n")
        return()
    }
    
    # 배치 분할
    total_jobs_to_do_count <- nrow(jobs_to_do)
    num_batches <- ceiling(total_jobs_to_do_count / batch_size)
    job_indices <- 1:total_jobs_to_do_count
    batches_indices <- split(job_indices, ceiling(job_indices / batch_size))
    
    cat(sprintf("--- 총 %d개 배치 처리 시작 (각 배치는 작은 청크로 분할하여 %d개 코어가 동적으로 처리) ---\n",
                num_batches, n_cores))
    flush.console()
    
    # 병렬 처리 설정
    plan(multisession, workers = n_cores, gc = TRUE, earlySignal = TRUE)
    required_packages <- c("data.table", "duckdb", "DBI", "arrow", "dplyr", "glue", "tidyr")
    
    progressr::with_progress({
        p <- progressr::progressor(steps = num_batches)
        
        # [1-6] 각 배치를 순차적으로 처리 (배치 내부는 병렬 처리)
        for (batch_idx in 1:length(batches_indices)) {
            batch_indices <- batches_indices[[batch_idx]]
            current_batch_jobs <- jobs_to_do[batch_indices, ]
            
            # [1-6-1] 작은 청크 기반 동적 할당 (부하 균형 향상)
            batch_job_count <- nrow(current_batch_jobs)
            target_chunks <- n_cores * chunks_per_core
            chunk_size <- max(1, ceiling(batch_job_count / target_chunks))
            num_chunks <- ceiling(batch_job_count / chunk_size)
            
            chunk_indices_list <- split(1:batch_job_count, ceiling((1:batch_job_count) / chunk_size))
            
            cat(sprintf("    [배치 %d/%d] 작은 청크 기반 할당: %d개 작업 → %d개 청크 (청크당 평균 %d개)\n",
                       batch_idx, num_batches, batch_job_count, num_chunks, chunk_size))
            flush.console()
            
            # [1-6-2] 각 청크를 워커에게 동적 병렬 할당
            results <- future_lapply(chunk_indices_list, function(chunk_indices) {
                chunk_jobs <- current_batch_jobs[chunk_indices, ]
                
                tryCatch({
                    process_batch(
                        batch_jobs = chunk_jobs,
                        fu = fu,
                        matched_parquet_folder_path = matched_parquet_folder_path,
                        outcome_parquet_file_path = outcome_parquet_file_path,
                        results_chunk_folder_path = results_chunk_folder_path,
                        db_completed_folder_path = db_completed_folder_path
                    )
                }, error = function(e) {
                    cat(sprintf("\nCRITICAL BATCH ERROR (first job: %s -> %s): %s\n",
                               chunk_jobs[1, ]$cause_abb,
                               chunk_jobs[1, ]$outcome_abb,
                               e$message))
                })
                return(TRUE)
            }, future.seed = TRUE, future.packages = required_packages)
            
            p()  # 배치 완료 시 프로그레스 업데이트
        }
    })
    
    plan(sequential)
    flush.console()
    cat("모든 작업 완료!\n")
    flush.console()
}

# 스크립트 실행
if (!interactive()) {
    main()
}
