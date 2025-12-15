#!/usr/bin/env Rscript
# ============================================================================
# HR Engine에서 diff < 0인 경우 제거된 매핑 데이터 수집 및 저장 (디버깅 버전)
# ============================================================================
# 
# [목적]
# - 에러 발생 지점을 정확히 파악하기 위한 디버깅 버전
# - 실제 파일 저장 없이 로그만 출력
# - 각 단계마다 상세한 디버깅 정보 출력
#
# [주의]
# - 이 스크립트는 파일을 생성하거나 수정하지 않습니다
# - 로그만 출력하여 에러 발생 지점을 확인합니다
#
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
# 설정 변수 및 경로
# ============================================================================

fu <- 1
matched_parquet_folder_path <- "/home/hashjamm/project_data/disease_network/matched_date_parquet/"
outcome_parquet_file_path <- "/home/hashjamm/project_data/disease_network/outcome_table.parquet"

# 병렬 처리 설정 (디버깅용으로 조정)
n_cores <- 4  # 최소 2 이상, 적당한 값으로 설정
batch_size <- 50  # 작은 배치 크기
chunks_per_core <- 2  # 적당한 값

# 로그 파일 경로
log_file_path <- "/home/hashjamm/results/disease_network/hr_rr_mapping_validation_manager_history.log"

# ============================================================================
# 함수: 로그 파일에서 에러 조합 추출
# ============================================================================

extract_error_combinations <- function(log_file_path, max_count = 100) {
    if (!file.exists(log_file_path)) {
        stop(sprintf("로그 파일을 찾을 수 없습니다: %s", log_file_path))
    }
    
    cat("로그 파일 읽는 중...\n")
    flush.console()
    
    # 효율적인 방법: grep으로 먼저 필터링 (시스템 grep 사용)
    # 또는 R에서 벡터화된 정규식 사용
    error_pattern <- "경고: ([A-Z0-9]+) -> ([A-Z0-9]+) 처리 중 오류 발생"
    
    # 방법 1: 시스템 grep 사용 (가장 빠름)
    if (Sys.which("grep") != "") {
        cat("grep으로 에러 라인 필터링 중...\n")
        flush.console()
        grep_cmd <- sprintf("grep '경고:.*처리 중 오류 발생' '%s'", log_file_path)
        error_lines <- system(grep_cmd, intern = TRUE)
        cat(sprintf("발견된 에러 라인 수: %d개\n", length(error_lines)))
        flush.console()
    } else {
        # 방법 2: R에서 읽되 벡터화된 방식 사용
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
    
    # 벡터화된 정규식 매칭 (효율적)
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
# 함수: 질병 코드 목록 가져오기
# ============================================================================

get_disease_codes_from_path <- function(matched_parquet_folder_path) {
    codes <- toupper(gsub("matched_(.*)\\.parquet", "\\1", 
                         list.files(matched_parquet_folder_path, pattern = "matched_.*\\.parquet")))
    return(sort(codes))
}

# ============================================================================
# 함수: 빠른 스캔 - diff < 0이 있는지 확인만 (상세 로깅 없음)
# ============================================================================

quick_scan_combination <- function(
    cause_abb,
    outcome_abb,
    fu,
    matched_parquet_folder_path,
    outcome_parquet_file_path
) {
    tryCatch({
        # 원본 엔진과 동일한 방식으로 데이터 로드 (DuckDB 사용)
        con_duck <- dbConnect(duckdb::duckdb())
        matched_parquet_file_path <- file.path(matched_parquet_folder_path, 
                                               sprintf("matched_%s.parquet", tolower(cause_abb)))
        
        if (!file.exists(matched_parquet_file_path)) {
            dbDisconnect(con_duck, shutdown = TRUE)
            return(FALSE)
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
        
        # 날짜 변환 (원본 엔진과 동일)
        clean_data[, `:=`(
            index_date = as.IDate(index_date, "%Y%m%d"),
            death_date = as.IDate(paste0(dth_ym, "15"), "%Y%m%d"),
            end_date = as.IDate(paste0(2003 + fu, "1231"), "%Y%m%d"),
            event_date = as.IDate(recu_fr_dt, "%Y%m%d")
        )]
        
        # final_date 계산
        clean_data[, final_date := fifelse(
            !is.na(event_date),
            pmin(event_date, end_date, na.rm = TRUE),
            pmin(death_date, end_date, na.rm = TRUE)
        )]
        
        # diff 계산
        clean_data[, diff := final_date - index_date]
        
        # diff < 0이 있는지 확인
        has_diff_negative <- sum(clean_data$diff < 0, na.rm = TRUE) > 0
        
        rm(clean_data)
        gc()
        
        return(has_diff_negative)
    }, error = function(e) {
        return(FALSE)
    })
}

# ============================================================================
# 함수: 하나의 cause-outcome 조합 처리 (디버깅 버전)
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
    
    # 출력 버퍼링 해제
    flush.console()
    cat(sprintf("[DEBUG] === %s -> %s 처리 시작 ===\n", cause_abb, outcome_abb))
    flush.console()
    
    # cause_abb와 outcome_abb 검증
    if (is.null(cause_abb) || length(cause_abb) == 0 || 
        (is.character(cause_abb) && nchar(trimws(cause_abb)) == 0)) {
        flush.console()
        cat(sprintf("[DEBUG] [0단계] cause_abb가 유효하지 않음: %s\n", 
                   ifelse(is.null(cause_abb), "NULL", as.character(cause_abb))))
        flush.console()
        return(NULL)
    }
    if (is.null(outcome_abb) || length(outcome_abb) == 0 || 
        (is.character(outcome_abb) && nchar(trimws(outcome_abb)) == 0)) {
        flush.console()
        cat(sprintf("[DEBUG] [0단계] outcome_abb가 유효하지 않음: %s\n", 
                   ifelse(is.null(outcome_abb), "NULL", as.character(outcome_abb))))
        flush.console()
        return(NULL)
    }
    
    # 문자열로 변환 (안전성 확보)
    cause_abb <- as.character(cause_abb)
    outcome_abb <- as.character(outcome_abb)
    
    tryCatch({
        # [1단계] DuckDB 쿼리로 데이터 로드
        flush.console()
        cat(sprintf("[DEBUG] [1단계] 데이터 로드 시작 (%s -> %s)\n", cause_abb, outcome_abb))
        flush.console()
        con_duck <- dbConnect(duckdb::duckdb())
        matched_parquet_file_path <- file.path(matched_parquet_folder_path, 
                                               sprintf("matched_%s.parquet", tolower(cause_abb)))
        
        # matched_parquet_file_path 검증
        if (is.null(matched_parquet_file_path) || length(matched_parquet_file_path) == 0 || 
            nchar(trimws(matched_parquet_file_path)) == 0) {
            flush.console()
            cat(sprintf("[DEBUG] [1단계] matched_parquet_file_path가 유효하지 않음: %s\n", 
                       ifelse(is.null(matched_parquet_file_path), "NULL", matched_parquet_file_path)))
            flush.console()
            dbDisconnect(con_duck, shutdown = TRUE)
            return(NULL)
        }
        
        if (!file.exists(matched_parquet_file_path)) {
            flush.console()
            cat(sprintf("[DEBUG] [1단계] 파일 없음: %s\n", matched_parquet_file_path))
            flush.console()
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
        flush.console()
        cat(sprintf("[DEBUG] [1단계] 데이터 로드 완료: %d행\n", nrow(clean_data)))
        flush.console()
        
        # [2단계] 전처리
        flush.console()
        cat(sprintf("[DEBUG] [2단계] 전처리 시작 (%s -> %s)\n", cause_abb, outcome_abb))
        flush.console()
        
        # 날짜 변환
        tryCatch({
            clean_data[, `:=`(
                index_date = as.IDate(index_date, "%Y%m%d"),
                death_date = as.IDate(paste0(dth_ym, "15"), "%Y%m%d"),
                end_date = as.IDate(paste0(2003 + fu, "1231"), "%Y%m%d"),
                event_date = as.IDate(recu_fr_dt, "%Y%m%d")
            )]
            flush.console()
            cat(sprintf("[DEBUG] [2단계-1] 날짜 변환 완료\n"))
            flush.console()
        }, error = function(e) {
            flush.console()
            cat(sprintf("[DEBUG] [2단계-1] 날짜 변환 오류: %s\n", e$message))
            flush.console()
            stop(e)
        })
        
        # final_date 계산
        tryCatch({
            clean_data[, final_date := fifelse(
                !is.na(event_date),
                pmin(event_date, end_date, na.rm = TRUE),
                pmin(death_date, end_date, na.rm = TRUE)
            )]
            flush.console()
            cat(sprintf("[DEBUG] [2단계-2] final_date 계산 완료\n"))
            flush.console()
            cat(sprintf("[DEBUG] [2단계-2] final_date NA 개수: %d / %d\n", 
                       sum(is.na(clean_data$final_date)), nrow(clean_data)))
            flush.console()
        }, error = function(e) {
            flush.console()
            cat(sprintf("[DEBUG] [2단계-2] final_date 계산 오류: %s\n", e$message))
            flush.console()
            stop(e)
        })
        
        # status 계산
        tryCatch({
            flush.console()
            cat(sprintf("[DEBUG] [2단계-3] status 계산 시작\n"))
            flush.console()
            cat(sprintf("[DEBUG] [2단계-3] event_date NA 개수: %d / %d\n", 
                       sum(is.na(clean_data$event_date)), nrow(clean_data)))
            flush.console()
            cat(sprintf("[DEBUG] [2단계-3] final_date NA 개수: %d / %d\n", 
                       sum(is.na(clean_data$final_date)), nrow(clean_data)))
            flush.console()
            cat(sprintf("[DEBUG] [2단계-3] death_date NA 개수: %d / %d\n", 
                       sum(is.na(clean_data$death_date)), nrow(clean_data)))
            flush.console()
            
            # event_date <= final_date 평가 테스트
            test_result <- tryCatch({
                clean_data$event_date <= clean_data$final_date
            }, error = function(e2) {
                flush.console()
                cat(sprintf("[DEBUG] [2단계-3] event_date <= final_date 평가 실패: %s\n", e2$message))
                flush.console()
                return(NULL)
            })
            if (!is.null(test_result)) {
                flush.console()
                cat(sprintf("[DEBUG] [2단계-3] event_date <= final_date 결과 샘플 (처음 5개): %s\n", 
                           paste(head(test_result, 5), collapse=", ")))
                flush.console()
                cat(sprintf("[DEBUG] [2단계-3] event_date <= final_date NA 개수: %d\n", 
                           sum(is.na(test_result))))
                flush.console()
            }
            
            clean_data[, status := fifelse(
                !is.na(event_date),
                fifelse(event_date <= final_date, 1, 0),
                fifelse(!is.na(death_date) & death_date <= final_date, 2, 0)
            )]
            flush.console()
            cat(sprintf("[DEBUG] [2단계-3] status 계산 완료\n"))
            flush.console()
            cat(sprintf("[DEBUG] [2단계-3] status NA 개수: %d / %d\n", 
                       sum(is.na(clean_data$status)), nrow(clean_data)))
            flush.console()
        }, error = function(e) {
            flush.console()
            cat(sprintf("[DEBUG] [2단계-3] status 계산 오류: %s\n", e$message))
            flush.console()
            flush.console()
            cat(sprintf("[DEBUG] [2단계-3] 에러 발생 시점의 데이터 상태:\n"))
            flush.console()
            cat(sprintf("  - clean_data 행 수: %d\n", nrow(clean_data)))
            flush.console()
            if (nrow(clean_data) > 0) {
                cat(sprintf("  - event_date 샘플: %s\n", paste(head(clean_data$event_date, 3), collapse=", ")))
                flush.console()
                cat(sprintf("  - final_date 샘플: %s\n", paste(head(clean_data$final_date, 3), collapse=", ")))
                flush.console()
            }
            stop(e)
        })
        
        # diff 계산
        tryCatch({
            clean_data[, diff := final_date - index_date]
            flush.console()
            cat(sprintf("[DEBUG] [2단계-4] diff 계산 완료\n"))
            flush.console()
            cat(sprintf("[DEBUG] [2단계-4] diff NA 개수: %d / %d\n", 
                       sum(is.na(clean_data$diff)), nrow(clean_data)))
            flush.console()
            cat(sprintf("[DEBUG] [2단계-4] diff < 0 개수: %d\n", 
                       sum(clean_data$diff < 0, na.rm = TRUE)))
            flush.console()
        }, error = function(e) {
            flush.console()
            cat(sprintf("[DEBUG] [2단계-4] diff 계산 오류: %s\n", e$message))
            flush.console()
            stop(e)
        })
        
        # [3단계] diff < 0인 경우의 matched_id 추출
        flush.console()
        cat(sprintf("[DEBUG] [3단계] diff < 0 필터링 시작 (%s -> %s)\n", cause_abb, outcome_abb))
        flush.console()
        tryCatch({
            # diff < 0 평가 테스트
            test_result <- tryCatch({
                clean_data$diff < 0
            }, error = function(e2) {
                flush.console()
                cat(sprintf("[DEBUG] [3단계] diff < 0 평가 실패: %s\n", e2$message))
                flush.console()
                return(NULL)
            })
            if (!is.null(test_result)) {
                flush.console()
                cat(sprintf("[DEBUG] [3단계] diff < 0 결과 샘플 (처음 5개): %s\n", 
                           paste(head(test_result, 5), collapse=", ")))
                flush.console()
                cat(sprintf("[DEBUG] [3단계] diff < 0 TRUE 개수: %d\n", 
                           sum(test_result, na.rm = TRUE)))
                flush.console()
                cat(sprintf("[DEBUG] [3단계] diff < 0 NA 개수: %d\n", 
                           sum(is.na(test_result))))
                flush.console()
            }
            
            problem_ids <- clean_data[diff < 0, unique(matched_id)]
            flush.console()
            cat(sprintf("[DEBUG] [3단계] diff < 0 필터링 완료: problem_ids 개수 = %d\n", length(problem_ids)))
            flush.console()
        }, error = function(e) {
            flush.console()
            cat(sprintf("[DEBUG] [3단계] diff < 0 필터링 오류: %s\n", e$message))
            flush.console()
            cat(sprintf("[DEBUG] [3단계] 에러 발생 시점의 데이터 상태:\n"))
            flush.console()
            cat(sprintf("  - clean_data 행 수: %d\n", nrow(clean_data)))
            flush.console()
            cat(sprintf("  - diff NA 개수: %d\n", sum(is.na(clean_data$diff))))
            flush.console()
            stop(e)
        })
        
        if (length(problem_ids) > 0) {
            # diff_negative_data 추출
            flush.console()
            cat(sprintf("[DEBUG] [4단계] diff_negative_data 추출 시작\n"))
            flush.console()
            tryCatch({
                diff_negative_data <- clean_data[matched_id %in% problem_ids]
                flush.console()
                cat(sprintf("[DEBUG] [4단계] diff_negative_data 추출 완료: %d행\n", nrow(diff_negative_data)))
                flush.console()
            }, error = function(e) {
                flush.console()
                cat(sprintf("[DEBUG] [4단계] diff_negative_data 추출 오류: %s\n", e$message))
                flush.console()
                stop(e)
            })
            
            # key 생성
            key <- paste(cause_abb, outcome_abb, sep = "_")
            
            # data.frame 생성
            flush.console()
            cat(sprintf("[DEBUG] [5단계] data.frame 생성 시작\n"))
            flush.console()
            tryCatch({
                flush.console()
                cat(sprintf("[DEBUG] [5단계] diff_negative_data 상태:\n"))
                flush.console()
                cat(sprintf("  - 행 수: %d\n", nrow(diff_negative_data)))
                flush.console()
                cat(sprintf("  - person_id NA 개수: %d\n", sum(is.na(diff_negative_data$person_id))))
                flush.console()
                cat(sprintf("  - status NA 개수: %d\n", sum(is.na(diff_negative_data$status))))
                flush.console()
                cat(sprintf("  - diff NA 개수: %d\n", sum(is.na(diff_negative_data$diff))))
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
                flush.console()
                cat(sprintf("[DEBUG] [5단계] data.frame 생성 완료: %d행\n", nrow(info_df)))
                flush.console()
            }, error = function(e) {
                flush.console()
                cat(sprintf("[DEBUG] [5단계] data.frame 생성 오류: %s\n", e$message))
                flush.console()
                stop(e)
            })
            
            info_df$key <- key
            result_list[["all_diff_negative_info"]] <- info_df
        } else {
            flush.console()
            cat(sprintf("[DEBUG] [4단계] diff < 0인 경우 없음\n"))
            flush.console()
        }
        
        rm(clean_data)
        if (exists("diff_negative_data")) rm(diff_negative_data)
        if (exists("problem_ids")) rm(problem_ids)
        gc()
        
        flush.console()
        cat(sprintf("[DEBUG] === %s -> %s 처리 완료 ===\n", cause_abb, outcome_abb))
        flush.console()
        return(result_list)
        
    }, error = function(e) {
        flush.console()
        cat(sprintf("[DEBUG] === %s -> %s 처리 중 에러 발생 ===\n", cause_abb, outcome_abb))
        flush.console()
        cat(sprintf("[DEBUG] 에러 메시지: %s\n", e$message))
        flush.console()
        cat(sprintf("[DEBUG] 에러 클래스: %s\n", class(e)[1]))
        flush.console()
        if (!is.null(e$call)) {
            cat(sprintf("[DEBUG] 에러 호출: %s\n", deparse(e$call)))
            flush.console()
        }
        tryCatch({
            if (exists("con_duck")) dbDisconnect(con_duck, shutdown = TRUE)
        }, error = function(e2) {})
        return(NULL)
    })
}

# ============================================================================
# 함수: 배치 처리 (디버깅 버전 - 파일 저장 없음)
# ============================================================================

process_batch <- function(
    batch_jobs,
    fu,
    matched_parquet_folder_path,
    outcome_parquet_file_path,
    results_chunk_folder_path,
    db_completed_folder_path
) {
    # 디버깅: batch_jobs 구조 확인
    flush.console()
    cat(sprintf("[DEBUG] process_batch 시작: batch_jobs 클래스=%s, 행 수=%d\n", 
               paste(class(batch_jobs), collapse=", "), nrow(batch_jobs)))
    flush.console()
    if (nrow(batch_jobs) > 0) {
        cat(sprintf("[DEBUG] batch_jobs 컬럼명: %s\n", paste(names(batch_jobs), collapse=", ")))
        flush.console()
        cat(sprintf("[DEBUG] batch_jobs 첫 번째 행 샘플: %s\n", 
                   paste(sapply(names(batch_jobs), function(n) {
                       val <- batch_jobs[[n]][1]
                       if (is.null(val)) return("NULL")
                       if (length(val) == 0) return("EMPTY")
                       return(as.character(val))
                   }), collapse=", ")))
        flush.console()
    }
    
    # batch_jobs를 명시적으로 data.frame으로 변환 (병렬 처리 환경에서 안전성 확보)
    if (inherits(batch_jobs, "data.table")) {
        batch_jobs <- as.data.frame(batch_jobs, stringsAsFactors = FALSE)
    }
    
    # 파일 저장 없이 처리만 수행
    for (i in 1:nrow(batch_jobs)) {
        # data.frame에서 안전하게 값 추출
        cause_abb <- batch_jobs$cause_abb[i]
        outcome_abb <- batch_jobs$outcome_abb[i]
        
        # 값 검증
        if (is.null(cause_abb) || is.na(cause_abb) || length(cause_abb) == 0 || 
            (is.character(cause_abb) && nchar(trimws(cause_abb)) == 0)) {
            flush.console()
            cat(sprintf("[DEBUG] 경고: 배치 작업 %d번째에서 cause_abb가 유효하지 않음 (값: %s)\n", 
                       i, ifelse(is.null(cause_abb), "NULL", as.character(cause_abb))))
            flush.console()
            next
        }
        if (is.null(outcome_abb) || is.na(outcome_abb) || length(outcome_abb) == 0 || 
            (is.character(outcome_abb) && nchar(trimws(outcome_abb)) == 0)) {
            flush.console()
            cat(sprintf("[DEBUG] 경고: 배치 작업 %d번째에서 outcome_abb가 유효하지 않음 (값: %s)\n", 
                       i, ifelse(is.null(outcome_abb), "NULL", as.character(outcome_abb))))
            flush.console()
            next
        }
        
        # 문자열로 변환 (안전성 확보)
        cause_abb <- as.character(cause_abb)
        outcome_abb <- as.character(outcome_abb)
        
        tryCatch({
            result <- process_one_combination(
                cause_abb = cause_abb,
                outcome_abb = outcome_abb,
                fu = fu,
                matched_parquet_folder_path = matched_parquet_folder_path,
                outcome_parquet_file_path = outcome_parquet_file_path
            )
        }, error = function(e) {
            flush.console()
            cat(sprintf("[DEBUG] 경고: %s -> %s 처리 중 오류 발생: %s\n", 
                       cause_abb, outcome_abb, e$message))
            flush.console()
        })
    }
    
    return(TRUE)
}

# ============================================================================
# 메인 실행 함수 (디버깅 버전)
# ============================================================================

main <- function() {
    # 로그 파일에서 에러 조합 추출
    cat("로그 파일에서 에러 조합 추출 중...\n")
    flush.console()
    error_combinations <- extract_error_combinations(log_file_path, max_count = 1000)
    cat(sprintf("추출된 에러 조합 수: %d개\n", nrow(error_combinations)))
    flush.console()
    
    if (nrow(error_combinations) == 0) {
        cat("에러 조합을 찾을 수 없습니다. 로그 파일을 확인하세요.\n")
        return()
    }
    
    # 1단계: 빠른 스캔 - diff < 0이 있는 조합만 필터링
    cat("\n[1단계] 빠른 스캔 시작: diff < 0이 있는 조합 찾기...\n")
    flush.console()
    
    # error_combinations 컬럼명 확인 및 수정
    if (!"outcome_abb" %in% names(error_combinations)) {
        cat("경고: error_combinations에 outcome_abb 컬럼이 없습니다. 컬럼명 확인 중...\n")
        flush.console()
        cat(sprintf("사용 가능한 컬럼: %s\n", paste(names(error_combinations), collapse=", ")))
        flush.console()
        # 컬럼명이 다를 수 있으므로 확인 필요
        if ("outcome" %in% names(error_combinations)) {
            error_combinations$outcome_abb <- error_combinations$outcome
        }
    }
    
    # 에러 조합 중에서 diff < 0이 있는 조합만 찾기
    valid_combinations <- data.frame()
    
    # future.globals.maxSize 제한 제거 (병렬 처리 시 큰 함수 전달 문제 해결)
    options(future.globals.maxSize = Inf)  # 제한 없음
    
    # 병렬 처리로 빠른 스캔
    plan(multisession, workers = n_cores)
    required_packages <- c("data.table", "duckdb", "DBI", "arrow", "dplyr", "glue")
    
    # 함수를 직접 전달하지 않고, 필요한 변수만 전달하도록 수정
    scan_results <- future_lapply(1:nrow(error_combinations), function(i) {
        cause_abb <- error_combinations$cause_abb[i]
        outcome_abb <- error_combinations$outcome_abb[i]
        
        # quick_scan_combination 함수를 여기서 직접 정의하여 전역 변수 의존성 제거
        has_diff <- tryCatch({
            con_duck <- dbConnect(duckdb::duckdb())
            matched_parquet_file_path <- file.path(matched_parquet_folder_path, 
                                                   sprintf("matched_%s.parquet", tolower(cause_abb)))
            
            if (!file.exists(matched_parquet_file_path)) {
                dbDisconnect(con_duck, shutdown = TRUE)
                return(FALSE)
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
            
            # 날짜 변환
            clean_data[, `:=`(
                index_date = as.IDate(index_date, "%Y%m%d"),
                death_date = as.IDate(paste0(dth_ym, "15"), "%Y%m%d"),
                end_date = as.IDate(paste0(2003 + fu, "1231"), "%Y%m%d"),
                event_date = as.IDate(recu_fr_dt, "%Y%m%d")
            )]
            
            # final_date 계산
            clean_data[, final_date := fifelse(
                !is.na(event_date),
                pmin(event_date, end_date, na.rm = TRUE),
                pmin(death_date, end_date, na.rm = TRUE)
            )]
            
            # diff 계산
            clean_data[, diff := final_date - index_date]
            
            # diff < 0이 있는지 확인
            has_diff_negative <- sum(clean_data$diff < 0, na.rm = TRUE) > 0
            
            rm(clean_data)
            gc()
            
            return(has_diff_negative)
        }, error = function(e) {
            return(FALSE)
        })
        
        # has_diff가 NA인 경우 처리
        if (is.na(has_diff)) {
            has_diff <- FALSE
        }
        
        # has_diff가 TRUE인 경우에만 data.frame 반환
        # 명시적으로 logical 값 체크
        if (!is.null(has_diff) && !is.na(has_diff) && is.logical(has_diff) && length(has_diff) == 1 && has_diff == TRUE) {
            result_df <- data.frame(cause_abb = cause_abb, outcome_abb = outcome_abb, stringsAsFactors = FALSE)
            return(result_df)
        } else {
            return(NULL)
        }
    }, future.packages = required_packages, future.globals = list(
        matched_parquet_folder_path = matched_parquet_folder_path,
        outcome_parquet_file_path = outcome_parquet_file_path,
        fu = fu
    ))
    
    plan(sequential)
    
    # NULL 제거하고 결합 (data.frame만 유지)
    non_null_results <- Filter(function(x) {
        !is.null(x) && is.data.frame(x) && nrow(x) > 0 && "cause_abb" %in% names(x) && "outcome_abb" %in% names(x)
    }, scan_results)
    
    flush.console()
    cat(sprintf("[DEBUG] scan_results 총 개수: %d개\n", length(scan_results)))
    flush.console()
    cat(sprintf("[DEBUG] non_null_results 개수: %d개\n", length(non_null_results)))
    flush.console()
    if (length(non_null_results) > 0) {
        cat(sprintf("[DEBUG] non_null_results 첫 번째 요소 구조: 클래스=%s, 컬럼명=%s\n",
                   paste(class(non_null_results[[1]]), collapse=", "),
                   paste(names(non_null_results[[1]]), collapse=", ")))
        flush.console()
    } else if (length(scan_results) > 0) {
        # data.frame이 아닌 요소가 있는지 확인
        first_non_null <- Find(function(x) !is.null(x), scan_results)
        if (!is.null(first_non_null)) {
            cat(sprintf("[DEBUG] scan_results 첫 번째 non-NULL 요소 구조: 클래스=%s, 컬럼명=%s\n",
                       paste(class(first_non_null), collapse=", "),
                       ifelse(is.data.frame(first_non_null), paste(names(first_non_null), collapse=", "), "N/A")))
            flush.console()
        }
    }
    
    if (length(non_null_results) == 0) {
        cat("[1단계] 완료: diff < 0이 있는 조합 0개 발견\n")
        flush.console()
        cat("diff < 0이 있는 조합을 찾을 수 없습니다. 에러가 발생한 조합이 실제로는 diff < 0이 없는 조합일 수 있습니다.\n")
        cat("원본 에러 조합에 대해 상세 디버깅을 진행합니다...\n")
        flush.console()
        valid_combinations <- error_combinations
    } else {
        # rbind 전에 각 요소가 제대로 된 data.frame인지 확인
        valid_combinations <- do.call(rbind, non_null_results)
        # data.frame으로 명시적 변환
        valid_combinations <- as.data.frame(valid_combinations, stringsAsFactors = FALSE)
        
        flush.console()
        cat(sprintf("[DEBUG] rbind 후 valid_combinations 구조: 클래스=%s, 행 수=%d, 컬럼명=%s\n",
                   paste(class(valid_combinations), collapse=", "),
                   nrow(valid_combinations),
                   paste(names(valid_combinations), collapse=", ")))
        flush.console()
        
        cat(sprintf("[1단계] 완료: diff < 0이 있는 조합 %d개 발견\n", nrow(valid_combinations)))
        flush.console()
        
        # 최대 100개만 선택
        if (nrow(valid_combinations) > 100) {
            valid_combinations <- valid_combinations[1:100, ]
            cat(sprintf("처음 100개 조합만 선택하여 상세 디버깅 진행\n"))
            flush.console()
        }
    }
    
    # 작업 목록 생성 (diff < 0이 있는 조합만)
    # valid_combinations가 제대로 data.frame인지 확인
    flush.console()
    cat(sprintf("[DEBUG] valid_combinations 구조 확인: 클래스=%s, 행 수=%d, 컬럼명=%s\n",
               paste(class(valid_combinations), collapse=", "), 
               nrow(valid_combinations),
               paste(names(valid_combinations), collapse=", ")))
    flush.console()
    if (nrow(valid_combinations) > 0) {
        cat(sprintf("[DEBUG] valid_combinations 첫 번째 행 샘플: %s\n",
                   paste(sapply(names(valid_combinations), function(n) {
                       val <- valid_combinations[[n]][1]
                       if (is.null(val)) return("NULL")
                       if (length(val) == 0) return("EMPTY")
                       return(as.character(val))
                   }), collapse=", ")))
        flush.console()
    }
    
    if (!is.data.frame(valid_combinations)) {
        valid_combinations <- as.data.frame(valid_combinations, stringsAsFactors = FALSE)
    }
    
    # valid_combinations에 cause_abb와 outcome_abb 컬럼이 있는지 확인
    if (!"cause_abb" %in% names(valid_combinations) || !"outcome_abb" %in% names(valid_combinations)) {
        flush.console()
        cat(sprintf("[DEBUG] 경고: valid_combinations에 cause_abb 또는 outcome_abb 컬럼이 없습니다!\n"))
        cat(sprintf("[DEBUG] 실제 컬럼명: %s\n", paste(names(valid_combinations), collapse=", ")))
        flush.console()
        # error_combinations를 사용하도록 폴백
        if (exists("error_combinations") && nrow(error_combinations) > 0) {
            cat("[DEBUG] error_combinations를 사용합니다.\n")
            flush.console()
            valid_combinations <- error_combinations
        }
    }
    
    instruction_list_all <- valid_combinations
    instruction_list_all$fu <- fu
    
    cat(sprintf("처리할 작업 수: %d개\n", nrow(instruction_list_all)))
    flush.console()
    cat(sprintf("[DEBUG] instruction_list_all 컬럼명: %s\n", paste(names(instruction_list_all), collapse=", ")))
    flush.console()
    
    # 배치 분할
    total_jobs_count <- nrow(instruction_list_all)
    num_batches <- ceiling(total_jobs_count / batch_size)
    job_indices <- 1:total_jobs_count
    batches_indices <- split(job_indices, ceiling(job_indices / batch_size))
    
    cat(sprintf("총 %d개 배치 처리 시작\n", num_batches))
    
    # 병렬 처리 설정
    plan(multisession, workers = n_cores, gc = TRUE, earlySignal = TRUE)
    required_packages <- c("data.table", "duckdb", "DBI", "arrow", "dplyr", "glue", "tidyr")
    
    progressr::with_progress({
        p <- progressr::progressor(steps = num_batches)
        
        for (batch_idx in 1:length(batches_indices)) {
            batch_indices <- batches_indices[[batch_idx]]
            
            # instruction_list_all 구조 확인
            flush.console()
            cat(sprintf("[DEBUG] [배치 %d] instruction_list_all 구조: 클래스=%s, 행 수=%d, 컬럼명=%s\n",
                       batch_idx,
                       paste(class(instruction_list_all), collapse=", "),
                       nrow(instruction_list_all),
                       paste(names(instruction_list_all), collapse=", ")))
            flush.console()
            
            # data.frame으로 명시적 변환 후 data.table로 변환
            if (!is.data.frame(instruction_list_all)) {
                instruction_list_all <- as.data.frame(instruction_list_all, stringsAsFactors = FALSE)
            }
            current_batch_jobs <- as.data.table(instruction_list_all)[batch_indices, ]
            
            # 작은 청크 기반 동적 할당
            batch_job_count <- nrow(current_batch_jobs)
            target_chunks <- n_cores * chunks_per_core
            chunk_size <- max(1, ceiling(batch_job_count / target_chunks))
            num_chunks <- ceiling(batch_job_count / chunk_size)
            
            chunk_indices_list <- split(1:batch_job_count, ceiling((1:batch_job_count) / chunk_size))
            
            flush.console()
            cat(sprintf("[배치 %d/%d] %d개 작업 → %d개 청크\n",
                       batch_idx, num_batches, batch_job_count, num_chunks))
            flush.console()
            
            # 각 청크를 워커에게 동적 병렬 할당
            results <- future_lapply(chunk_indices_list, function(chunk_indices) {
                # data.table을 data.frame으로 명시적 변환 (병렬 처리 환경에서 안전성 확보)
                chunk_jobs <- as.data.frame(current_batch_jobs[chunk_indices, ], stringsAsFactors = FALSE)
                
                tryCatch({
                    process_batch(
                        batch_jobs = chunk_jobs,
                        fu = fu,
                        matched_parquet_folder_path = matched_parquet_folder_path,
                        outcome_parquet_file_path = outcome_parquet_file_path,
                        results_chunk_folder_path = NULL,  # 사용 안 함
                        db_completed_folder_path = NULL     # 사용 안 함
                    )
                }, error = function(e) {
                    flush.console()
                    cat(sprintf("\nCRITICAL BATCH ERROR: %s\n", e$message))
                    flush.console()
                })
                return(TRUE)
            }, future.seed = TRUE, future.packages = required_packages)
            
            p()
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
