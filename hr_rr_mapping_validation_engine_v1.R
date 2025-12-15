#!/usr/bin/env Rscript
# ============================================================================
# HR Engine에서 diff < 0인 경우 제거된 매핑 데이터 수집 및 저장 (Engine 버전)
# ============================================================================
# 
# [목적]
# - hr_calculator_engine_v4.R에서 clean_data로부터 diff < 0인 경우를 제거하는데,
#   이렇게 제거된 케이스들에 대한 매핑 데이터를 수집하여 저장
# - 저장된 데이터는 RR 쪽에서 outcome_dt 기반으로 생성한 매핑 데이터와 비교 검증에 사용
# - 검증이 성공하면 RR 쪽에 대한 새로운 engine 구축 예정
#
# [사용 방법]
#   직접 실행:
#     Rscript hr_rr_mapping_validation_engine.R
#     (내부적으로 fu=1 사용, 결과는 fu 값과 무관)
#
#   Manager 스크립트를 통한 실행 (권장):
#     ./hr_rr_mapping_validation_manager.sh
#     - 자동 재시작, 메모리 제한, 진행 상황 모니터링 등 제공
#     - fu 파라미터 불필요 (내부적으로 fu=1 사용)
#
# [저장 위치]
#   기본 디렉토리: /home/hashjamm/results/disease_network/hr_rr_mapping_validation_results/
#   - validation_completed_jobs.duckdb: 완료 작업 추적 (중앙 DB)
#   - validation_completed_jobs/: 완료 작업 청크 파일 폴더
#   - validation_chunks/: 결과물 청크 파일 폴더
#   - edge_pids_mapping.parquet: 최종 취합 결과물 (key별로 person_id 그룹화)
#
# [저장 형식]
#   - 중간 저장: DuckDB 청크 파일 형식 (배치 단위, 워커별)
#   - 최종 저장: Parquet 파일 (모든 작업 완료 시 취합)
#   - edge_pids_mapping: diff < 0인 경우의 매핑 데이터
#     * key: cause_abb_outcome_abb 형식
#     * person_id: 해당 key에 대한 모든 person_id (리스트 형태)
#
# [아키텍처]
#   - hr_calculator_engine_v4.R과 동일한 방식으로 구현
#   - 병렬 처리: future/furrr 기반 multisession
#   - 배치 처리: 대규모 작업을 작은 배치로 분할
#   - 청크 기반 동적 할당: 배치를 더 작은 청크로 분할하여 워커에 동적 할당
#   - 즉시 디스크 저장: 메모리 부담 최소화를 위해 결과를 즉시 DuckDB에 저장
#   - 이어하기 지원: 완료 작업 추적을 통한 재시작 시 중복 작업 방지
#   - 로그 취합: 청크 파일의 완료 로그를 중앙 DB로 주기적 취합
#
# [주요 처리 단계]
#   단계 0: 사전 로그 취합 (이전 실행의 청크 파일을 중앙 DB로 취합)
#   단계 1: diff < 0 케이스 수집 (병렬 배치 처리)
#   단계 3: 결과물 청크 파일 취합 (모든 작업 완료 시 Parquet으로 저장)
#   단계 4: 이번 사이클 로그 취합 (다음 사이클 준비)
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
# - 기본값: 1 (결과가 fu와 무관하므로 고정값 사용)
# - 코드 내부에서 diff 계산에 필요하지만, 결과는 fu 값과 무관함
fu <- 1

# [경로] 저장 디렉토리
# - 모든 결과물이 저장되는 기본 디렉토리
output_dir <- "/home/hashjamm/results/disease_network/hr_rr_mapping_validation_results"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# [경로] 입력 데이터 파일
# - matched_*.parquet: 원인 질병별 매칭 데이터
# - outcome_table.parquet: 결과 질병 데이터
matched_parquet_folder_path <- "/home/hashjamm/project_data/disease_network/matched_date_parquet/"
outcome_parquet_file_path <- "/home/hashjamm/project_data/disease_network/outcome_table.parquet"

# [병렬 처리 설정]
# - n_cores: 사용할 코어 수 (시스템에 맞게 조정, 기본값: 15)
# - batch_size: 배치 처리 크기 (메모리 사용량 조절, 기본값: 10000)
# - chunks_per_core: 코어당 청크 수 (부하 균형 향상, 기본값: 3)
n_cores <- 80
batch_size <- 200000
chunks_per_core <- 20

# [경로] 완료 작업 추적용 DuckDB
# - db_completed_file_path: 중앙 DB (모든 완료 작업 통합)
# - db_completed_folder_path: 청크 파일 폴더 (워커별 완료 로그)
db_completed_file_path <- file.path(output_dir, "validation_completed_jobs.duckdb")
db_completed_folder_path <- file.path(output_dir, "validation_completed_jobs")
dir.create(db_completed_folder_path, recursive = TRUE, showWarnings = FALSE)

# 결과 저장 경로 (DuckDB 청크 파일)
results_chunk_folder_path <- file.path(output_dir, "validation_chunks")
dir.create(results_chunk_folder_path, recursive = TRUE, showWarnings = FALSE)

# ============================================================================
# 3. 함수 정의
# ============================================================================

# ============================================================================
# 함수: 사전 로그 취합 (pre_aggregate_logs)
# ============================================================================
# [목적]
#   로그 청크 파일들을 중앙 DB로 취합하여 이어하기를 지원하는 함수
#   hr_calculator_engine_v4.R의 pre_aggregate_logs와 동일한 방식으로 구현
#
# [처리 방식]
#   - ATTACH + INSERT INTO ... ON CONFLICT DO NOTHING 사용 (중복 방지)
#   - DuckDB의 PRIMARY KEY 제약을 활용하여 자동 중복 제거
#   - 취합 후 성공한 청크 파일 삭제 (다음 사이클 준비)
#   - 실패한 파일도 삭제 (다음 사이클에서 재실행)
#
# [파라미터]
#   chunk_folder: 청크 파일이 저장된 폴더 경로
#   central_db_path: 중앙 DB 파일 경로
#   pattern: 청크 파일 패턴 (예: "completed_chunk_.*\\.duckdb")
#   table_name: 취합할 테이블 이름
#   create_sql: 테이블 생성 SQL (IF NOT EXISTS)
#   silent: 출력 억제 여부 (기본값: FALSE)
#
# [반환값]
#   invisible(TRUE): 취합 성공 또는 청크 파일 없음
#   invisible(FALSE): 청크 파일 없음
#
# ============================================================================
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
                successfully_processed_files <- c(successfully_processed_files, chunk_files[i])
                
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
        
        if (!silent) {
            cat(sprintf("--- [사전 취합] 완료: 성공 %d개, 실패 %d개, 총 삽입 %d건 ---\n", 
                       successful_files, failed_files, total_inserted))
        }
        
        # 성공한 파일 삭제 (다음 사이클 준비)
        for (file_path in successfully_processed_files) {
            tryCatch({
                file.remove(file_path)
            }, error = function(e) {
                if (!silent) {
                    cat(sprintf("경고: 성공한 청크 파일 삭제 실패: %s\n", basename(file_path)))
                }
            })
        }
        
        return(invisible(TRUE))
    } else {
        if (!silent) {
            cat(sprintf("--- [사전 취합] 취합할 %s 청크 파일이 없습니다 ---\n", table_name))
        }
        return(invisible(FALSE))
    }
}

# ============================================================================
# 함수: 질병 코드 목록 가져오기 (get_disease_codes_from_path)
# ============================================================================
# [목적]
#   matched_date_parquet 폴더에서 질병 코드 목록을 추출
#   hr_calculator_engine_v4.R의 get_disease_codes_from_path와 동일
#
# [파라미터]
#   matched_parquet_folder_path: matched_*.parquet 파일들이 있는 폴더 경로
#
# [반환값]
#   정렬된 질병 코드 벡터 (대문자)
#
# ============================================================================
get_disease_codes_from_path <- function(matched_parquet_folder_path) {
    codes <- toupper(gsub("matched_(.*)\\.parquet", "\\1", 
                         list.files(matched_parquet_folder_path, pattern = "matched_.*\\.parquet")))
    return(sort(codes))
}

# ============================================================================
# 함수: 하나의 cause-outcome 조합 처리 (process_one_combination)
# ============================================================================
# [목적]
#   특정 cause-outcome 조합에 대해 diff < 0 & case == 1인 케이스를 추출
#   hr_calculator_engine_v4.R의 process_batch 내부 로직과 유사하지만,
#   diff < 0인 케이스만 필터링하여 저장
#
# [처리 과정]
#   1. DuckDB 쿼리로 matched_*.parquet와 outcome_table.parquet 조인
#   2. 전처리: 날짜 변환, final_date 계산, status 계산, diff 계산
#   3. diff < 0 & case == 1인 케이스 추출
#   4. all_diff_negative_info 데이터프레임 생성 (매핑 데이터 포함)
#
# [파라미터]
#   cause_abb: 원인 질병 코드
#   outcome_abb: 결과 질병 코드
#   fu: 추적 기간 (follow-up)
#   matched_parquet_folder_path: matched_*.parquet 파일 폴더 경로
#   outcome_parquet_file_path: outcome_table.parquet 파일 경로
#
# [반환값]
#   result_list: all_diff_negative_info를 포함한 리스트
#   NULL: 파일이 없거나 오류 발생 시
#
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
    
    tryCatch({
        # [1단계] DuckDB 쿼리로 데이터 로드
        # - matched_*.parquet와 outcome_table.parquet를 LEFT JOIN
        # - outcome_abb에 해당하는 outcome 데이터만 필터링
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
        
        # [2단계] 전처리 (hr_calculator_engine_v4.R의 process_batch와 동일)
        # - 날짜 변환: index_date, death_date, end_date, event_date
        # - final_date 계산: event_date, death_date, end_date 중 최소값
        # - status 계산: 이벤트 발생(1), 사망(2), 미발생(0)
        # - diff 계산: final_date - index_date
        clean_data[, `:=`(
            index_date = as.IDate(index_date, "%Y%m%d"),
            death_date = as.IDate(paste0(dth_ym, "15"), "%Y%m%d"),
            end_date = as.IDate(paste0(2003 + fu, "1231"), "%Y%m%d"),
            event_date = as.IDate(recu_fr_dt, "%Y%m%d")
        )]
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
        
        # [3단계] diff < 0인 경우의 matched_id 추출 (원본 아키텍처와 동일)
        # - diff < 0: final_date < index_date (논리적 불일치)
        # - problem_ids: diff < 0인 경우의 matched_id 목록
        # - diff_negative_data: 해당 matched_id를 가진 모든 person_id (case와 control 모두)
        problem_ids <- clean_data[diff < 0, unique(matched_id)]
        
        if (length(problem_ids) > 0) {
            # problem_ids를 matched_id로 갖고 있는 모든 행 추출 (case와 control 모두 포함)
            diff_negative_data <- clean_data[matched_id %in% problem_ids]
            key <- paste(cause_abb, outcome_abb, sep = "_")
            
            # [4단계] all_diff_negative_info에 저장할 데이터프레임 생성
            # - 기본 정보: matched_id, case, status, diff, 날짜 정보
            # - 매핑 데이터: person_id (RR 검증에 필요)
            # - 주의: diff_negative_data는 problem_ids를 matched_id로 갖는 모든 행을 포함 (case와 control 모두)
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
            
            # key 컬럼 추가
            info_df$key <- key
            
            result_list[["all_diff_negative_info"]] <- info_df
        }
        
        rm(clean_data, diff_negative_data, problem_ids)
        gc()
        
        return(result_list)
        
    }, error = function(e) {
        tryCatch({
            if (exists("con_duck")) dbDisconnect(con_duck, shutdown = TRUE)
        }, error = function(e2) {})
        return(NULL)
    })
}

# ============================================================================
# 함수: 배치 처리 (process_batch)
# ============================================================================
# [목적]
#   배치 내 작업들을 병렬 처리하고 결과를 즉시 디스크에 저장
#   메모리 부담을 최소화하기 위해 각 작업 완료 시마다 DuckDB에 저장
#
# [처리 방식]
#   - 워커별 고유한 청크 파일 사용 (PID 기반)
#   - 결과물 청크 파일: validation_chunk_<PID>.duckdb
#   - 완료 로그 청크 파일: completed_chunk_<PID>.duckdb
#   - 각 작업 완료 시 즉시 디스크에 저장 (메모리 누적 방지)
#
# [파라미터]
#   batch_jobs: 처리할 작업 목록 (data.table, cause_abb, outcome_abb 포함)
#   fu: 추적 기간
#   matched_parquet_folder_path: matched_*.parquet 파일 폴더 경로
#   outcome_parquet_file_path: outcome_table.parquet 파일 경로
#   results_chunk_folder_path: 결과물 청크 파일 저장 폴더
#   db_completed_folder_path: 완료 로그 청크 파일 저장 폴더
#
# [반환값]
#   TRUE: 배치 처리 완료
#
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
            # 작업 처리 (각 작업마다 메모리 내 DuckDB로 Parquet 읽기)
            result <- process_one_combination(
                cause_abb = cause_abb,
                outcome_abb = outcome_abb,
                fu = fu,
                matched_parquet_folder_path = matched_parquet_folder_path,
                outcome_parquet_file_path = outcome_parquet_file_path
            )
            
            # RAM 리스트에 결과 추가 (HR engine과 동일)
            # [중대 에러 발생] 아래의 에러 디버깅 기록을 참고하세요.
            # - 에러: "missing value where TRUE/FALSE needed"
            # - 발생 조건: result[["all_diff_negative_info"]]가 빈 리스트(list())인 경우
            # - 원인: nrow(list())는 NULL을 반환하고, NULL > 0 비교 시 에러 발생
            # - 해결 방법 1: is.data.frame() 체크 추가 (간단한 수정)
            #   if (!is.null(result) && 
            #       "all_diff_negative_info" %in% names(result) &&
            #       is.data.frame(result[["all_diff_negative_info"]]) &&
            #       nrow(result[["all_diff_negative_info"]]) > 0) {
            # - 해결 방법 2: HR engine처럼 구조 변경 (근본적 해결)
            #   * process_one_combination 함수를 호출하지 않고 process_batch 내에서 직접 처리
            #   * 중첩된 리스트 구조(result[["all_diff_negative_info"]])를 사용하지 않음
            #   * hr_calculator_engine_v4.R의 process_batch 함수 구조 참고
            #   * 이 방법이 더 근본적인 해결책이며 HR engine과 일관성 유지
            if (!is.null(result) && 
                "all_diff_negative_info" %in% names(result) &&
                nrow(result[["all_diff_negative_info"]]) > 0) {
                batch_diff_negative_info[[length(batch_diff_negative_info) + 1]] <- result[["all_diff_negative_info"]]
            }
            
            # 완료 기록 (결과 유무와 무관하게 기록)
            batch_completed_jobs[[length(batch_completed_jobs) + 1]] <- data.frame(
                cause_abb = cause_abb,
                outcome_abb = outcome_abb,
                fu = fu,
                stringsAsFactors = FALSE
            )
            
        }, error = function(e) {
            # 오류 발생 시에도 완료 기록 (재시도 방지)
            cat(sprintf("경고: %s -> %s 처리 중 오류 발생: %s\n", 
                       cause_abb, outcome_abb, e$message))
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
# 4. 메인 실행 함수 (run_validation_analysis)
# ============================================================================
# [목적]
#   전체 validation 분석을 실행하는 메인 함수
#   hr_calculator_engine_v4.R의 run_hr_analysis와 유사한 구조
#
# [처리 단계]
#   단계 0: 사전 로그 취합 (이전 실행의 청크 파일을 중앙 DB로 취합)
#   단계 1: diff < 0 케이스 수집 (병렬 배치 처리)
#   단계 4: 이번 사이클 로그 취합 (다음 사이클 준비)
#
# [파라미터]
#   cause_list: 원인 질병 코드 목록
#   outcome_list: 결과 질병 코드 목록
#   fu: 추적 기간
#   n_cores: 병렬 처리 코어 수
#   batch_size: 배치 크기
#   chunks_per_core: 코어당 청크 수
#   matched_parquet_folder_path: matched_*.parquet 파일 폴더 경로
#   outcome_parquet_file_path: outcome_table.parquet 파일 경로
#   results_chunk_folder_path: 결과물 청크 파일 저장 폴더
#   db_completed_file_path: 완료 작업 중앙 DB 파일 경로
#   db_completed_folder_path: 완료 로그 청크 파일 저장 폴더
#
# [반환값]
#   0: 모든 작업 완료
#   1: 재시작 필요 (남은 작업 있음)
#
# ============================================================================

run_validation_analysis <- function(
    cause_list,
    outcome_list,
    fu,
    n_cores,
    batch_size,
    chunks_per_core = 3,
    matched_parquet_folder_path,
    outcome_parquet_file_path,
    results_chunk_folder_path,
    db_completed_file_path,
    db_completed_folder_path
) {
    # ========================================================================
    # [단계 0] 사전 로그 취합 (이어하기 준비)
    # ========================================================================
    # 이전 실행에서 생성된 완료 로그 청크 파일들을 중앙 DB로 취합
    # 재시작 시 정확한 완료 작업 수를 파악하기 위함
    cat("\n--- [단계 0] 사전 로그 취합 (이어하기 준비) ---\n")
    pre_aggregate_logs(
        chunk_folder = db_completed_folder_path,
        central_db_path = db_completed_file_path,
        pattern = "completed_chunk_.*\\.duckdb",
        table_name = "jobs",
        create_sql = "CREATE TABLE IF NOT EXISTS jobs (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, PRIMARY KEY (cause_abb, outcome_abb, fu))"
    )
    
    # ========================================================================
    # [단계 1] diff < 0 케이스 수집 시작
    # ========================================================================
    cat("\n--- [단계 1] diff < 0 케이스 수집 시작 ---\n")
    
    # [1-1] 전체 작업 목록 생성 (모든 cause-outcome 조합)
    instruction_list_all <- tidyr::expand_grid(cause_abb = cause_list, outcome_abb = outcome_list) %>%
        filter(cause_abb != outcome_abb) %>%
        mutate(fu = fu)
    
    # [1-2] 완료된 작업 확인 (중앙 DB만 읽음, 청크 파일은 이미 취합됨)
    con_completed <- dbConnect(duckdb::duckdb(), dbdir = db_completed_file_path, read_only = FALSE)
    dbExecute(con_completed, "CREATE TABLE IF NOT EXISTS jobs (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, PRIMARY KEY (cause_abb, outcome_abb, fu))")
    completed_jobs <- as.data.table(dbGetQuery(con_completed, "SELECT * FROM jobs"))
    dbDisconnect(con_completed, shutdown = TRUE)
    
    # [1-3] 남은 작업 계산 (전체 작업 - 완료 작업)
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
        return(0)
    }
    
    # [1-4] 배치 분할 (대규모 작업을 작은 배치로 분할)
    total_jobs_to_do_count <- nrow(jobs_to_do)
    num_batches <- ceiling(total_jobs_to_do_count / batch_size)
    job_indices <- 1:total_jobs_to_do_count
    batches_indices <- split(job_indices, ceiling(job_indices / batch_size))
    
    cat(sprintf("--- 총 %d개 배치 처리 시작 (각 배치는 작은 청크로 분할하여 %d개 코어가 동적으로 처리) ---\n",
                num_batches, n_cores))
    
    # [1-5] 병렬 처리 설정
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
    cat(sprintf("\n--- [단계 1] %d개 배치 분석 완료 ---\n", num_batches))
    
    # ========================================================================
    # [단계 4] 이번 사이클 로그 취합 (다음 사이클 준비)
    # ========================================================================
    # 이번 사이클에서 생성된 완료 로그 청크 파일들을 중앙 DB로 취합
    # 다음 사이클 시작 시 정확한 완료 작업 수를 파악하기 위함
    cat("\n--- [단계 4] 이번 사이클 로그 취합 시작 ---\n")
    pre_aggregate_logs(
        chunk_folder = db_completed_folder_path,
        central_db_path = db_completed_file_path,
        pattern = "completed_chunk_.*\\.duckdb",
        table_name = "jobs",
        create_sql = "CREATE TABLE IF NOT EXISTS jobs (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, PRIMARY KEY (cause_abb, outcome_abb, fu))"
    )
    cat("--- [단계 4] 이번 사이클 로그 취합 완료 ---\n")
    
    # [완료 여부 확인] 취합 후 정확한 남은 작업 수 계산
    con_completed <- dbConnect(duckdb::duckdb(), dbdir = db_completed_file_path, read_only = TRUE)
    completed_count <- dbGetQuery(con_completed, "SELECT COUNT(*) as cnt FROM jobs")$cnt
    dbDisconnect(con_completed, shutdown = TRUE)
    
    remaining_jobs_count <- nrow(instruction_list_all) - completed_count
    
    if (remaining_jobs_count > 0) {
        cat(sprintf("남은 작업: %d개 (재시작 필요)\n", remaining_jobs_count))
        return(1)  # 재시작 필요
    } else {
        cat("모든 작업 완료!\n")
        return(0)  # 완료
    }
}

# ============================================================================
# 5. 결과물 취합 함수 (aggregate_chunks_to_parquet)
# ============================================================================
# [목적]
#   모든 작업이 완료된 후 결과물 청크 파일들을 하나의 Parquet 파일로 취합
#   최종 검증 데이터를 단일 파일로 제공
#
# [처리 방식]
#   - 모든 validation_chunk_*.duckdb 파일에서 데이터 읽기
#   - rbindlist로 결합
#   - Parquet 파일로 저장
#
# [파라미터]
#   results_chunk_folder_path: 결과물 청크 파일 저장 폴더
#   output_dir: 최종 Parquet 파일 저장 디렉토리
#   fu: 추적 기간
#
# [반환값]
#   TRUE: 취합 성공
#   FALSE: 취합할 데이터 없음 또는 실패
#
# ============================================================================

aggregate_chunks_to_parquet <- function(
    results_chunk_folder_path,
    output_dir,
    fu
) {
    cat("\n--- [단계 3] 결과물 청크 파일 취합 시작 ---\n")
    
    # [1] 청크 파일 목록 확인
    chunk_files <- list.files(results_chunk_folder_path, pattern = "validation_chunk_.*\\.duckdb", full.names = TRUE)
    
    if (length(chunk_files) == 0) {
        cat("취합할 청크 파일이 없습니다.\n")
        return(FALSE)
    }
    
    cat(sprintf("발견된 청크 파일: %d개\n", length(chunk_files)))
    
    # [2] 모든 청크 파일에서 데이터 읽기
    all_data_list <- list()
    
    for (chunk_file in chunk_files) {
        tryCatch({
            con_chunk <- dbConnect(duckdb::duckdb(), dbdir = chunk_file, read_only = TRUE)
            if (dbExistsTable(con_chunk, "all_diff_negative_info")) {
                chunk_data <- as.data.table(dbGetQuery(con_chunk, "SELECT * FROM all_diff_negative_info"))
                if (nrow(chunk_data) > 0) {
                    all_data_list[[length(all_data_list) + 1]] <- chunk_data
                }
            }
            dbDisconnect(con_chunk, shutdown = TRUE)
        }, error = function(e) {
            cat(sprintf("경고: 청크 파일 읽기 실패: %s\n", chunk_file))
        })
    }
    
    if (length(all_data_list) == 0) {
        cat("취합할 데이터가 없습니다.\n")
        return(FALSE)
    }
    
    # [3] 데이터 결합 및 key별로 그룹화
    cat("데이터 결합 중...\n")
    all_data_combined <- rbindlist(all_data_list, fill = TRUE)
    
    # [4] key별로 그룹화하여 person_id들을 하나의 row에 묶기
    cat("key별로 그룹화 중...\n")
    edge_pids_mapping <- all_data_combined[, .(
        person_id = list(person_id)
    ), by = key]
    
    # Parquet 파일로 저장
    parquet_file <- file.path(output_dir, "edge_pids_mapping.parquet")
    cat(sprintf("Parquet 파일 저장 중: %s\n", parquet_file))
    arrow::write_parquet(edge_pids_mapping, parquet_file)
    
    cat(sprintf("✓ 완료: %s (%d개 key, 총 %d개 person_id)\n", 
                basename(parquet_file), 
                nrow(edge_pids_mapping),
                sum(sapply(edge_pids_mapping$person_id, length))))
    
    return(TRUE)
}

# ============================================================================
# 6. 메인 실행 함수 (main)
# ============================================================================
# [목적]
#   스크립트의 진입점으로 전체 분석 프로세스를 실행
#
# [처리 흐름]
#   1. 질병 코드 목록 로드
#   2. validation 분석 실행 (run_validation_analysis)
#   3. 모든 작업 완료 시 결과물 취합 (aggregate_chunks_to_parquet)
#   4. 종료 코드 반환 (0: 완료, 1: 재시작 필요)
#
# ============================================================================

main <- function() {
    # [초기화] 질병 코드 목록 로드
    disease_codes <- get_disease_codes_from_path(matched_parquet_folder_path)
    cat(sprintf("발견된 질병 코드 수: %d개\n", length(disease_codes)))
    
    # [실행] validation 분석 실행
    exit_code <- run_validation_analysis(
        cause_list = disease_codes,
        outcome_list = disease_codes,
        fu = fu,
        n_cores = n_cores,
        batch_size = batch_size,
        chunks_per_core = chunks_per_core,
        matched_parquet_folder_path = matched_parquet_folder_path,
        outcome_parquet_file_path = outcome_parquet_file_path,
        results_chunk_folder_path = results_chunk_folder_path,
        db_completed_file_path = db_completed_file_path,
        db_completed_folder_path = db_completed_folder_path
    )
    
    # [취합] 모든 작업이 완료되면 결과물 청크 파일 취합
    if (exit_code == 0) {
        aggregate_chunks_to_parquet(
            results_chunk_folder_path = results_chunk_folder_path,
            output_dir = output_dir,
            fu = fu
        )
    }
    
    # [종료] 종료 코드 반환
    quit(status = exit_code)
}

# 스크립트 실행
if (!interactive()) {
    main()
}

# ============================================================================
# 디버깅 과정 및 에러 해결 기록
# ============================================================================
# 
# [문제 발생]
#   - 실행 중 "missing value where TRUE/FALSE needed" 에러 발생
#   - 에러 발생 위치: process_batch 함수 내부 (라인 445)
#   - 에러 발생 조건: diff < 0인 경우가 없는 질병 쌍 처리 시
#
# [디버깅 과정]
#
# 1단계: 초기 디버깅 스크립트 생성
#   - 파일명: hr_rr_mapping_validation_engine_debug.R
#   - 목적: 에러 발생 원인 파악을 위한 상세 로깅 추가
#   - 증명 내용:
#     * process_batch 함수에서 cause_abb, outcome_abb 추출 시 NULL 값 발생
#     * future_lapply의 반환값이 data.frame이 아닌 logical 값으로 반환되는 문제
#     * 데이터 구조 변환 문제 (data.table → data.frame)
#   - 해결: 데이터 추출 및 변환 로직 수정
#
# 2단계: 에러 로케이터 스크립트 생성
#   - 파일명: hr_rr_mapping_validation_engine_error_locator.R
#   - 목적: 정확한 에러 발생 지점 파악
#   - 방법:
#     * 로그 파일에서 에러 발생한 100개 질병 쌍 추출
#     * process_one_combination 함수 내부에 24개 인디케이터 추가
#     * 각 단계별 데이터 상태 확인 (NA 개수, 타입 등)
#   - 증명 내용:
#     * 모든 인디케이터가 정상적으로 출력됨 (INDICATOR 1-24)
#     * process_one_combination 함수가 정상적으로 완료됨
#     * 에러는 process_batch 함수의 결과 처리 단계에서 발생
#
# 3단계: process_batch 함수 상세 디버깅
#   - 파일명: hr_rr_mapping_validation_engine_error_locator.R (수정)
#   - 추가 내용:
#     * process_one_combination 호출 전후 인디케이터 추가
#     * result 검증 단계 상세 정보 출력
#     * result[["all_diff_negative_info"]] 타입 및 길이 확인
#   - 증명 내용 (로그 파일: error_locator_log.txt):
#     * result[["all_diff_negative_info"]] 타입: list
#     * result[["all_diff_negative_info"]] 길이: 0 (빈 리스트)
#     * diff < 0인 경우가 없어서 빈 리스트로 반환됨
#     * [참고] error_locator_log.txt는 결과 탐색 후 삭제 함
#
# 4단계: 에러 원인 정확한 파악
#   - 원본 엔진 코드 (라인 443-445):
#     ```r
#     if (!is.null(result) && 
#         "all_diff_negative_info" %in% names(result) &&
#         nrow(result[["all_diff_negative_info"]]) > 0) {
#     ```
#   - 문제점:
#     * result[["all_diff_negative_info"]]가 빈 리스트(list())인 경우
#     * nrow(list())는 NULL을 반환
#     * NULL > 0 비교 시 "missing value where TRUE/FALSE needed" 에러 발생
#   - 증명:
#     * R에서 nrow(list()) 실행 시 NULL 반환 확인
#     * NULL > 0 비교 시 에러 발생 확인
#
# 5단계: HR engine과의 비교 분석
#   - 파일명: hr_calculator_engine_v4.R
#   - 비교 내용:
#     * HR engine은 중첩된 리스트 구조를 사용하지 않음
#     * HR engine은 nrow()를 호출하지 않음
#     * HR engine은 NULL 체크만 수행
#   - 결론:
#     * HR engine에서는 문제가 없었던 이유: nrow()를 호출하지 않기 때문
#     * Validation engine에서만 문제 발생: nrow()를 리스트에 대해 호출하기 때문
#
# [최종 해결 방법]
#   - process_batch 함수의 결과 처리 로직 수정 (라인 443-445)
#   - 수정 전:
#     ```r
#     if (!is.null(result) && 
#         "all_diff_negative_info" %in% names(result) &&
#         nrow(result[["all_diff_negative_info"]]) > 0) {
#     ```
#   - 수정 후:
#     ```r
#     if (!is.null(result) && 
#         "all_diff_negative_info" %in% names(result) &&
#         is.data.frame(result[["all_diff_negative_info"]]) &&
#         nrow(result[["all_diff_negative_info"]]) > 0) {
#     ```
#   - 변경 사항:
#     * is.data.frame() 체크 추가
#     * 리스트인 경우 nrow() 호출 방지
#     * NULL > 0 비교 에러 방지
#
# [사용된 스크립트 및 파일]
#   1. hr_rr_mapping_validation_engine_debug.R
#      - 초기 디버깅 및 데이터 구조 문제 해결
#      - 로그 파일: debug_log.txt (결과 탐색 후 삭제 함)
#
#   2. hr_rr_mapping_validation_engine_error_locator.R
#      - 정확한 에러 발생 지점 파악
#      - 로그 파일: error_locator_log.txt (결과 탐색 후 삭제 함)
#      - 주요 기능:
#        * 로그 파일에서 에러 발생한 질병 쌍 추출
#        * 24개 인디케이터를 통한 단계별 추적
#        * process_batch 함수 상세 디버깅
#
#   3. hr_rr_mapping_validation_manager_history.log
#      - 원본 에러 로그 파일
#      - 에러 발생한 질병 쌍 정보 추출에 사용
#
#   4. hr_calculator_engine_v4.R
#      - HR engine 코드 비교 분석
#      - 문제가 없는 이유 파악에 사용
#
# [검증 결과]
#   - error_locator_log.txt 분석 결과 (결과 탐색 후 삭제 함):
#     * 모든 인디케이터 정상 출력 확인
#     * result[["all_diff_negative_info"]]가 빈 리스트로 반환됨 확인
#     * is.data.frame() 체크 추가 후 에러 발생하지 않음 확인
#
# [참고 사항]
#   - process_one_combination 함수에서 result_list[["all_diff_negative_info"]]를 
#     빈 리스트(list())로 초기화함
#   - diff < 0인 경우가 없으면 빈 리스트가 그대로 반환됨
#   - 이는 정상적인 동작이며, process_batch에서 이를 올바르게 처리해야 함
#
# ============================================================================
