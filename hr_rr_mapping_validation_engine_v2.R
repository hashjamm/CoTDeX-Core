#!/usr/bin/env Rscript
# ============================================================================
# HR Engine에서 diff < 0인 경우 제거된 매핑 데이터 수집 및 저장 (Engine 버전 v2)
# ============================================================================
# 
# [목적]
# - hr_calculator_engine_v4.R에서 clean_data로부터 diff < 0인 경우를 제거하는데,
#   이렇게 제거된 케이스들에 대한 매핑 데이터를 수집하여 저장
# - 저장된 데이터는 RR 쪽에서 outcome_dt 기반으로 생성한 매핑 데이터와 비교 검증에 사용
# - 검증이 성공하면 RR 쪽에 대한 새로운 engine 구축 예정
#
# [v2 주요 변경 사항]
# - 해결 방법 2 적용: HR engine과 동일한 구조로 변경
#   * process_one_combination 함수 제거
#   * process_batch 함수 내에서 직접 처리 (HR engine 구조 참고)
#   * 중첩된 리스트 구조(result[["all_diff_negative_info"]]) 사용하지 않음
#   * 에러 원인: nrow(list())는 NULL을 반환하고, NULL > 0 비교 시 에러 발생
#   * 해결: 구조 변경으로 근본적 해결 (HR engine과 일관성 유지)
#
# [사용 방법]
#   Manager 스크립트를 통한 실행 (권장):
#     1. manager 스크립트의 R_SCRIPT 경로를 v2로 수정:
#        R_SCRIPT="/home/hashjamm/codes/disease_network/hr_rr_mapping_validation_engine_v2.R"
#     
#     2. tmux 세션에서 실행:
#        # 상대 경로 사용 (스크립트가 있는 디렉토리에서 실행)
#        tmux new-session -d -s validation_v2 './hr_rr_mapping_validation_manager.sh'
#        # 절대 경로 사용 (어느 디렉토리에서든 실행 가능)
#        tmux new-session -d -s validation_v2 '/home/hashjamm/codes/disease_network/hr_rr_mapping_validation_manager.sh'
#        
#     3. 프로그래스 확인 (다른 터미널에서):
#        tmux attach-session -t validation_v2
#        
#     4. tmux 세션에서 나가기 (프로세스는 계속 실행): Ctrl+B, D
#        세션 종료: tmux kill-session -t validation_v2
#     
#     - 자동 재시작, 메모리 제한, 진행 상황 모니터링 등 제공
#     - fu 파라미터 불필요 (내부적으로 fu=1 사용)
#
#   직접 실행 (테스트용):
#     Rscript hr_rr_mapping_validation_engine_v2.R
#     (내부적으로 fu=1 사용, 결과는 fu 값과 무관)
#
# [이어쓰기 기능]
#   - vv1의 완료 기록을 읽어서 이어서 처리합니다
#   - 동일한 경로(/home/hashjamm/results/disease_network/hr_rr_mapping_validation_results/) 사용
#   - validation_completed_jobs.duckdb에서 완료된 작업 확인
#   - 남은 작업만 처리하여 중복 방지
#
# [주의 사항]
#   - vv1과 동일한 경로를 사용하므로 vv1의 결과를 이어서 처리합니다
#   - vv1에서 에러가 발생한 작업들은 vv2에서 재처리됩니다
#   - vv1에서 정상 완료된 작업들은 건너뜁니다
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

# [경로] 결과물 청크 파일 폴더
results_chunk_folder_path <- file.path(output_dir, "validation_chunks")
dir.create(results_chunk_folder_path, recursive = TRUE, showWarnings = FALSE)

# ============================================================================
# 3. 유틸리티 함수
# ============================================================================

# ============================================================================
# 함수: 질병 코드 목록 추출
# ============================================================================
# [목적]
#   matched_*.parquet 파일 목록에서 질병 코드 추출
#
# [파라미터]
#   matched_parquet_folder_path: matched_*.parquet 파일이 있는 폴더 경로
#
# [반환값]
#   질병 코드 벡터 (대문자)
#
# ============================================================================

get_disease_codes_from_path <- function(matched_parquet_folder_path) {
    parquet_files <- list.files(matched_parquet_folder_path, pattern = "^matched_.*\\.parquet$", full.names = FALSE)
    disease_codes <- gsub("^matched_", "", gsub("\\.parquet$", "", parquet_files))
    disease_codes <- toupper(disease_codes)
    return(sort(unique(disease_codes)))
}

# ============================================================================
# 함수: 로그 취합 (pre_aggregate_logs)
# ============================================================================
# [목적]
#   청크 파일의 로그를 중앙 DB로 취합
#
# [파라미터]
#   chunk_folder: 청크 파일이 있는 폴더 경로
#   central_db_path: 중앙 DB 파일 경로
#   pattern: 청크 파일 패턴
#   table_name: 테이블 이름
#   create_sql: 테이블 생성 SQL
#   silent: 로그 출력 여부 (기본값: FALSE)
#
# [v2 수정] HR engine 및 v1과 동일한 ATTACH/DETACH 방식 적용
#   - 기존: 각 청크 파일을 개별 연결하여 읽기 (동시성 문제 가능)
#   - 변경: ATTACH/DETACH 방식 사용 (HR engine과 일관성 유지)
#   - 실패한 파일 자동 삭제로 재시도 가능
#   - 성공한 파일 삭제로 중복 처리 방지
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
# 함수: 배치 처리 (process_batch) - v2: HR engine 구조 적용
# ============================================================================
# [v2 변경 사항]
#   - process_one_combination 함수 제거
#   - process_batch 함수 내에서 직접 처리 (HR engine 구조 참고)
#   - 중첩된 리스트 구조 사용하지 않음
#   - diff_negative_info를 직접 batch_diff_negative_info에 추가
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
    
    # 배치 내 각 작업 처리 (HR engine과 동일한 방식 - 직접 처리)
    for (i in 1:nrow(batch_jobs)) {
        current_job <- batch_jobs[i, ]
        cause_abb <- current_job$cause_abb
        outcome_abb <- current_job$outcome_abb
        
        tryCatch({
            # [v2 변경] process_one_combination 함수 호출 제거, 직접 처리
            # --- 1. DuckDB 쿼리로 데이터 로드 ---
            con_duck <- dbConnect(duckdb::duckdb())
            matched_parquet_file_path <- file.path(matched_parquet_folder_path, 
                                                   sprintf("matched_%s.parquet", tolower(cause_abb)))
            
            if (!file.exists(matched_parquet_file_path)) {
                dbDisconnect(con_duck, shutdown = TRUE)
                # 완료 기록 (파일이 없어도 완료로 기록)
                batch_completed_jobs[[length(batch_completed_jobs) + 1]] <- data.frame(
                    cause_abb = cause_abb,
                    outcome_abb = outcome_abb,
                    fu = fu,
                    stringsAsFactors = FALSE
                )
                next
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
            
            # --- 2. 전처리 (hr_calculator_engine_v4.R의 process_batch와 동일) ---
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
            
            # --- 3. diff < 0인 경우의 matched_id 추출 ---
            problem_ids <- clean_data[diff < 0, unique(matched_id)]
            
            # [v2 변경] 중첩된 리스트 구조 사용하지 않음, 직접 추가
            if (length(problem_ids) > 0) {
                diff_negative_data <- clean_data[matched_id %in% problem_ids]
                key <- paste(cause_abb, outcome_abb, sep = "_")
                
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
                info_df$key <- key
                
                # [v2 변경] 직접 batch_diff_negative_info에 추가 (중첩 구조 없음)
                batch_diff_negative_info[[length(batch_diff_negative_info) + 1]] <- info_df
            }
            
            # 완료 기록 (결과 유무와 무관하게 기록)
            batch_completed_jobs[[length(batch_completed_jobs) + 1]] <- data.frame(
                cause_abb = cause_abb,
                outcome_abb = outcome_abb,
                fu = fu,
                stringsAsFactors = FALSE
            )
            
            # 메모리 정리
            rm(clean_data)
            if (exists("diff_negative_data")) rm(diff_negative_data)
            if (exists("problem_ids")) rm(problem_ids)
            gc()
            
        }, error = function(e) {
            # 오류 발생 시에도 완료 기록 (재시도 방지)
            cat(sprintf("경고: %s -> %s 처리 중 오류 발생: %s\n", 
                       cause_abb, outcome_abb, e$message))
            batch_completed_jobs[[length(batch_completed_jobs) + 1]] <- data.frame(
                cause_abb = cause_abb,
                outcome_abb = outcome_abb,
                fu = fu,
                stringsAsFactors = FALSE
            )
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
    # [단계 0] 사전 로그 취합 (이어하기 준비)
    cat("\n--- [단계 0] 사전 로그 취합 (이어하기 준비) ---\n")
    pre_aggregate_logs(
        chunk_folder = db_completed_folder_path,
        central_db_path = db_completed_file_path,
        pattern = "completed_chunk_.*\\.duckdb",
        table_name = "jobs",
        create_sql = "CREATE TABLE IF NOT EXISTS jobs (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, PRIMARY KEY (cause_abb, outcome_abb, fu))"
    )
    
    # [단계 1] diff < 0 케이스 수집 시작
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
    
    # [단계 4] 이번 사이클 로그 취합 (다음 사이클 준비)
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

