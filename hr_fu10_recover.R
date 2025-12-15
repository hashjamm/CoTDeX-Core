# ============================================================================
# v10 매핑 데이터 복구 스크립트
# ============================================================================
# 목적: total_hr_results_10.parquet에 있는 조합쌍에 대해서만
#       v5의 청크 파일들에서 매핑 데이터를 복구하여 v10_recovered에 저장
#
# [v10_recover vs hr_calculator_engine 주요 차이점]
# ============================================================================
# 1. 목적 및 역할
#    - v10_recover: HR 분석 없이 매핑 데이터만 생성하는 복구 전용 스크립트
#    - hr_calculator_engine: HR 분석과 매핑 데이터를 모두 생성하는 메인 분석 엔진
#
# 2. HR 분석 수행 여부
#    - v10_recover: HR 분석 없음 (perform_hr_analysis() 호출 없음)
#    - hr_calculator_engine: HR 분석 수행 (Cox 회귀, 경쟁위험 분석)
#
# 3. 출력 파일
#    - v10_recover: map_chunk_*.duckdb만 생성 (매핑 데이터만)
#    - hr_calculator_engine: hr_chunk_*.duckdb + map_chunk_*.duckdb 생성
#
# 4. 완료 로그 기록 조건
#    - v10_recover: 매핑 데이터가 있거나 통계 실패 로그가 있는 경우만 기록
#    - hr_calculator_engine: HR 결과가 있거나 통계 실패 로그가 있는 경우만 기록
#
# 5. 배치 쓰기 순서
#    - v10_recover: Map 데이터 → 통계 실패 로그 → 시스템 실패 로그 → 완료 로그
#    - hr_calculator_engine: HR 결과 → 통계 실패 로그 → 시스템 실패 로그 → 완료 로그 → Map 데이터
#
# 6. 데이터 검증 로직
#    - v10_recover: 검증 로직 없음 (HR 분석이 없으므로 불필요)
#    - hr_calculator_engine: 성공 로그와 HR 결과/통계 실패 로그 일치 여부 검증
#
# 7. 통계 실패 로그
#    - v10_recover: n=0 케이스만 기록 (HR 분석이 없으므로 통계 실패는 원칙적으로 발생 불가)
#    - hr_calculator_engine: HR 분석 실패 및 n=0 케이스 모두 기록
#
# 8. 공통점
#    - 데이터 로딩/전처리 로직 동일 (DuckDB 쿼리, 날짜 변환, status 계산 등)
#    - 성공/통계 실패/시스템 실패 로그 관리 방식 동일
#    - 배치 처리 및 병렬 처리 구조 동일
# ============================================================================

library(arrow)
library(dplyr)
library(duckdb)
library(DBI)
library(data.table)
library(glue)
library(future)
library(future.apply)
library(progressr)

# progressr 핸들러 설정: 터미널 출력 비활성화 (Bash 프로그래스 바와 충돌 방지)
progressr::handlers(global = TRUE)
options(progressr.enable = TRUE)  # 프로그레스 바 강제 활성화
# 터미널 출력은 비활성화하여 Bash 프로그래스 바와의 충돌 방지 (로그 파일에는 기록됨)
progressr::handlers(progressr::handler_void())  # 터미널 출력 비활성화

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

# 복구 함수
recover_mapping_data <- function(
    hr_results_file,
    v5_chunk_folder,
    output_folder,
    fu = 10
) {
    cat("\n==========================================================\n")
    cat("v10 매핑 데이터 복구 시작\n")
    cat("==========================================================\n")
    
    # 1. HR 결과 파일에서 조합쌍 읽기
    cat("\n1. HR 결과 파일에서 조합쌍 읽기 중...\n")
    hr_results <- arrow::read_parquet(hr_results_file)
    target_combinations <- hr_results %>%
        select(cause_abb, outcome_abb, fu) %>%
        distinct() %>%
        mutate(key = paste(cause_abb, outcome_abb, fu, sep = "_"))
    
    cat(sprintf("   - 총 %d개 조합쌍 발견\n", nrow(target_combinations)))
    cat(sprintf("   - 예시: %s\n", paste(head(target_combinations$key, 5), collapse = ", ")))
    
    # 2. v5 청크 파일 목록 가져오기
    cat("\n2. v5 청크 파일 목록 가져오기 중...\n")
    map_chunk_files <- list.files(v5_chunk_folder, pattern="map_chunk_.*\\.duckdb", full.names=TRUE)
    cat(sprintf("   - 총 %d개 청크 파일 발견\n", length(map_chunk_files)))
    
    # 3. Edge 매핑 데이터 취합 (aggregate 함수 로직 사용)
    cat("\n3. Edge 매핑 데이터 취합 중...\n")
    con_map_agg <- dbConnect(duckdb::duckdb())
    on.exit(dbDisconnect(con_map_agg, shutdown=TRUE), add=TRUE)
    
    # 각 DuckDB 파일을 ATTACH하여 데이터 취합
    all_edge_pids <- list()
    all_edge_index <- list()
    all_edge_key <- list()
    
    # v5 청크에서 찾은 key 목록 추적 (누락 조합 쌍 확인용)
    found_keys_in_chunks <- character(0)
    
    successful_chunks <- 0
    failed_chunks <- 0
    
    for (i in 1:length(map_chunk_files)) {
        tryCatch({
            dbExecute(con_map_agg, sprintf("ATTACH '%s' AS db%d (READ_ONLY)", map_chunk_files[i], i))
            
            # 각 테이블에서 데이터 읽기 (테이블이 없으면 자동으로 에러 처리됨)
            tryCatch({
                pids_data <- dbGetQuery(con_map_agg, sprintf("SELECT * FROM db%d.edge_pids", i))
                if (nrow(pids_data) > 0) {
                    # target_combinations에 있는 key만 필터링
                    pids_filtered <- pids_data %>%
                        filter(key %in% target_combinations$key)
                    if (nrow(pids_filtered) > 0) {
                        all_edge_pids[[length(all_edge_pids) + 1]] <- pids_filtered
                        # 찾은 key 목록에 추가
                        found_keys_in_chunks <- unique(c(found_keys_in_chunks, pids_filtered$key))
                    }
                }
            }, error = function(e) {}) # 테이블이 없으면 무시
            
            tryCatch({
                index_data <- dbGetQuery(con_map_agg, sprintf("SELECT * FROM db%d.edge_index_key_seq", i))
                if (nrow(index_data) > 0) {
                    # target_combinations에 있는 key만 필터링
                    index_filtered <- index_data %>%
                        filter(key %in% target_combinations$key)
                    if (nrow(index_filtered) > 0) {
                        all_edge_index[[length(all_edge_index) + 1]] <- index_filtered
                        # 찾은 key 목록에 추가
                        found_keys_in_chunks <- unique(c(found_keys_in_chunks, index_filtered$key))
                    }
                }
            }, error = function(e) {}) # 테이블이 없으면 무시
            
            tryCatch({
                key_data <- dbGetQuery(con_map_agg, sprintf("SELECT * FROM db%d.edge_key_seq", i))
                if (nrow(key_data) > 0) {
                    # target_combinations에 있는 key만 필터링
                    key_filtered <- key_data %>%
                        filter(key %in% target_combinations$key)
                    if (nrow(key_filtered) > 0) {
                        all_edge_key[[length(all_edge_key) + 1]] <- key_filtered
                        # 찾은 key 목록에 추가
                        found_keys_in_chunks <- unique(c(found_keys_in_chunks, key_filtered$key))
                    }
                }
            }, error = function(e) {}) # 테이블이 없으면 무시
            
            dbExecute(con_map_agg, sprintf("DETACH db%d", i))
            successful_chunks <- successful_chunks + 1
            
            # 진행 상황 출력 (매 50개 청크마다)
            if (i %% 50 == 0) {
                cat(sprintf("     진행: %d/%d 청크 처리 완료\n", i, length(map_chunk_files)))
            }
        }, error = function(e) {
            failed_chunks <<- failed_chunks + 1
            cat(sprintf("경고: 매핑 청크 파일 읽기 실패 %s: %s\n", basename(map_chunk_files[i]), e$message))
        })
    }
    
    cat(sprintf("   - 청크 처리 완료: 성공 %d개, 실패 %d개\n", successful_chunks, failed_chunks))
    
    # 4. 데이터 결합 및 그룹화 (aggregate 함수 로직)
    cat("\n4. 데이터 결합 및 그룹화 중...\n")
    
    if (length(all_edge_pids) > 0) {
        cat("   - edge_pids 데이터 결합 중...\n")
        # 타입 통일: person_id를 numeric으로 변환
        all_edge_pids <- lapply(all_edge_pids, function(x) {
            x$person_id <- as.numeric(x$person_id)
            return(x)
        })
        edge_pids_combined <- distinct(bind_rows(all_edge_pids))
        rm(all_edge_pids); gc()  # 즉시 메모리 해제
        
        edge_pids_df <- edge_pids_combined %>%
            group_by(key) %>%
            summarise(values = list(person_id), .groups = 'drop')
        rm(edge_pids_combined); gc()  # 즉시 메모리 해제
        
        edge_pids_list <- setNames(edge_pids_df$values, edge_pids_df$key)
        rm(edge_pids_df); gc()  # 즉시 메모리 해제
        
        save_mapping_to_parquet(edge_pids_list, "edge_pids", output_folder, fu)
        rm(edge_pids_list); gc()  # 파일 저장 후 즉시 메모리 해제
    } else {
        cat("   - edge_pids 데이터 없음\n")
    }
    
    if (length(all_edge_index) > 0) {
        cat("   - edge_index_key_seq 데이터 결합 중...\n")
        # 타입 통일: index_key_seq를 numeric으로 변환
        all_edge_index <- lapply(all_edge_index, function(x) {
            x$index_key_seq <- as.numeric(x$index_key_seq)
            return(x)
        })
        edge_index_combined <- distinct(bind_rows(all_edge_index))
        rm(all_edge_index); gc()  # 즉시 메모리 해제
        
        edge_index_df <- edge_index_combined %>%
            group_by(key) %>%
            summarise(values = list(index_key_seq), .groups = 'drop')
        rm(edge_index_combined); gc()  # 즉시 메모리 해제
        
        edge_index_key_seq_list <- setNames(edge_index_df$values, edge_index_df$key)
        rm(edge_index_df); gc()  # 즉시 메모리 해제
        
        save_mapping_to_parquet(edge_index_key_seq_list, "edge_index_key_seq", output_folder, fu)
        rm(edge_index_key_seq_list); gc()  # 파일 저장 후 즉시 메모리 해제
    } else {
        cat("   - edge_index_key_seq 데이터 없음\n")
    }
    
    if (length(all_edge_key) > 0) {
        cat("   - edge_key_seq 데이터 결합 중...\n")
        # 타입 통일: outcome_key_seq를 numeric으로 변환
        all_edge_key <- lapply(all_edge_key, function(x) {
            x$outcome_key_seq <- as.numeric(x$outcome_key_seq)
            return(x)
        })
        edge_key_combined <- distinct(bind_rows(all_edge_key))
        rm(all_edge_key); gc()  # 즉시 메모리 해제
        
        edge_key_df <- edge_key_combined %>%
            group_by(key) %>%
            summarise(values = list(outcome_key_seq), .groups = 'drop')
        rm(edge_key_combined); gc()  # 즉시 메모리 해제
        
        edge_key_seq_list <- setNames(edge_key_df$values, edge_key_df$key)
        rm(edge_key_df); gc()  # 즉시 메모리 해제
        
        save_mapping_to_parquet(edge_key_seq_list, "edge_key_seq", output_folder, fu)
        rm(edge_key_seq_list); gc()  # 파일 저장 후 즉시 메모리 해제
    } else {
        cat("   - edge_key_seq 데이터 없음\n")
    }
    
    # 메모리 정리 (found_keys_in_chunks는 나중에 사용하므로 유지)
    gc()
    
    # 5. 복구된 매핑 파일 검증 (상위 5개 행 확인)
    cat("\n5. 복구된 매핑 파일 검증 중...\n")
    
    parquet_files <- c(
        file.path(output_folder, sprintf("edge_pids_mapping_%d.parquet", fu)),
        file.path(output_folder, sprintf("edge_index_key_seq_mapping_%d.parquet", fu)),
        file.path(output_folder, sprintf("edge_key_seq_mapping_%d.parquet", fu))
    )
    
    for (parquet_file in parquet_files) {
        if (file.exists(parquet_file)) {
            cat(sprintf("\n   파일: %s\n", basename(parquet_file)))
            tryCatch({
                df <- arrow::read_parquet(parquet_file)
                cat(sprintf("     총 행 수: %d\n", nrow(df)))
                cat("     상위 5개 행:\n")
                print(head(df, 5))
            }, error = function(e) {
                cat(sprintf("     경고: 파일 읽기 실패: %s\n", e$message))
            })
        } else {
            cat(sprintf("\n   파일 없음: %s\n", basename(parquet_file)))
        }
    }
    
    # 6. 누락된 조합 쌍 확인
    cat("\n6. 누락된 조합 쌍 확인 중...\n")
    
    # HR 결과의 모든 key
    hr_keys <- target_combinations$key
    
    # v5 청크에서 찾은 key
    found_keys <- unique(found_keys_in_chunks)
    
    # 누락된 key 찾기
    missing_keys <- setdiff(hr_keys, found_keys)
    
    cat(sprintf("   - HR 결과의 총 조합 수: %d개\n", length(hr_keys)))
    cat(sprintf("   - v5 청크에서 찾은 조합 수: %d개\n", length(found_keys)))
    cat(sprintf("   - 누락된 조합 수: %d개\n", length(missing_keys)))
    
    if (length(missing_keys) > 0) {
        cat("\n   누락된 조합 쌍 (처음 20개):\n")
        missing_keys_sorted <- sort(missing_keys)
        for (i in 1:min(20, length(missing_keys_sorted))) {
            cat(sprintf("     %d. %s\n", i, missing_keys_sorted[i]))
        }
        if (length(missing_keys) > 20) {
            cat(sprintf("     ... (총 %d개 중 20개만 표시)\n", length(missing_keys)))
        }
        
        # 누락된 조합 쌍을 데이터프레임으로 변환하여 저장
        missing_parts <- strsplit(missing_keys, "_")
        missing_combinations <- data.frame(
            cause_abb = sapply(missing_parts, function(x) x[1]),
            outcome_abb = sapply(missing_parts, function(x) x[2]),
            fu = as.integer(sapply(missing_parts, function(x) x[3])),
            key = missing_keys,
            stringsAsFactors = FALSE
        )
        
        # 누락된 조합 쌍을 파일로 저장
        missing_file <- file.path(output_folder, sprintf("missing_combinations_%d.parquet", fu))
        arrow::write_parquet(missing_combinations, missing_file)
        cat(sprintf("\n   ✓ 누락된 조합 쌍 저장 완료: %s\n", basename(missing_file)))
        cat(sprintf("     → 이 조합들이 최종 recover 대상입니다.\n"))
    } else {
        cat("\n   ✓ 모든 조합이 v5 청크에서 찾아졌습니다!\n")
    }
    
    # 메모리 정리
    rm(target_combinations, hr_results, found_keys_in_chunks)
    gc()
    
    cat("\n==========================================================\n")
    cat("복구 완료!\n")
    cat("==========================================================\n")
    cat(sprintf("출력 폴더: %s\n", output_folder))
    cat(sprintf("복구된 매핑 파일:\n"))
    cat(sprintf("  - edge_pids_mapping_%d.parquet\n", fu))
    cat(sprintf("  - edge_index_key_seq_mapping_%d.parquet\n", fu))
    cat(sprintf("  - edge_key_seq_mapping_%d.parquet\n", fu))
    if (length(missing_keys) > 0) {
        cat(sprintf("\n최종 recover 대상 (누락된 조합):\n"))
        cat(sprintf("  - missing_combinations_%d.parquet (%d개 조합)\n", fu, length(missing_keys)))
    }
    cat("==========================================================\n")
}

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
        successfully_processed_files <- character(0)
        
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
                
                tryCatch({
                    dbExecute(con_agg, sprintf("DETACH IF EXISTS db%d", i))
                }, error = function(e2) {})
                
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
        
        if (length(successfully_processed_files) > 0) {
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
# process_batch_mapping_only: HR 분석 없이 매핑 데이터만 생성하는 배치 처리 함수
# ============================================================================
# [hr_calculator_engine.R의 process_batch()와의 차이점]
# 1. HR 분석 수행 없음: perform_hr_analysis() 호출하지 않음
# 2. HR 결과 파일 생성 없음: db_hr_chunk_path 사용하지 않음
# 3. 완료 로그 조건: 매핑 데이터 기준 (HR 결과 기준 아님)
# 4. 배치 쓰기 순서: Map 데이터를 먼저 처리
# 5. 데이터 검증 로직 없음: HR 분석이 없으므로 검증 불필요
# 6. 통계 실패 로그: n=0 케이스만 기록 (HR 분석 실패는 발생 불가)
#
# [공통점]
# - 데이터 로딩/전처리 로직 동일 (DuckDB 쿼리, 날짜 변환, status 계산)
# - 성공/통계 실패/시스템 실패 로그 관리 방식 동일
# - 배치 처리 및 트랜잭션 관리 방식 동일
# ============================================================================
process_batch_mapping_only <- function(
    batch_jobs, # data.table: 처리할 작업 목록 (N개 행)
    fu,
    matched_parquet_folder_path,
    outcome_parquet_file_path,
    results_mapping_folder_path,
    db_completed_folder_path, # 성공 로그 청크 폴더
    db_system_failed_folder_path, # 시스템 실패 로그 청크 폴더
    db_stat_failed_folder_path # 통계 실패 로그 청크 폴더
) {
    # 워커별 프로그레스 바 설정 강제 활성화
    if (!isTRUE(getOption("progressr.enable"))) {
        options(progressr.enable = TRUE)
    }
    
    # 워커별 청크 파일 경로 설정
    worker_pid <- Sys.getpid()
    db_map_chunk_path <- file.path(results_mapping_folder_path, sprintf("map_chunk_%s.duckdb", worker_pid))
    db_completed_chunk_path <- file.path(db_completed_folder_path, sprintf("completed_chunk_%s.duckdb", worker_pid))
    db_system_failed_chunk_path <- file.path(db_system_failed_folder_path, sprintf("system_failed_chunk_%s.duckdb", worker_pid))
    db_stat_failed_chunk_path <- file.path(db_stat_failed_folder_path, sprintf("stat_failed_chunk_%s.duckdb", worker_pid))
    
    # RAM에 결과를 모을 임시 리스트 초기화
    batch_edge_pids <- list()
    batch_edge_index <- list()
    batch_edge_key <- list()
    batch_completed_jobs <- list()
    batch_system_failed_jobs <- list()
    batch_stat_failed_jobs <- list()
    
    # 배치 내 각 작업 처리
    progressr::with_progress({
        p_job <- progressr::progressor(steps = nrow(batch_jobs))
        
        for (i in 1:nrow(batch_jobs)) {
            current_job <- batch_jobs[i, ]
            cause_abb <- current_job$cause_abb
            outcome_abb <- current_job$outcome_abb
            
            tryCatch({
                # --- 1. DuckDB 쿼리 (hr_calculator_engine.R의 로직) ---
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
                
                # --- 2. 전처리 (hr_calculator_engine.R의 로직) ---
                clean_data[, `:=`(
                    index_date = as.IDate(index_date, "%Y%m%d"), 
                    death_date = as.IDate(paste0(dth_ym, "15"), "%Y%m%d"), 
                    end_date = as.IDate(paste0(2003 + fu, "1231"), "%Y%m%d"), 
                    event_date = as.IDate(recu_fr_dt, "%Y%m%d")
                )]
                clean_data[, final_date := fifelse(!is.na(event_date), pmin(event_date, end_date, na.rm = TRUE), pmin(death_date, end_date, na.rm = TRUE))]
                clean_data[, status := fifelse(!is.na(event_date), fifelse(event_date <= final_date, 1, 0), fifelse(!is.na(death_date) & death_date <= final_date, 2, 0))]
                clean_data[, diff := final_date - index_date]
                problem_ids <- clean_data[diff < 0, unique(matched_id)]
                if (length(problem_ids) > 0) clean_data <- clean_data[!matched_id %in% problem_ids]
                
                # --- 3. Mapping 데이터 추출 (HR 분석 없이) ---
                if (nrow(clean_data) > 0) {
                    key <- paste(cause_abb, outcome_abb, fu, sep = "_")
                    pids <- clean_data[case == 1, .(person_id)]
                    idx_key <- clean_data[case == 1, .(index_key_seq)]
                    out_key <- clean_data[case == 1 & status == 1, .(key_seq)]
                    
                    if (nrow(pids) > 0) {
                        batch_edge_pids[[length(batch_edge_pids) + 1]] <- data.frame(key = key, person_id = pids$person_id)
                    }
                    if (nrow(idx_key) > 0) {
                        batch_edge_index[[length(batch_edge_index) + 1]] <- data.frame(key = key, index_key_seq = idx_key$index_key_seq)
                    }
                    if (nrow(out_key) > 0) {
                        batch_edge_key[[length(batch_edge_key) + 1]] <- data.frame(key = key, outcome_key_seq = out_key$key_seq)
                    }
                    
                    # 성공 로그 기록
                    batch_completed_jobs[[length(batch_completed_jobs) + 1]] <- data.frame(cause_abb = cause_abb, outcome_abb = outcome_abb, fu = fu)
                } else {
                    # [주의] n=0 케이스: 통계 실패 로그에 기록
                    # 이 스크립트는 HR 분석을 수행하지 않으므로, 통계 실패는 원칙적으로 발생 불가능함
                    # 다만 n=0 (매칭된 데이터 없음) 케이스는 매핑 데이터 생성이 불가능하므로
                    # 통계 실패 로그에 기록하여 추적 가능하도록 함
                    # 참고: 이 케이스가 발생한다는 것은 HR 결과는 있지만 실제 매칭 데이터가 없다는 의미로,
                    # 데이터 불일치 가능성을 시사하므로 주의가 필요함
                    batch_stat_failed_jobs[[length(batch_stat_failed_jobs) + 1]] <- data.frame(
                        cause_abb = cause_abb, outcome_abb = outcome_abb, fu = fu,
                        error_msg = "n=0: No matched data available for mapping", timestamp = Sys.time()
                    )
                }
                
            }, error = function(sys_e) {
                # 시스템/R 실패 로그 기록
                parent_env <- parent.frame()
                parent_env$batch_system_failed_jobs[[length(parent_env$batch_system_failed_jobs) + 1]] <- data.frame(
                    cause_abb = cause_abb, outcome_abb = outcome_abb, fu = fu,
                    error_msg = as.character(sys_e$message), timestamp = Sys.time()
                )
            })
            
            p_job() # 각 작업 완료 시 프로그레스 업데이트
        }
    })
    
    # --- 일괄 쓰기 (트랜잭션 관리) ---
    con_map <- if (length(batch_edge_pids) > 0 || length(batch_edge_index) > 0 || length(batch_edge_key) > 0) dbConnect(duckdb::duckdb(), dbdir = db_map_chunk_path, read_only = FALSE) else NULL
    con_comp <- if (length(batch_completed_jobs) > 0) dbConnect(duckdb::duckdb(), dbdir = db_completed_chunk_path, read_only = FALSE) else NULL
    con_system_fail <- if (length(batch_system_failed_jobs) > 0) dbConnect(duckdb::duckdb(), dbdir = db_system_failed_chunk_path, read_only = FALSE) else NULL
    con_stat_fail <- if (length(batch_stat_failed_jobs) > 0) dbConnect(duckdb::duckdb(), dbdir = db_stat_failed_chunk_path, read_only = FALSE) else NULL
    
    on.exit({
        if (!is.null(con_map)) dbDisconnect(con_map, shutdown = TRUE)
        if (!is.null(con_comp)) dbDisconnect(con_comp, shutdown = TRUE)
        if (!is.null(con_system_fail)) dbDisconnect(con_system_fail, shutdown = TRUE)
        if (!is.null(con_stat_fail)) dbDisconnect(con_stat_fail, shutdown = TRUE)
    })
    
    # 트랜잭션 시작
    if (!is.null(con_map)) dbExecute(con_map, "BEGIN TRANSACTION;")
    if (!is.null(con_comp)) dbExecute(con_comp, "BEGIN TRANSACTION;")
    if (!is.null(con_system_fail)) dbExecute(con_system_fail, "BEGIN TRANSACTION;")
    if (!is.null(con_stat_fail)) dbExecute(con_stat_fail, "BEGIN TRANSACTION;")
    
    tryCatch({
        # [배치 쓰기 순서] v10_recover는 HR 분석이 없으므로 Map 데이터를 먼저 처리
        # hr_calculator_engine과의 차이: HR 결과 → 통계 실패 → 시스템 실패 → 완료 → Map 순서
        # v10_recover 순서: Map → 통계 실패 → 시스템 실패 → 완료
        
        # 1. Map 데이터 쓰기
        if (!is.null(con_map)) {
            dbExecute(con_map, "CREATE TABLE IF NOT EXISTS edge_pids (key VARCHAR, person_id BIGINT);")
            dbExecute(con_map, "CREATE TABLE IF NOT EXISTS edge_index_key_seq (key VARCHAR, index_key_seq BIGINT);")
            dbExecute(con_map, "CREATE TABLE IF NOT EXISTS edge_key_seq (key VARCHAR, outcome_key_seq BIGINT);")
            if (length(batch_edge_pids) > 0) dbWriteTable(con_map, "edge_pids", bind_rows(batch_edge_pids), append = TRUE)
            if (length(batch_edge_index) > 0) dbWriteTable(con_map, "edge_index_key_seq", bind_rows(batch_edge_index), append = TRUE)
            if (length(batch_edge_key) > 0) dbWriteTable(con_map, "edge_key_seq", bind_rows(batch_edge_key), append = TRUE)
            dbExecute(con_map, "COMMIT;")
        }
        
        # 2. 통계 실패 로그 쓰기 (n=0 케이스만 기록, 원칙적으로 발생 불가능)
        if (!is.null(con_stat_fail)) {
            if (length(batch_stat_failed_jobs) > 0) {
                dbExecute(con_stat_fail, "CREATE TABLE IF NOT EXISTS stat_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME)")
                dbWriteTable(con_stat_fail, "stat_failures", bind_rows(batch_stat_failed_jobs), append = TRUE)
                dbExecute(con_stat_fail, "COMMIT;")
            }
        } else if (length(batch_stat_failed_jobs) > 0) {
            con_stat_fail <- dbConnect(duckdb::duckdb(), dbdir = db_stat_failed_chunk_path, read_only = FALSE)
            dbExecute(con_stat_fail, "BEGIN TRANSACTION;")
            dbExecute(con_stat_fail, "CREATE TABLE IF NOT EXISTS stat_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME)")
            dbWriteTable(con_stat_fail, "stat_failures", bind_rows(batch_stat_failed_jobs), append = TRUE)
            dbExecute(con_stat_fail, "COMMIT;")
            dbDisconnect(con_stat_fail, shutdown = TRUE)
        }
        
        # 3. 시스템 실패 로그 쓰기
        if (!is.null(con_system_fail)) {
            dbExecute(con_system_fail, "CREATE TABLE IF NOT EXISTS system_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME)")
            dbWriteTable(con_system_fail, "system_failures", bind_rows(batch_system_failed_jobs), append = TRUE)
            dbExecute(con_system_fail, "COMMIT;")
        }
        
        # 4. 완료 로그 쓰기 (매핑 데이터가 있거나 통계 실패 로그가 있는 작업만)
        if (!is.null(con_comp)) {
            completed_keys <- if (length(batch_completed_jobs) > 0) {
                completed_df <- bind_rows(batch_completed_jobs)
                paste(completed_df$cause_abb, completed_df$outcome_abb, completed_df$fu, sep = "_")
            } else {
                character(0)
            }
            
            # 매핑 데이터 키 추출
            mapping_keys <- character(0)
            if (length(batch_edge_pids) > 0 || length(batch_edge_index) > 0 || length(batch_edge_key) > 0) {
                if (length(batch_edge_pids) > 0) {
                    mapping_keys <- unique(c(mapping_keys, sapply(batch_edge_pids, function(x) unique(x$key))))
                }
                if (length(batch_edge_index) > 0) {
                    mapping_keys <- unique(c(mapping_keys, sapply(batch_edge_index, function(x) unique(x$key))))
                }
                if (length(batch_edge_key) > 0) {
                    mapping_keys <- unique(c(mapping_keys, sapply(batch_edge_key, function(x) unique(x$key))))
                }
            }
            
            # 통계 실패 로그 키 추출
            stat_fail_keys <- if (length(batch_stat_failed_jobs) > 0) {
                sapply(batch_stat_failed_jobs, function(x) paste(x$cause_abb, x$outcome_abb, x$fu, sep = "_"))
            } else {
                character(0)
            }
            
            # 매핑 데이터나 통계 실패 로그가 있는 작업만 완료 로그에 기록
            valid_completed_keys <- intersect(completed_keys, c(mapping_keys, stat_fail_keys))
            
            if (length(valid_completed_keys) > 0) {
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
        
    }, error = function(e) {
        warning("Batch write failed, attempting rollback: ", e$message)
        if (!is.null(con_map)) try(dbExecute(con_map, "ROLLBACK;"), silent = TRUE)
        if (!is.null(con_comp)) try(dbExecute(con_comp, "ROLLBACK;"), silent = TRUE)
        if (!is.null(con_system_fail)) try(dbExecute(con_system_fail, "ROLLBACK;"), silent = TRUE)
        if (!is.null(con_stat_fail)) try(dbExecute(con_stat_fail, "ROLLBACK;"), silent = TRUE)
        stop(e)
    })
    
    # 메모리 정리
    rm(batch_edge_pids, batch_edge_index, batch_edge_key,
       batch_completed_jobs, batch_system_failed_jobs, batch_stat_failed_jobs)
    gc()
    
    return(TRUE)
}

# 누락 조합에 대한 mapping 생성 메인 함수
# ============================================================================
# 목적: missing_combinations_10.parquet에 있는 조합들에 대해 매핑 데이터 생성
#       (HR 결과는 있지만 v5에서 매핑 데이터를 찾지 못한 조합들)
#
# [hr_calculator_engine.R의 run_hr_analysis()와의 차이점]
# - HR 분석 수행 없음: process_batch_mapping_only() 호출 (HR 분석 없이 매핑만 생성)
# - HR 결과 파일 생성 없음: hr_chunk_*.duckdb 생성하지 않음
# - 완료 로그 조건: 매핑 데이터 기준
# - 나머지 로직은 동일 (배치 분할, 병렬 처리, 로그 관리 등)
#
# [파라미터]
# - 모든 파라미터는 필수이며, main() 함수에서 paths 리스트를 통해 전달됨
# - default 값 없음: 모든 값은 명시적으로 전달되어야 함
# ============================================================================
generate_mapping_for_missing_combinations <- function(
    missing_combinations_file,
    matched_parquet_folder_path,
    outcome_parquet_file_path,
    results_mapping_folder_path,
    db_completed_file_path, # 중앙 성공 로그 경로
    db_completed_folder_path, # 청크 성공 로그 폴더 경로
    db_system_failed_folder_path, # 청크 시스템 실패 로그 경로
    db_stat_failed_folder_path, # 청크 통계 실패 로그 경로
    fu,
    n_cores,
    batch_size,
    chunks_per_core
) {
    cat("\n==========================================================\n")
    cat("누락 조합에 대한 Edge Mapping 생성 시작\n")
    cat("==========================================================\n")
    
    # 1. 누락 조합 파일 읽기
    if (!file.exists(missing_combinations_file)) {
        stop(sprintf("누락 조합 파일을 찾을 수 없습니다: %s", missing_combinations_file))
    }
    
    cat("\n1. 누락 조합 파일 읽기 중...\n")
    missing_combinations <- arrow::read_parquet(missing_combinations_file)
    cat(sprintf("   - 총 %d개 누락 조합 발견\n", nrow(missing_combinations)))
    
    if (nrow(missing_combinations) == 0) {
        cat("   - 처리할 조합이 없습니다.\n")
        return(invisible(TRUE))
    }
    
    # 출력 디렉토리 생성
    if (!dir.exists(results_mapping_folder_path)) {
        dir.create(results_mapping_folder_path, recursive = TRUE)
        cat(sprintf("   - 출력 디렉토리 생성: %s\n", results_mapping_folder_path))
    }
    
    # 2. 작업 목록 준비 (완료된 작업 제외)
    con_completed <- dbConnect(duckdb::duckdb(), dbdir = db_completed_file_path, read_only = FALSE)
    dbExecute(con_completed, "CREATE TABLE IF NOT EXISTS jobs (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, PRIMARY KEY (cause_abb, outcome_abb, fu))")
    completed_jobs <- as.data.table(dbGetQuery(con_completed, "SELECT * FROM jobs"))
    dbDisconnect(con_completed, shutdown = TRUE)
    
    jobs_to_do <- as.data.table(missing_combinations[, c("cause_abb", "outcome_abb", "fu")])
    if (nrow(completed_jobs) > 0) {
        jobs_to_do <- jobs_to_do[!completed_jobs, on = c("cause_abb", "outcome_abb", "fu")]
    }
    
    cat(sprintf("\n2. 작업 목록 준비 완료: 전체 %d개 중, 기완료 %d개 제외, 총 %d개 작업 시작\n", 
        nrow(missing_combinations), nrow(completed_jobs), nrow(jobs_to_do)))
    
    if (nrow(jobs_to_do) == 0) {
        cat("   - 모든 작업이 이미 완료되었습니다.\n")
        return(invisible(0))
    }
    
    # 3. 배치로 분할
    total_jobs <- nrow(jobs_to_do)
    num_batches <- ceiling(total_jobs / batch_size)
    job_indices <- 1:total_jobs
    batches_indices <- split(job_indices, ceiling(job_indices / batch_size))
    
    cat(sprintf("\n3. 배치 처리 시작: %d개 배치 (각 배치는 %d개 코어로 처리)\n", num_batches, n_cores))
    
    # 4. 병렬 처리 설정
    plan(multisession, workers = n_cores, gc = TRUE, earlySignal = TRUE)
    required_packages <- c("data.table", "duckdb", "DBI", "arrow", "dplyr", "glue")
    
    progressr::with_progress({
        p <- progressr::progressor(steps = num_batches)
        
        for (batch_idx in 1:length(batches_indices)) {
            batch_indices <- batches_indices[[batch_idx]]
            current_batch_jobs <- jobs_to_do[batch_indices, ]
            
            # 작은 청크 기반 동적 할당
            batch_job_count <- nrow(current_batch_jobs)
            target_chunks <- n_cores * chunks_per_core
            chunk_size <- max(1, ceiling(batch_job_count / target_chunks))
            num_chunks <- ceiling(batch_job_count / chunk_size)
            chunk_indices_list <- split(1:batch_job_count, ceiling((1:batch_job_count) / chunk_size))
            
            cat(sprintf("    [배치 %d/%d] 작은 청크 기반 할당: %d개 작업 → %d개 청크 (청크당 평균 %d개)\n",
                        batch_idx, length(batches_indices), batch_job_count, num_chunks, chunk_size))
            
            # 각 청크를 워커에게 동적 병렬 할당
            results <- future_lapply(chunk_indices_list, function(chunk_indices) {
                chunk_jobs <- current_batch_jobs[chunk_indices, ]
                
                tryCatch({
                    process_batch_mapping_only(
                        batch_jobs = chunk_jobs,
                        fu = fu,
                        matched_parquet_folder_path = matched_parquet_folder_path,
                        outcome_parquet_file_path = outcome_parquet_file_path,
                        results_mapping_folder_path = results_mapping_folder_path,
                        db_completed_folder_path = db_completed_folder_path,
                        db_system_failed_folder_path = db_system_failed_folder_path,
                        db_stat_failed_folder_path = db_stat_failed_folder_path
                    )
                }, error = function(e) {
                    cat(sprintf("\n배치 처리 오류 (첫 작업: %s -> %s): %s\n", 
                        chunk_jobs[1, ]$cause_abb, 
                        chunk_jobs[1, ]$outcome_abb, 
                        e$message))
                })
                return(TRUE)
            }, future.seed = TRUE, future.packages = required_packages)
            
            p() # 배치 완료 시 프로그레스 업데이트
        }
    })
    
    plan(sequential)
    
    cat(sprintf("\n==========================================================\n"))
    cat("누락 조합에 대한 Edge Mapping 생성 완료\n")
    cat("==========================================================\n")
    cat(sprintf("출력 디렉토리: %s\n", results_mapping_folder_path))
    cat(sprintf("생성된 map_chunk 파일 수: %d개\n", 
        length(list.files(results_mapping_folder_path, pattern = "map_chunk_.*\\.duckdb", full.names = FALSE))))
    cat("==========================================================\n")
    
    # 남은 작업 수 계산
    con_completed <- dbConnect(duckdb::duckdb(), dbdir = db_completed_file_path, read_only = TRUE)
    completed_count <- dbGetQuery(con_completed, "SELECT COUNT(*) as cnt FROM jobs")$cnt
    dbDisconnect(con_completed, shutdown = TRUE)
    
    remaining_jobs <- nrow(missing_combinations) - completed_count
    
    if (remaining_jobs == 0) {
        return(invisible(0))
    } else {
        return(invisible(remaining_jobs))
    }
}

# 경로 설정
paths <- list(
    hr_results_file = "/home/hashjamm/results/disease_network/hr_results_fu10/total_hr_results_10.parquet",
    v5_chunk_folder = "/home/hashjamm/results/disease_network/hr_mapping_results_v5", # v5 매핑 파일 남은걸 사용했던 것. 지금은 정리해서 없음.
    output_folder = "/home/hashjamm/results/disease_network/hr_mapping_results_fu10_recovered",
    matched_parquet_folder = "/home/hashjamm/project_data/disease_network/matched_date_parquet/",
    outcome_parquet_file = "/home/hashjamm/project_data/disease_network/outcome_table.parquet",
    results_mapping_folder = "/home/hashjamm/results/disease_network/hr_mapping_results_fu10_recovered/hr_mapping_recovered_chunks/",
    db_completed_folder = "/home/hashjamm/results/disease_network/hr_job_queue_db_fu10_recovered/completed_jobs/",
    db_completed_file = "/home/hashjamm/results/disease_network/hr_job_queue_db_fu10_recovered/completed_jobs.duckdb",
    db_system_failed_folder = "/home/hashjamm/results/disease_network/hr_job_queue_db_fu10_recovered/system_failed_jobs/",
    db_system_failed_file = "/home/hashjamm/results/disease_network/hr_job_queue_db_fu10_recovered/system_failed_jobs.duckdb",
    db_stat_failed_folder = "/home/hashjamm/results/disease_network/hr_job_queue_db_fu10_recovered/stat_failed_jobs/",
    db_stat_failed_file = "/home/hashjamm/results/disease_network/hr_job_queue_db_fu10_recovered/stat_failed_jobs.duckdb"
)

# 메인 실행 함수
main <- function(
    paths = paths,
    fu,
    n_cores = 15,
    batch_size = 500,
    chunks_per_core = 3,
    warming_up = FALSE
) {
    total_start_time <- Sys.time()
    
    # OS 디스크 캐시 예열
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
    pre_aggregate_logs(
        chunk_folder = paths$db_stat_failed_folder,
        central_db_path = paths$db_stat_failed_file,
        pattern = "stat_failed_chunk_.*\\.duckdb",
        table_name = "stat_failures",
        create_sql = "CREATE TABLE IF NOT EXISTS stat_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME, PRIMARY KEY (cause_abb, outcome_abb, fu, error_msg))"
    )
    
    # --- 단계 2: v5에서 매핑 데이터 복구 (한 번만 실행) ---
    missing_combinations_file <- file.path(paths$output_folder, sprintf("missing_combinations_%d.parquet", fu))
    
    if (!file.exists(missing_combinations_file)) {
        cat("\n--- [단계 2] v5에서 매핑 데이터 복구 시작 ---\n")
        recover_mapping_data(
            hr_results_file = paths$hr_results_file,
            v5_chunk_folder = paths$v5_chunk_folder,
            output_folder = paths$output_folder,
            fu = fu
        )
    } else {
        cat("\n--- [단계 2] 누락 조합 파일이 이미 존재합니다. 복구 단계를 건너뜁니다. ---\n")
    }
    
    # --- 단계 3: 누락 조합에 대한 mapping 생성 ---
    if (file.exists(missing_combinations_file)) {
        result <- generate_mapping_for_missing_combinations(
            missing_combinations_file = missing_combinations_file,
            matched_parquet_folder_path = paths$matched_parquet_folder,
            outcome_parquet_file_path = paths$outcome_parquet_file,
            results_mapping_folder_path = paths$results_mapping_folder,
            db_completed_file_path = paths$db_completed_file,
            db_completed_folder_path = paths$db_completed_folder,
            db_system_failed_folder_path = paths$db_system_failed_folder,
            db_stat_failed_folder_path = paths$db_stat_failed_folder,
            fu = fu,
            n_cores = n_cores,
            batch_size = batch_size,
            chunks_per_core = chunks_per_core
        )
    } else {
        cat("\n누락 조합 파일이 없어 mapping 생성을 건너뜁니다.\n")
        result <- 0
    }
    
    # --- 단계 4: 종료 시 '로그' 취합 (다음 사이클 준비) ---
    cat("\n--- [단계 4] 이번 사이클 로그 취합 시작 ---\n")
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
    pre_aggregate_logs(
        chunk_folder = paths$db_stat_failed_folder,
        central_db_path = paths$db_stat_failed_file,
        pattern = "stat_failed_chunk_.*\\.duckdb",
        table_name = "stat_failures",
        create_sql = "CREATE TABLE IF NOT EXISTS stat_failures (cause_abb VARCHAR, outcome_abb VARCHAR, fu INTEGER, error_msg VARCHAR, timestamp DATETIME, PRIMARY KEY (cause_abb, outcome_abb, fu, error_msg))"
    )
    cat("--- [단계 4] 이번 사이클 로그 취합 완료 ---\n")
    
    # --- 작업 완료 여부 확인 ---
    if (is.null(result) || result == 0) {
        cat("\n--- 모든 작업이 완료되었습니다! ---\n")
        total_elapsed <- as.numeric(difftime(Sys.time(), total_start_time, units = "hours"))
        cat(sprintf("모든 작업 완료! 총 소요 시간: %.2f시간 (%.1f일)\n", total_elapsed, total_elapsed / 24))
        cat(sprintf("매핑 결과물 위치: %s\n", paths$results_mapping_folder))
        cat(sprintf("성공한 작업 로그 (중앙): %s\n", paths$db_completed_file))
        cat(sprintf("시스템 실패 로그 (중앙): %s\n", paths$db_system_failed_file))
        cat(sprintf("통계 실패 로그 (중앙): %s\n", paths$db_stat_failed_file))
    } else {
        cat("\n==========================================================\n")
        cat("--- 메모리 파편화 방지를 위한 프로세스 종료 ---\n")
        cat("==========================================================\n")
        cat(sprintf("남은 작업: %d개\n", result))
        cat("(크래시 또는 메모리 문제로 중단된 경우, sh 스크립트가 자동으로 재시작하여 이어서 진행)\n\n")
        cat("체크포인트 정보:\n")
        cat(sprintf("  - 완료된 작업은 중앙 DB에 기록됨\n"))
        cat(sprintf("  - 남은 작업은 다음 실행 시 자동으로 이어서 진행됨\n"))
        cat("==========================================================\n")
        flush.console()
        
        return(invisible(result))
    }
}

# --- 메인 실행 ---
result <- main(
    paths = paths,
    fu = 10,
    n_cores = 45,
    batch_size = 100000,
    chunks_per_core = 8,
    warming_up = FALSE
)

# --- 종료 코드 설정 (셸 스크립트용) ---
if (!is.null(result) && result > 0) {
    quit(save = "no", status = 1)  # 재시작 필요
} else {
    quit(save = "no", status = 0)  # 모든 작업 완료
}
