# ============================================================================
# 결과물 취합 스크립트 (Standalone)
# ============================================================================
# 목적: hr_calculator_engine_v3.R에서 생성된 청크 파일들을 취합하여 최종 파일로 저장
# 
# 취합 대상:
#   1. HR 결과 취합: hr_chunk_*.duckdb → total_hr_results_{fu}.parquet
#   2. Node 매핑 데이터 생성: matched_*.parquet → node_*_mapping_{fu}.parquet
#   3. Edge 매핑 데이터 취합: map_chunk_*.duckdb → edge_*_mapping_{fu}.parquet
# 
# [실행 방법]
#   1. 직접 실행 (기본값 사용):
#      Rscript aggregate_results.R
#      → fu=10, 기본 경로 사용
#   
#   2. 파라미터 지정:
#      Rscript aggregate_results.R --fu=9 --hr_folder=/path/to/hr --mapping_folder=/path/to/mapping --matched_folder=/path/to/matched
#      → fu와 경로를 명시적으로 지정
#   
#   3. sh 스크립트로 실행 (자동 재시작 기능 포함):
#      ./aggregate_results.sh
#      → 실패 시 자동 재시도, 로그 기록 등 추가 기능
# 
# [실행 디자인]
# - Standalone 스크립트: hr_calculator_engine_v3.R과 독립적으로 실행 가능
# - 점진적 실행: hr_calculator_engine_v3.R이 실행 중이거나 완료 후 언제든 실행 가능
# - 이어쓰기 지원: HR 결과는 기존 parquet 파일과 병합 (누적 저장)
# - 덮어쓰기 방식: Node/Edge 매핑은 매번 전체 재생성
# - 자동 경로 감지: fu 값에 따라 기본 경로 자동 설정
# - 자동 질병 코드 감지: matched 폴더에서 질병 코드 자동 추출
# ============================================================================

library(arrow)
library(dplyr)
library(duckdb)
library(DBI)

# ============================================================================
# 헬퍼 함수
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

# ============================================================================
# HR 결과 취합 함수
# ============================================================================
# 
# [처리 방식]
# R 메모리 기반 순차 처리 방식 사용
# - 각 청크 파일을 순차적으로 ATTACH → 읽기 → DETACH
# - R 리스트에 데이터 누적 후 bind_rows()로 합치기
# - 기존 parquet 파일과 병합 (이어쓰기)
# 
# [SQL 기반 방식으로 변경하지 않는 이유]
# 1. 데이터 크기: HR 결과는 질병 쌍당 1행 (17개 컬럼)으로 상대적으로 작음
#    - 각 청크당 수백~수천 행 수준
#    - Edge 매핑 대비 메모리 사용량이 매우 작음
# 
# 2. 이어쓰기 요구사항: 기존 parquet 파일과 병합 필요 (누적 저장)
#    - SQL 기반으로 변경 시 이어쓰기 로직이 복잡해짐
#    - 현재 방식이 이어쓰기에 더 적합
# 
# 3. 안정성: 현재 방식으로도 충분히 안정적
#    - 구조화된 테이블이라 bind_rows()가 효율적
#    - 순차 처리로 메모리 사용량이 예측 가능
#    - OOM 발생 가능성이 낮음
# 
# 4. 구현 복잡도: 현재 방식이 더 단순하고 유지보수 용이
#    - SQL 기반으로 변경 시 이어쓰기 로직 추가 필요
#    - 기존 parquet 파일과 SQL 결과 병합 로직 필요
# 
# [변경을 고려할 시점]
# - 청크 파일 수가 1000개 이상이고 순차 처리 시간이 과도하게 길 때
# - 실제 OOM 발생 시
# - 코드 일관성을 우선시할 때 (Edge 매핑과 동일한 방식)
# 
# [데이터 소스]
# - hr_chunk_*.duckdb: engine_v3에서 생성된 청크 파일들
# - 각 청크 파일은 hr_results 테이블을 포함
# 
# [출력]
# - total_hr_results_{fu}.parquet: 모든 청크를 합친 최종 HR 결과
# ============================================================================

aggregate_hr_results <- function(
    results_hr_folder_path,
    fu = 10
) {
    cat("\n==========================================================\n")
    cat("HR 결과 취합 시작\n")
    cat("==========================================================\n")
    cat(sprintf("대상 폴더: %s\n", results_hr_folder_path))
    cat(sprintf("Follow-up: %d\n", fu))
    cat("==========================================================\n")
    
    hr_chunk_files <- list.files(results_hr_folder_path, pattern="hr_chunk_.*\\.duckdb", full.names=TRUE)
    
    if (length(hr_chunk_files) == 0) {
        cat("   - 취합할 HR 청크 파일 없음.\n")
        return(invisible(TRUE))
    }
    
    cat(sprintf("\n발견된 HR 청크 파일 수: %d개\n", length(hr_chunk_files)))
    
    con_agg <- dbConnect(duckdb::duckdb())
    on.exit(dbDisconnect(con_agg, shutdown=TRUE), add=TRUE)
    
    # 각 DuckDB 파일을 ATTACH하여 읽기
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
            cat(sprintf("경고: HR 청크 파일 읽기 실패 %s: %s\n", basename(hr_chunk_files[i]), e$message))
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
    
    cat("==========================================================\n")
    return(invisible(TRUE))
}

# ============================================================================
# Node 매핑 데이터 생성 함수
# ============================================================================
# 
# [처리 방식]
# R 메모리 기반 순차 처리 방식 사용
# - 각 matched_*.parquet 파일을 순차적으로 읽기
# - R 리스트에 데이터 저장 후 Parquet 파일로 저장
# 
# [데이터 소스]
# - matched_*.parquet: engine_v3가 생성하지 않음, 기존에 존재하는 파일들
# - 각 파일에서 person_id, index_key_seq 변수 추출
# 
# [작업 성격]
# - 조회 및 변환 작업: 기존 파일에서 특정 변수들을 추출하여 리스트로 구성
# - HR/Edge 취합과는 다른 성격: 청크 파일을 합치는 것이 아니라 기존 파일에서 변수 추출
# 
# [출력]
# - node_pids_mapping_{fu}.parquet: 질병별 person_id 리스트
# - node_index_key_seq_mapping_{fu}.parquet: 질병별 index_key_seq 리스트 (case=1만)
# ============================================================================

generate_node_mappings <- function(
    cause_list,
    fu,
    matched_parquet_folder_path,
    results_mapping_folder_path
) {
    cat("\n==========================================================\n")
    cat("Node 매핑 데이터 생성 시작\n")
    cat("==========================================================\n")
    cat(sprintf("Matched 폴더: %s\n", matched_parquet_folder_path))
    cat(sprintf("출력 폴더: %s\n", results_mapping_folder_path))
    cat(sprintf("Follow-up: %d\n", fu))
    cat("==========================================================\n")
    
    # [설계 원칙] 매핑 자료는 덮어쓰기 방식 (이어쓰기 아님)
    # - HR 결과: 작업이 점진적으로 완료되므로 이어쓰기 필요
    # - 매핑 자료 (Node/Edge): 원본/청크 파일에 항상 전체 데이터가 존재하므로
    #   매번 전체를 재생성하는 덮어쓰기 방식이 적절함
    # - Node: matched_*.parquet에서 직접 읽어서 전체 재생성
    
    node_pids_list <- list()
    node_index_key_seq_list <- list()
    
    for (cause_abb in cause_list) {
        key <- paste(cause_abb, fu, sep = "_")
        matched_path <- file.path(matched_parquet_folder_path, sprintf("matched_%s.parquet", tolower(cause_abb)))
        if (file.exists(matched_path)) {
            tryCatch({
                matched_data <- arrow::read_parquet(matched_path, col_select = c("person_id", "index_key_seq", "case"))
                node_pids_list[[key]] <- matched_data$person_id
                node_index_key_seq_list[[key]] <- matched_data$index_key_seq[matched_data$case == 1]
            }, error = function(e) {
                cat(sprintf("경고: matched 파일 읽기 실패 %s: %s\n", basename(matched_path), e$message))
            })
        }
    }
    
    save_mapping_to_parquet(node_pids_list, "node_pids", results_mapping_folder_path, fu)
    save_mapping_to_parquet(node_index_key_seq_list, "node_index_key_seq", results_mapping_folder_path, fu)
    rm(node_pids_list, node_index_key_seq_list); gc()
    
    cat("==========================================================\n")
    return(invisible(TRUE))
}

# ============================================================================
# Edge 매핑 데이터 취합 함수
# ============================================================================
# 
# [처리 방식]
# DuckDB SQL 기반 일괄 처리 방식 사용
# - 모든 청크 파일을 한 번에 ATTACH
# - SQL UNION ALL 쿼리로 모든 데이터를 합침
# - SQL GROUP BY와 LIST(DISTINCT ...) 함수로 key별 집계
# - 결과를 Parquet 파일로 저장
# 
# [SQL 기반 방식을 사용하는 이유]
# 1. 데이터 크기: Edge 매핑은 key별로 수백~수천 개의 값이 리스트로 저장됨
#    - 각 청크의 데이터량이 매우 큼
#    - R 메모리 기반 방식으로는 OOM 발생 가능성 높음
# 
# 2. 메모리 효율성: DuckDB의 디스크 기반 처리 활용
#    - SQL 엔진이 최적화된 집계 수행
#    - R 메모리에 모든 데이터를 로드하지 않음
# 
# 3. 성능: SQL의 집계 기능 활용
#    - GROUP BY와 LIST(DISTINCT ...)로 효율적인 집계
#    - 일괄 처리로 순차 처리보다 빠름
# 
# [데이터 소스]
# - map_chunk_*.duckdb: engine_v3에서 생성된 청크 파일들
# - 각 청크 파일은 edge_pids, edge_index_key_seq, edge_key_seq 테이블을 포함할 수 있음
# - 일부 청크에서는 3개 테이블이 모두 존재하지 않을 수 있음 (테이블 존재 여부 확인 필요)
# 
# [처리 단계]
# 1. 모든 청크 파일 ATTACH
# 2. 각 청크의 테이블 존재 여부 확인 (SELECT 1 FROM ... LIMIT 1)
# 3. 존재하는 테이블만 UNION ALL에 포함하여 SQL 쿼리 동적 생성
# 4. GROUP BY로 key별 집계 후 Parquet 파일로 저장
# 
# [출력]
# - edge_pids_mapping_{fu}.parquet: 질병 쌍별 person_id 리스트
# - edge_index_key_seq_mapping_{fu}.parquet: 질병 쌍별 index_key_seq 리스트
# - edge_key_seq_mapping_{fu}.parquet: 질병 쌍별 outcome_key_seq 리스트
# ============================================================================

aggregate_edge_mappings <- function(
    results_mapping_folder_path,
    fu = 10,
    chunk_batch_size = NULL  # 현재는 사용하지 않음 (SQL 기반 처리로 변경)
) {
    cat("\n==========================================================\n")
    cat("Edge 매핑 데이터 취합 시작\n")
    cat("==========================================================\n")
    cat(sprintf("대상 폴더: %s\n", results_mapping_folder_path))
    cat(sprintf("Follow-up: %d\n", fu))
    cat("==========================================================\n")
    
    # 청크 파일 목록 가져오기
    chunk_files <- list.files(results_mapping_folder_path, pattern="map_chunk_.*\\.duckdb", full.names=TRUE)
    
    if (length(chunk_files) == 0) {
        cat("경고: 청크 파일을 찾을 수 없습니다.\n")
        return(invisible(NULL))
    }
    
    cat(sprintf("\n총 %d개의 청크 파일 발견\n", length(chunk_files)))
    
    # DuckDB 연결 (SQL 기반 처리)
    con <- dbConnect(duckdb::duckdb(), ":memory:")
    on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    # 1단계: 모든 청크 파일 ATTACH
    cat("\n=== 1단계: 청크 파일 ATTACH 중 ===\n")
    successful_attach <- 0
    failed_attach <- 0
    
    for (i in seq_along(chunk_files)) {
        tryCatch({
            dbExecute(con, sprintf("ATTACH '%s' AS db%d (READ_ONLY)", chunk_files[i], i))
            successful_attach <- successful_attach + 1
            
            if (i %% 50 == 0) {
                cat(sprintf("     진행: %d/%d 청크 ATTACH 완료\n", i, length(chunk_files)))
            }
        }, error = function(e) {
            failed_attach <<- failed_attach + 1
            cat(sprintf("경고: 청크 파일 ATTACH 실패 %s: %s\n", basename(chunk_files[i]), e$message))
        })
    }
    
    cat(sprintf("ATTACH 완료: 성공 %d개, 실패 %d개\n", successful_attach, failed_attach))
    
    # 2단계: 각 청크의 테이블 존재 여부 확인 (실제 쿼리 방식)
    # [방법 선택]
    # 방법 1: dbListTables 사용 → 버그로 인해 사용 불가 (여러 스키마 ATTACH 시 잘못된 결과 반환)
    # 방법 2: 실제 쿼리로 확인 (SELECT 1 FROM ... LIMIT 1) ⭐ 최종 선택
    #   - 실제로 쿼리 가능한 테이블만 확인 → 정확하고 신뢰할 수 있음
    #   - LIMIT 1로 최소한의 데이터만 읽어 부하가 낮음
    #
    cat("\n=== 2단계: 테이블 존재 여부 확인 중 (실제 쿼리 방식) ===\n")
    table_map <- list()
    
    for (i in seq_along(chunk_files)) {
        schema_name <- sprintf("db%d", i)
        available_tables <- character(0)
        
        # 각 테이블에 대해 실제 쿼리로 존재 여부 확인
        for (table_name in c("edge_pids", "edge_index_key_seq", "edge_key_seq")) {
            tryCatch({
                test_query <- sprintf("SELECT 1 FROM %s.%s LIMIT 1", schema_name, table_name)
                dbGetQuery(con, test_query)
                # 쿼리가 성공하면 테이블이 존재함
                available_tables <- c(available_tables, table_name)
            }, error = function(e) {
                # 테이블이 없거나 접근할 수 없음 → 무시
            })
        }
        
        table_map[[i]] <- available_tables
        
        if (i %% 100 == 0) {
            cat(sprintf("     진행: %d/%d 청크 확인 완료\n", i, length(chunk_files)))
        }
    }
    
    # 각 테이블 타입별로 존재하는 청크 개수 확인
    pids_chunks <- sum(sapply(table_map, function(t) "edge_pids" %in% t))
    index_chunks <- sum(sapply(table_map, function(t) "edge_index_key_seq" %in% t))
    key_chunks <- sum(sapply(table_map, function(t) "edge_key_seq" %in% t))
    
    cat(sprintf("edge_pids 테이블 존재: %d개 청크\n", pids_chunks))
    cat(sprintf("edge_index_key_seq 테이블 존재: %d개 청크\n", index_chunks))
    cat(sprintf("edge_key_seq 테이블 존재: %d개 청크\n", key_chunks))
    
    # 3단계: SQL 쿼리 동적 생성 및 실행
    cat("\n=== 3단계: SQL 집계 쿼리 실행 중 ===\n")
    
    # edge_pids 처리
    if (pids_chunks > 0) {
        cat("   - edge_pids 집계 중...\n")
        
        # 존재하는 테이블만 UNION ALL에 포함
        union_parts_pids <- character()
        for (i in seq_along(chunk_files)) {
            if ("edge_pids" %in% table_map[[i]]) {
                union_parts_pids <- c(union_parts_pids, 
                    sprintf("SELECT key, CAST(person_id AS BIGINT) as person_id FROM db%d.edge_pids", i))
            }
        }
        
        if (length(union_parts_pids) > 0) {
            # SQL 쿼리 생성 및 실행
            query_pids <- paste(
                "SELECT key, LIST(DISTINCT person_id) as values",
                "FROM (",
                paste(union_parts_pids, collapse = " UNION ALL "),
                ")",
                "GROUP BY key"
            )
            
            result_pids <- dbGetQuery(con, query_pids)
            
            # 결과를 리스트로 변환
            edge_pids_list <- setNames(result_pids$values, result_pids$key)
            
            # Parquet 파일로 저장
            df_pids <- data.frame(
                key = names(edge_pids_list),
                stringsAsFactors = FALSE
            )
            df_pids$values <- I(edge_pids_list)
            
            parquet_file_pids <- file.path(results_mapping_folder_path, sprintf("edge_pids_mapping_%d.parquet", fu))
            arrow::write_parquet(df_pids, parquet_file_pids)
            cat(sprintf("     ✓ 완료: %s (%d개 key)\n", basename(parquet_file_pids), length(edge_pids_list)))
            rm(edge_pids_list, df_pids, result_pids); gc()
        }
    } else {
        cat("   - edge_pids 데이터 없음\n")
    }
    
    # edge_index_key_seq 처리
    if (index_chunks > 0) {
        cat("   - edge_index_key_seq 집계 중...\n")
        
        union_parts_index <- character()
        for (i in seq_along(chunk_files)) {
            if ("edge_index_key_seq" %in% table_map[[i]]) {
                union_parts_index <- c(union_parts_index, 
                    sprintf("SELECT key, CAST(index_key_seq AS BIGINT) as index_key_seq FROM db%d.edge_index_key_seq", i))
            }
        }
        
        if (length(union_parts_index) > 0) {
            query_index <- paste(
                "SELECT key, LIST(DISTINCT index_key_seq) as values",
                "FROM (",
                paste(union_parts_index, collapse = " UNION ALL "),
                ")",
                "GROUP BY key"
            )
            
            result_index <- dbGetQuery(con, query_index)
            edge_index_list <- setNames(result_index$values, result_index$key)
            
            df_index <- data.frame(
                key = names(edge_index_list),
                stringsAsFactors = FALSE
            )
            df_index$values <- I(edge_index_list)
            
            parquet_file_index <- file.path(results_mapping_folder_path, sprintf("edge_index_key_seq_mapping_%d.parquet", fu))
            arrow::write_parquet(df_index, parquet_file_index)
            cat(sprintf("     ✓ 완료: %s (%d개 key)\n", basename(parquet_file_index), length(edge_index_list)))
            rm(edge_index_list, df_index, result_index); gc()
        }
    } else {
        cat("   - edge_index_key_seq 데이터 없음\n")
    }
    
    # edge_key_seq 처리
    if (key_chunks > 0) {
        cat("   - edge_key_seq 집계 중...\n")
        
        union_parts_key <- character()
        for (i in seq_along(chunk_files)) {
            if ("edge_key_seq" %in% table_map[[i]]) {
                union_parts_key <- c(union_parts_key, 
                    sprintf("SELECT key, CAST(outcome_key_seq AS BIGINT) as outcome_key_seq FROM db%d.edge_key_seq", i))
            }
        }
        
        if (length(union_parts_key) > 0) {
            query_key <- paste(
                "SELECT key, LIST(DISTINCT outcome_key_seq) as values",
                "FROM (",
                paste(union_parts_key, collapse = " UNION ALL "),
                ")",
                "GROUP BY key"
            )
            
            result_key <- dbGetQuery(con, query_key)
            edge_key_list <- setNames(result_key$values, result_key$key)
            
            df_key <- data.frame(
                key = names(edge_key_list),
                stringsAsFactors = FALSE
            )
            df_key$values <- I(edge_key_list)
            
            parquet_file_key <- file.path(results_mapping_folder_path, sprintf("edge_key_seq_mapping_%d.parquet", fu))
            arrow::write_parquet(df_key, parquet_file_key)
            cat(sprintf("     ✓ 완료: %s (%d개 key)\n", basename(parquet_file_key), length(edge_key_list)))
            rm(edge_key_list, df_key, result_key); gc()
        }
    } else {
        cat("   - edge_key_seq 데이터 없음\n")
    }
    
    cat("\n=== Edge 매핑 데이터 취합 완료 ===\n")
    cat("==========================================================\n")
    return(invisible(TRUE))
}

# ============================================================================
# 메인 취합 함수 (모든 취합을 순차적으로 실행)
# ============================================================================
# 
# [세 가지 취합 방식 비교]
# 
# 1. HR 결과 취합 (R 메모리 기반 순차 처리)
#    - 데이터 소스: hr_chunk_*.duckdb (engine_v3 생성)
#    - 작업 성격: 청크 파일들을 합치기 (aggregation)
#    - 처리 방식: 순차 ATTACH → 읽기 → DETACH → R 메모리 누적 → bind_rows()
#    - 저장 방식: 이어쓰기 (기존 parquet 파일과 병합)
#    - 변경 필요성: 낮음 (데이터 크기가 작고 이어쓰기 요구사항)
# 
# 2. Node 매핑 생성 (R 메모리 기반 순차 처리)
#    - 데이터 소스: matched_*.parquet (기존 파일)
#    - 작업 성격: 기존 파일에서 변수 추출 및 변환
#    - 처리 방식: 순차적으로 파일 읽기 → R 리스트에 저장
#    - 저장 방식: 덮어쓰기 (매번 전체 재생성)
#    - 변경 필요성: 없음 (HR/Edge와 다른 성격의 작업)
# 
# 3. Edge 매핑 취합 (SQL 기반 일괄 처리)
#    - 데이터 소스: map_chunk_*.duckdb (engine_v3 생성)
#    - 작업 성격: 청크 파일들을 합치기 (aggregation)
#    - 처리 방식: 모든 청크 ATTACH → SQL UNION ALL → GROUP BY
#    - 저장 방식: 덮어쓰기 (매번 전체 재생성)
#    - 변경 필요성: 필수 (데이터 크기가 커서 SQL 기반 필수)
# 
# [실행 순서]
# 1. HR 결과 취합
# 2. Node 매핑 데이터 생성
# 3. Edge 매핑 데이터 취합
# ============================================================================

aggregate_all_results <- function(
    cause_list,
    fu = 10,
    matched_parquet_folder_path,
    results_hr_folder_path,
    results_mapping_folder_path,
    chunk_batch_size = NULL  # 현재는 사용하지 않음 (SQL 기반 처리로 변경되어 배치 처리 불필요)
) {
    cat("\n==========================================================\n")
    cat("결과물 취합 시작\n")
    cat("==========================================================\n")
    cat(sprintf("Follow-up: %d\n", fu))
    cat(sprintf("HR 결과 폴더: %s\n", results_hr_folder_path))
    cat(sprintf("매핑 결과 폴더: %s\n", results_mapping_folder_path))
    cat(sprintf("Matched 폴더: %s\n", matched_parquet_folder_path))
    cat("==========================================================\n\n")
    
    # 1. HR 결과 취합
    aggregate_hr_results(
        results_hr_folder_path = results_hr_folder_path,
        fu = fu
    )
    
    # 2. Node 매핑 데이터 생성
    generate_node_mappings(
        cause_list = cause_list,
        fu = fu,
        matched_parquet_folder_path = matched_parquet_folder_path,
        results_mapping_folder_path = results_mapping_folder_path
    )
    
    # 3. Edge 매핑 데이터 취합
    aggregate_edge_mappings(
        results_mapping_folder_path = results_mapping_folder_path,
        fu = fu,
        chunk_batch_size = chunk_batch_size
    )
    
    cat("\n==========================================================\n")
    cat("모든 결과물 취합 완료\n")
    cat("==========================================================\n")
    
    return(invisible(TRUE))
}

# ============================================================================
# 스크립트 실행 부분
# ============================================================================

# 명령줄 인자 파싱 (간단한 버전)
args <- commandArgs(trailingOnly = TRUE)
fu <- 10
results_hr_folder <- NULL
results_mapping_folder <- NULL
matched_parquet_folder <- NULL
chunk_batch_size <- NULL
cause_list <- NULL

for (arg in args) {
    if (grepl("^--fu=", arg)) {
        fu <- as.integer(sub("^--fu=", "", arg))
    } else if (grepl("^--hr_folder=", arg)) {
        results_hr_folder <- sub("^--hr_folder=", "", arg)
    } else if (grepl("^--mapping_folder=", arg)) {
        results_mapping_folder <- sub("^--mapping_folder=", "", arg)
    } else if (grepl("^--matched_folder=", arg)) {
        matched_parquet_folder <- sub("^--matched_folder=", "", arg)
    } else if (grepl("^--chunk_batch_size=", arg)) {
        chunk_batch_size <- as.integer(sub("^--chunk_batch_size=", "", arg))
    }
}

# 기본값 설정 (fu 기반)
if (is.null(results_hr_folder)) {
    results_hr_folder <- sprintf("/home/hashjamm/results/disease_network/hr_results_fu%d/", fu)
}
if (is.null(results_mapping_folder)) {
    results_mapping_folder <- sprintf("/home/hashjamm/results/disease_network/hr_mapping_results_fu%d/", fu)
}
if (is.null(matched_parquet_folder)) {
    matched_parquet_folder <- "/home/hashjamm/project_data/disease_network/matched_date_parquet/"
}

# cause_list 자동 감지 (matched 폴더에서)
if (is.null(cause_list)) {
    matched_files <- list.files(matched_parquet_folder, pattern="matched_.*\\.parquet", full.names=FALSE)
    cause_list <- toupper(gsub("matched_(.*)\\.parquet", "\\1", matched_files))
    cause_list <- sort(cause_list)
}

# 실행
if (length(cause_list) == 0) {
    cat("경고: 질병 코드를 찾을 수 없습니다.\n")
    quit(save = "no", status = 1)
}

aggregate_all_results(
    cause_list = cause_list,
    fu = fu,
    matched_parquet_folder_path = matched_parquet_folder,
    results_hr_folder_path = results_hr_folder,
    results_mapping_folder_path = results_mapping_folder,
    chunk_batch_size = chunk_batch_size
)

