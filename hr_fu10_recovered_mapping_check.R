library(duckdb)
library(dplyr)
library(arrow)

# ============================================================================
# 함수 1: v10_recovered의 유니크한 조합을 벡터로 리턴하고 개수 출력
# ============================================================================
# 주석: recover 하는 과정에서 stat_failed 및 system_failed는 발생하지 않았음
get_unique_combinations_v10_recovered <- function(
    db_path = "/home/hashjamm/results/disease_network/hr_job_queue_db_v10_recovered/completed_jobs.duckdb"
) {
    
    # DuckDB 연결
    con <- dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)
    on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)
    
    # 유니크한 조합 조회
    unique_combinations <- dbGetQuery(
        con, 
        "SELECT DISTINCT cause_abb, outcome_abb, fu FROM jobs"
    )
    
    # 조합을 문자열 벡터로 변환 (cause_abb_outcome_abb_fu 형식)
    combination_vector <- paste(
        unique_combinations$cause_abb,
        unique_combinations$outcome_abb,
        unique_combinations$fu,
        sep = "_"
    )
    
    # 개수 출력
    cat(sprintf("v10_recovered 유니크한 조합 개수: %d개\n", length(combination_vector)))
    
    return(combination_vector)
}

# ============================================================================
# 함수 2: v10_recovered 매핑 파일들의 유니크한 조합을 벡터로 리턴하고 개수 출력
# ============================================================================
get_unique_combinations_v10_mapping_results <- function(
    mapping_dir = "/home/hashjamm/results/disease_network/hr_mapping_results_v10_recovered/"
) {
    
    # 매핑 파일 경로
    edge_index_file <- file.path(mapping_dir, "edge_index_key_seq_mapping_10.parquet")
    edge_key_file <- file.path(mapping_dir, "edge_key_seq_mapping_10.parquet")
    edge_pids_file <- file.path(mapping_dir, "edge_pids_mapping_10.parquet")
    
    all_combinations <- character()
    
    # edge_index_key_seq_mapping 파일 처리
    if (file.exists(edge_index_file)) {
        edge_index_data <- arrow::read_parquet(edge_index_file)
        edge_index_keys <- unique(edge_index_data$key)
        cat(sprintf("edge_index_key_seq_mapping_10.parquet 유니크한 조합 개수: %d개\n", length(edge_index_keys)))
        all_combinations <- c(all_combinations, edge_index_keys)
    } else {
        cat("경고: edge_index_key_seq_mapping_10.parquet 파일을 찾을 수 없습니다.\n")
    }
    
    # edge_key_seq_mapping 파일 처리
    if (file.exists(edge_key_file)) {
        edge_key_data <- arrow::read_parquet(edge_key_file)
        edge_key_keys <- unique(edge_key_data$key)
        cat(sprintf("edge_key_seq_mapping_10.parquet 유니크한 조합 개수: %d개\n", length(edge_key_keys)))
        all_combinations <- c(all_combinations, edge_key_keys)
    } else {
        cat("경고: edge_key_seq_mapping_10.parquet 파일을 찾을 수 없습니다.\n")
    }
    
    # edge_pids_mapping 파일 처리
    if (file.exists(edge_pids_file)) {
        edge_pids_data <- arrow::read_parquet(edge_pids_file)
        edge_pids_keys <- unique(edge_pids_data$key)
        cat(sprintf("edge_pids_mapping_10.parquet 유니크한 조합 개수: %d개\n", length(edge_pids_keys)))
        all_combinations <- c(all_combinations, edge_pids_keys)
    } else {
        cat("경고: edge_pids_mapping_10.parquet 파일을 찾을 수 없습니다.\n")
    }
    
    # 전체 유니크한 조합 (중복 제거)
    unique_combinations <- unique(all_combinations)
    
    # 전체 개수 출력
    cat(sprintf("\n기존에 불러올 수 있던 매핑 파일 전체 유니크한 조합 개수: %d개\n", length(unique_combinations)))
    
    return(unique_combinations)
}

# ============================================================================
# 함수 3: v10 HR 결과 파일의 유니크한 조합을 벡터로 리턴하고 개수 출력
# ============================================================================
get_unique_combinations_v10_hr_results <- function(
    hr_results_path = "/home/hashjamm/results/disease_network/hr_results_v10/total_hr_results_10.parquet"
) {
    
    # Parquet 파일 읽기
    hr_data <- arrow::read_parquet(hr_results_path)
    
    # 유니크한 조합 조회
    unique_combinations <- hr_data %>%
        distinct(cause_abb, outcome_abb, fu) %>%
        collect()
    
    # 조합을 문자열 벡터로 변환 (cause_abb_outcome_abb_fu 형식)
    combination_vector <- paste(
        unique_combinations$cause_abb,
        unique_combinations$outcome_abb,
        unique_combinations$fu,
        sep = "_"
    )
    
    # 개수 출력
    cat(sprintf("v10_hr_results 유니크한 조합 개수: %d개\n", length(combination_vector)))
    
    return(combination_vector)
}

# ============================================================================
# 함수 4: 세 벡터의 교집합, 합집합 분석
# ============================================================================
compare_combinations <- function(vec1, vec2, vec3, name1 = "벡터1", name2 = "벡터2", name3 = "벡터3") {
    # 쌍별 교집합
    intersection_12 <- intersect(vec1, vec2)
    intersection_13 <- intersect(vec1, vec3)
    intersection_23 <- intersect(vec2, vec3)
    
    # 세 벡터 모두의 교집합
    intersection_all <- intersect(intersection_12, vec3)
    intersection_all_exists <- length(intersection_all) > 0
    
    # 전체 합집합
    union_set <- union(union(vec1, vec2), vec3)
    union_count <- length(union_set)
    
    # 각 벡터에만 있는 것들 (차집합)
    only_vec1 <- setdiff(setdiff(vec1, vec2), vec3)
    only_vec2 <- setdiff(setdiff(vec2, vec1), vec3)
    only_vec3 <- setdiff(setdiff(vec3, vec1), vec2)
    
    # 결과 출력
    cat("\n=== 조합 비교 결과 ===\n")
    cat(sprintf("%s 개수: %d개\n", name1, length(vec1)))
    cat(sprintf("%s 개수: %d개\n", name2, length(vec2)))
    cat(sprintf("%s 개수: %d개\n", name3, length(vec3)))
    cat(sprintf("\n--- 쌍별 교집합 ---\n"))
    cat(sprintf("%s & %s: %d개\n", name1, name2, length(intersection_12)))
    cat(sprintf("%s & %s: %d개\n", name1, name3, length(intersection_13)))
    cat(sprintf("%s & %s: %d개\n", name2, name3, length(intersection_23)))
    cat(sprintf("\n--- 세 벡터 모두의 교집합 ---\n"))
    cat(sprintf("교집합 존재 여부: %s\n", ifelse(intersection_all_exists, "예", "아니오")))
    cat(sprintf("교집합 개수: %d개\n", length(intersection_all)))
    cat(sprintf("\n--- 전체 합집합 ---\n"))
    cat(sprintf("합집합 개수: %d개\n", union_count))
    cat(sprintf("\n--- 각 벡터에만 있는 조합 ---\n"))
    cat(sprintf("%s에만 있는 조합: %d개\n", name1, length(only_vec1)))
    cat(sprintf("%s에만 있는 조합: %d개\n", name2, length(only_vec2)))
    cat(sprintf("%s에만 있는 조합: %d개\n", name3, length(only_vec3)))
    
    # 결과를 리스트로 반환
    return(list(
        intersection_12 = intersection_12,
        intersection_13 = intersection_13,
        intersection_23 = intersection_23,
        intersection_all = intersection_all,
        intersection_all_exists = intersection_all_exists,
        intersection_all_count = length(intersection_all),
        union = union_set,
        union_count = union_count,
        only_vec1 = only_vec1,
        only_vec2 = only_vec2,
        only_vec3 = only_vec3,
        vec1_count = length(vec1),
        vec2_count = length(vec2),
        vec3_count = length(vec3)
    ))
}

v10_recovered_combinations <- get_unique_combinations_v10_recovered()
v10_mapping_results_combinations <- get_unique_combinations_v10_mapping_results()
v10_hr_results_combinations <- get_unique_combinations_v10_hr_results()

# v10_recovered 유니크한 조합 개수: 175397개
# edge_index_key_seq_mapping_10.parquet 유니크한 조합 개수: 770257개
# edge_key_seq_mapping_10.parquet 유니크한 조합 개수: 530925개
# edge_pids_mapping_10.parquet 유니크한 조합 개수: 770257개
# 기존에 불러올 수 있던 매핑 파일 전체 유니크한 조합 개수: 770257개

# v10_hr_results 유니크한 조합 개수: 945654개

comparison_result <- compare_combinations(
    v10_recovered_combinations, 
    v10_mapping_results_combinations,
    v10_hr_results_combinations,
    name1 = "v10_recovered_combinations",
    name2 = "v10_mapping_results_combinations",
    name3 = "v10_hr_results_combinations"
)

hr_results_v10 <- arrow::read_parquet("/home/hashjamm/results/disease_network/hr_results_v10/total_hr_results_10.parquet")

# hr_values의 최소값과 최대값 출력
hr_values_protect <- hr_results_v10 %>% filter(hr_values < 1)
hr_values_protect

# ============================================================================
# 함수 5: Edge Mapping 청크 파일 집계 함수
# 
# 목적: v10_mapping_recovered_chunks 폴더의 DuckDB 청크 파일들을 
#       key별로 그룹화하여 단일 Parquet 파일로 집계
# 
# [집계 방법 선택 과정]
# 
# 옵션 1: Incremental Processing (R memory based)
#   - 방법: 각 청크 파일을 순차적으로 읽어서 R 메모리에 누적
#   - 장점: 구현이 간단함
#   - 단점: 
#     * 메모리 사용량이 매우 큼 (OOM 발생 가능)
#     * 집계된 결과도 메모리에 올라가므로 메모리 부족 위험
#   - 결론: ❌ 사용 불가 (초기 구현 시도 → OOM으로 Killed 발생)
#
# 옵션 2: Batch Processing
#   - 방법: 청크 파일들을 배치로 나누어 처리
#   - 장점: 메모리 사용량을 일부 제어 가능
#   - 단점: 여전히 메모리 사용량이 큼, 배치 크기 조정이 복잡함
#   - 결론: ❌ 사용하지 않음
#
# 옵션 3: DuckDB SQL based processing ⭐ 최종 선택
#   - 방법: 
#     1. 모든 청크 파일을 ATTACH하여 하나의 DuckDB 연결에 연결
#     2. SQL UNION ALL 쿼리로 모든 데이터를 합침
#     3. SQL GROUP BY와 LIST() 함수로 key별 집계
#     4. 결과를 Parquet 파일로 저장
#   - 장점:
#     * DuckDB의 디스크 기반 처리 활용 → 메모리 효율적
#     * SQL의 집계 기능 활용 → 빠르고 효율적
#     * 집계된 결과도 메모리 사용량이 적음
#   - 단점: 일부 청크에 테이블이 없을 수 있음 → 사전 확인 필요
#   - 결론: ✅ 사용 (메모리 효율성과 성능이 가장 우수)
#
# [중요 사항]
# - 원본 청크 파일은 절대 삭제하면 안 됨!
# - 모든 작업은 READ_ONLY 모드로 수행
# ============================================================================
aggregate_edge_mapping_chunks <- function(
    chunk_folder = "/home/hashjamm/results/disease_network/hr_mapping_results_v10_recovered/v10_mapping_recovered_chunks/",
    output_folder = "/home/hashjamm/results/disease_network/hr_mapping_results_v10_recovered/",
    fu = 10
) {
    
    cat("\n=== Edge Mapping 청크 파일 Aggregate 시작 ===\n")
    cat(sprintf("청크 폴더: %s\n", chunk_folder))
    cat(sprintf("출력 폴더: %s\n", output_folder))
    cat(sprintf("FU: %d\n", fu))
    
    # 청크 파일 목록 가져오기
    chunk_files <- list.files(chunk_folder, pattern = "\\.duckdb$", full.names = TRUE)
    if (length(chunk_files) == 0) {
        cat("경고: 청크 파일을 찾을 수 없습니다.\n")
        return(NULL)
    }
    
    cat(sprintf("\n총 %d개의 청크 파일 발견\n", length(chunk_files)))
    
    # DuckDB 연결 (옵션 3: SQL 기반 처리)
    # → 메모리 효율적인 디스크 기반 집계를 위해 DuckDB의 SQL 기능 활용
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
    
    # 2단계: 각 청크의 테이블 존재 여부 확인
    # 
    # [테이블 존재 여부 확인 방법 선택 과정]
    # 
    # 방법 1: dbListTables 사용
    #   - 장점: 매우 빠름 (메타데이터만 조회)
    #   - 단점: 여러 데이터베이스를 ATTACH한 상태에서 스키마 파라미터가 제대로 작동하지 않음
    #           → 특정 스키마(db208 등)의 테이블을 조회할 때 다른 스키마의 테이블도 함께 반환됨
    #           → 중복된 테이블 이름이 수백 개 반환되는 버그 발생
    #           → 검증 결과: 576개 청크 중 4개에서 dbListTables가 잘못된 결과 반환
    #   - 결론: ❌ 사용 불가 (버그로 인해 신뢰할 수 없음)
    #
    # 방법 2: 실제 쿼리로 확인 (SELECT 1 FROM ... LIMIT 1) ⭐ 최종 선택
    #   - 장점: 
    #     * 실제로 쿼리 가능한 테이블만 확인 → 정확하고 신뢰할 수 있음
    #     * dbListTables 버그에 영향받지 않음
    #     * LIMIT 1로 최소한의 데이터만 읽어 부하가 낮음
    #   - 단점: 
    #     * 각 테이블마다 쿼리를 실행해야 하므로 방법 1보다 약간 느림
    #     * 예상 부하: 576개 청크 × 최대 3개 테이블 = 최대 1,728번 쿼리 실행
    #     * 예상 시간: 약 5-15초 (전체 집계 시간 대비 무시할 수 있는 수준)
    #   - 결론: ✅ 사용 (정확성과 안정성이 속도보다 중요)
    #
    # 방법 3: 하이브리드 (dbListTables + tryCatch 검증)
    #   - 장점: 빠르면서도 정확함
    #   - 단점: dbListTables 버그로 인해 실용적이지 않음
    #   - 결론: ❌ 사용 불가
    #
    # 방법 4: information_schema 쿼리
    #   - 장점: 표준 SQL 방식, 빠름
    #   - 단점: DuckDB에서 지원되지 않음
    #   - 결론: ❌ 사용 불가
    #
    # [최종 결정]
    # 방법 2를 선택: 실제 쿼리(SELECT 1 FROM schema.table LIMIT 1)로 테이블 존재 여부 확인
    # → 정확성과 안정성을 위해 약간의 성능 저하를 감수
    
    cat("\n=== 2단계: 테이블 존재 여부 확인 중 (실제 쿼리 방식) ===\n")
    table_map <- list()  # 각 청크의 테이블 목록 저장
    
    for (i in seq_along(chunk_files)) {
        schema_name <- sprintf("db%d", i)
        available_tables <- character(0)
        
        # 각 테이블에 대해 실제 쿼리로 존재 여부 확인
        # LIMIT 1을 사용하여 최소한의 데이터만 읽어 부하 최소화
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
            
            parquet_file_pids <- file.path(output_folder, sprintf("recovered_edge_pids_mapping_%d.parquet", fu))
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
            
            parquet_file_index <- file.path(output_folder, sprintf("recovered_edge_index_key_seq_mapping_%d.parquet", fu))
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
            
            parquet_file_key <- file.path(output_folder, sprintf("recovered_edge_key_seq_mapping_%d.parquet", fu))
            arrow::write_parquet(df_key, parquet_file_key)
            cat(sprintf("     ✓ 완료: %s (%d개 key)\n", basename(parquet_file_key), length(edge_key_list)))
            rm(edge_key_list, df_key, result_key); gc()
        }
    } else {
        cat("   - edge_key_seq 데이터 없음\n")
    }
    
    cat("\n=== Aggregate 완료 ===\n")
    return(invisible(NULL))
}

aggregate_edge_mapping_chunks(fu = 10)
# edge_pids 테이블 존재: 572개 청크
# edge_index_key_seq 테이블 존재: 572개 청크
# edge_key_seq 테이블 존재: 572개 청크

# === 3단계: SQL 집계 쿼리 실행 중 ===
#    - edge_pids 집계 중...
#      ✓ 완료: recovered_edge_pids_mapping_10.parquet (175397개 key)
#    - edge_index_key_seq 집계 중...
#      ✓ 완료: recovered_edge_index_key_seq_mapping_10.parquet (175397개 key)
#    - edge_key_seq 집계 중...
#      ✓ 완료: recovered_edge_key_seq_mapping_10.parquet (147027개 key)

# edge_index_key_seq_mapping_10.parquet 유니크한 조합 개수(770257개) + 
# recovered_edge_index_key_seq_mapping_10.parquet (175397개) = 945654개

# edge_key_seq_mapping_10.parquet 유니크한 조합 개수: 530925개 +
# recovered_edge_key_seq_mapping_10.parquet (147027개) = 677952개

# edge_pids_mapping_10.parquet 유니크한 조합 개수: 770257개 +
# recovered_edge_pids_mapping_10.parquet (175397개) = 945654개

# v10_hr_results 유니크한 조합 개수: 945654개

# ============================================================================
# 함수 6: 기존 매핑 파일과 recovered 매핑 파일을 병합하여 repaired 파일로 저장
# ============================================================================
merge_mapping_files <- function(
    mapping_dir = "/home/hashjamm/results/disease_network/hr_mapping_results_v10_recovered/",
    fu = 10
) {
    cat("\n=== 매핑 파일 병합 시작 ===\n")
    cat(sprintf("매핑 디렉토리: %s\n", mapping_dir))
    cat(sprintf("FU: %d\n", fu))
    
    # 파일 경로 정의
    base_names <- c("edge_pids", "edge_index_key_seq", "edge_key_seq")
    
    for (base_name in base_names) {
        cat(sprintf("\n--- %s 처리 중 ---\n", base_name))
        
        # 기존 파일 경로
        original_file <- file.path(mapping_dir, sprintf("%s_mapping_%d.parquet", base_name, fu))
        # recovered 파일 경로
        recovered_file <- file.path(mapping_dir, sprintf("recovered_%s_mapping_%d.parquet", base_name, fu))
        # 출력 파일 경로
        repaired_file <- file.path(mapping_dir, sprintf("repaired_%s_mapping_%d.parquet", base_name, fu))
        
        # 기존 파일 읽기
        original_data <- NULL
        if (file.exists(original_file)) {
            original_data <- arrow::read_parquet(original_file)
            cat(sprintf("  기존 파일 로드: %s (%d개 key)\n", basename(original_file), nrow(original_data)))
        } else {
            cat(sprintf("  경고: 기존 파일 없음: %s\n", basename(original_file)))
        }
        
        # recovered 파일 읽기
        recovered_data <- NULL
        if (file.exists(recovered_file)) {
            recovered_data <- arrow::read_parquet(recovered_file)
            cat(sprintf("  recovered 파일 로드: %s (%d개 key)\n", basename(recovered_file), nrow(recovered_data)))
        } else {
            cat(sprintf("  경고: recovered 파일 없음: %s\n", basename(recovered_file)))
        }
        
        # 두 파일 모두 없으면 스킵
        if (is.null(original_data) && is.null(recovered_data)) {
            cat(sprintf("  ⚠ 두 파일 모두 없어서 스킵\n"))
            next
        }
        
        # 병합 로직: DuckDB를 사용한 디스크 기반 병합
        # 
        # [에러 해결]
        # 원래 방식: rbind()로 두 데이터프레임을 합침
        # 문제: Arrow의 리스트 배열 제한 에러 발생
        #   "Error: Capacity error: List array cannot contain more than 2147483646 elements"
        #
        # [Arrow의 리스트 배열 구조와 제한]
        # Arrow는 리스트 컬럼을 저장할 때 두 가지 배열을 사용:
        # 1. Value 배열: 모든 리스트의 값들을 평탄화해서 저장
        #    예: [1,2,3], [4,5], [6] → Value 배열: [1,2,3,4,5,6]
        # 2. Offset 배열: 각 리스트의 시작/끝 위치를 저장 (32비트 정수 인덱스)
        #    예: [0, 3, 5, 6] (첫 번째 리스트는 0~2, 두 번째는 3~4, ...)
        #
        # 제한: Offset 배열이 32비트 정수를 사용하므로 최대값은 2^31 - 2 = 2147483646
        #      이것은 각 리스트의 크기 제한이 아니라, 전체 리스트 배열의 총 요소 개수 제한
        #
        # [데이터 구조 차이]
        # - DuckDB 청크 파일: key 하나에 대해 여러 value들이 각각 row로 나뉘어져 있음
        #   예: (key="A_B_10", person_id=1001), (key="A_B_10", person_id=1002), ...
        #   → rbind() 가능, 리스트 배열 제한 없음
        #
        # - Parquet 파일: key 하나에 값들을 모두 리스트 형태로 가지고 있음
        #   예: (key="A_B_10", values=[1001, 1002, 1003, ...])
        #   → rbind() 시 Arrow가 내부적으로 리스트 배열을 재구성하면서 제한에 걸림
        #
        # [에러 발생 시나리오]
        # 예: edge_pids_mapping_10.parquet (770257개 key, 평균 각 key당 1000개 person_id)
        #   → 약 7.7억 개 요소
        #   + recovered_edge_pids_mapping_10.parquet (175397개 key, 평균 각 key당 1000개 person_id)
        #   → 약 1.7억 개 요소
        #   = 총 약 9.4억 개 요소
        #
        # rbind()로 합칠 때:
        # 1. 각 파일의 Value 배열을 메모리로 읽음
        # 2. Arrow가 내부적으로 리스트 배열을 재구성
        # 3. 이 과정에서 총 요소 개수가 제한을 넘거나, 내부 구조 재구성 중 문제 발생
        #
        # [해결 방법]
        # DuckDB의 UNION ALL을 사용하여 디스크 기반으로 병합:
        # - 각 파일을 읽을 때 Arrow의 리스트 배열을 DuckDB의 자체 구조로 변환
        # - UNION ALL로 단순히 row를 이어붙임
        # - Arrow의 리스트 배열 제한을 우회
        # - 파일을 메모리에 올리지 않고 DuckDB가 직접 읽어서 합침
        # - 메모리 효율적이고 Arrow 제한 문제 회피
        #
        if (is.null(original_data)) {
            # 기존 파일이 없으면 recovered만 사용
            merged_data <- recovered_data
            cat(sprintf("  → recovered 파일만 사용\n"))
            rm(recovered_data); gc()
        } else if (is.null(recovered_data)) {
            # recovered 파일이 없으면 기존만 사용
            merged_data <- original_data
            cat(sprintf("  → 기존 파일만 사용\n"))
            rm(original_data); gc()
        } else {
            # 두 파일 모두 있으면 구조 확인 후 DuckDB로 병합
            # 구조 확인
            if (!identical(names(original_data), names(recovered_data))) {
                cat(sprintf("  ⚠ 경고: 파일 구조가 다릅니다!\n"))
                cat(sprintf("    기존 파일 컬럼: %s\n", paste(names(original_data), collapse = ", ")))
                cat(sprintf("    recovered 파일 컬럼: %s\n", paste(names(recovered_data), collapse = ", ")))
            }
            
            # 원본 데이터 해제 (구조 확인 완료, 이제 DuckDB로 직접 읽음)
            rm(original_data, recovered_data); gc()
            
            # DuckDB를 사용하여 Parquet 파일을 직접 읽어서 병합 및 저장
            # [추가 에러 해결]
            # 문제: dbGetQuery()로 데이터를 가져온 후 arrow::write_parquet()로 저장할 때도 같은 에러 발생
            # 원인: R 데이터프레임으로 변환할 때 Arrow의 리스트 배열 제한에 걸림
            # 해결: DuckDB의 COPY ... TO를 사용하여 직접 Parquet 파일로 저장
            #   → R 메모리로 데이터를 가져오지 않고 DuckDB가 직접 저장
            #   → Arrow의 리스트 배열 제한 완전 회피
            #
            con_temp <- dbConnect(duckdb::duckdb(), ":memory:")
            
            tryCatch({
                # UNION ALL 쿼리 생성
                query <- sprintf(
                    "SELECT * FROM read_parquet('%s') UNION ALL SELECT * FROM read_parquet('%s')",
                    original_file, recovered_file
                )
                
                # DuckDB에서 직접 Parquet 파일로 저장 (Arrow 제한 회피)
                cat(sprintf("  저장 중: %s...\n", basename(repaired_file)))
                copy_query <- sprintf(
                    "COPY (%s) TO '%s' (FORMAT PARQUET)",
                    query, repaired_file
                )
                dbExecute(con_temp, copy_query)
                
                # 저장 완료 후 통계 확인
                count_query <- sprintf(
                    "SELECT COUNT(*) as total_rows, COUNT(DISTINCT key) as unique_keys FROM (%s)",
                    query
                )
                count_result <- dbGetQuery(con_temp, count_query)
                cat(sprintf("  → 병합 완료: 총 %d개 행, 유니크한 key %d개\n", 
                    count_result$total_rows, count_result$unique_keys))
                
                # merged_data는 사용하지 않음 (DuckDB에서 직접 저장했으므로)
                merged_data <- NULL
            }, finally = {
                # DuckDB 연결 해제 (에러가 발생해도 반드시 실행)
                dbDisconnect(con_temp, shutdown = TRUE)
                rm(con_temp); gc()
            })
        }
        
        # repaired 파일로 저장
        if (!is.null(merged_data)) {
            # merged_data가 있는 경우 (기존 파일만 또는 recovered 파일만 있는 경우)
            cat(sprintf("  저장 중: %s...\n", basename(repaired_file)))
            arrow::write_parquet(merged_data, repaired_file)
            unique_keys_final <- length(unique(merged_data$key))
            cat(sprintf("  ✓ 저장 완료: %s (총 %d개 행, 유니크한 key %d개)\n", 
                basename(repaired_file), nrow(merged_data), unique_keys_final))
            rm(merged_data)
        } else {
            # DuckDB에서 이미 저장한 경우
            cat(sprintf("  ✓ 저장 완료: %s (DuckDB에서 직접 저장)\n", basename(repaired_file)))
        }
        
        # 메모리 정리
        gc(verbose = FALSE)
        
        cat(sprintf("  ✓ 메모리 정리 완료\n"))
    }
    
    cat("\n=== 매핑 파일 병합 완료 ===\n")
    return(invisible(NULL))
}

# 매핑 파일 병합 실행
merge_mapping_files(fu = 10)
# === 매핑 파일 병합 시작 ===
# 매핑 디렉토리: /home/hashjamm/results/disease_network/hr_mapping_results_v10_recovered/
# FU: 10

# --- edge_pids 처리 중 ---
#   기존 파일 로드: edge_pids_mapping_10.parquet (770257개 key)
#   recovered 파일 로드: recovered_edge_pids_mapping_10.parquet (175397개 key)
#   저장 중: repaired_edge_pids_mapping_10.parquet...
#   → 병합 완료: 총 945654개 행, 유니크한 key 945654개
#   ✓ 저장 완료: repaired_edge_pids_mapping_10.parquet (DuckDB에서 직접 저장)
#   ✓ 메모리 정리 완료

# --- edge_index_key_seq 처리 중 ---
#   기존 파일 로드: edge_index_key_seq_mapping_10.parquet (770257개 key)
#   recovered 파일 로드: recovered_edge_index_key_seq_mapping_10.parquet (175397개 key)
#   저장 중: repaired_edge_index_key_seq_mapping_10.parquet...
#   → 병합 완료: 총 945654개 행, 유니크한 key 945654개
#   ✓ 저장 완료: repaired_edge_index_key_seq_mapping_10.parquet (DuckDB에서 직접 저장)
#   ✓ 메모리 정리 완료

# --- edge_key_seq 처리 중 ---
#   기존 파일 로드: edge_key_seq_mapping_10.parquet (530925개 key)
#   recovered 파일 로드: recovered_edge_key_seq_mapping_10.parquet (147027개 key)
#   저장 중: repaired_edge_key_seq_mapping_10.parquet...
#   → 병합 완료: 총 677952개 행, 유니크한 key 677952개
#   ✓ 저장 완료: repaired_edge_key_seq_mapping_10.parquet (DuckDB에서 직접 저장)
#   ✓ 메모리 정리 완료

# === 매핑 파일 병합 완료 ===

# ============================================================================
# 함수 7: repaired 파일 기준으로 edge_key_seq에만 없는 key들의 HR 결과 검증
# 
# [가설 검증 목적]
# 가설: HR 결과는 있지만 edge_key_seq에는 없는 조합들은 HR < 1이어야 함
#
# [가설의 근거]
# - edge_key_seq는 case=1 & status=1 (원인 질병이 있고, outcome event가 발생한 경우)만 기록됨
# - HR 결과는 있지만 edge_key_seq에 없다는 것은:
#   * case=1인 그룹이 존재함 (HR 분석 가능)
#   * 하지만 case=1 그룹에서 outcome event가 발생하지 않음 (status=0)
# - 따라서 case=0 그룹에서만 event가 발생했거나, 양쪽 그룹 모두 event가 없을 수 있음
# - case=0에서만 event 발생 → HR < 1
# - 양쪽 그룹 모두 event 없음 → HR = 1 (또는 정의되지 않음)
#
# [검증 방법]
# 1. repaired 파일들에서 key 추출
# 2. HR 결과에서 key 추출
# 3. HR에는 있지만 edge_key_seq에는 없는 조합 찾기
# 4. 해당 조합들의 HR 값 분포 확인
# 5. HR > 1인 경우가 있는지 확인 (가설과 다르면 추가 조사 필요)
#
# ============================================================================
validate_hr_for_missing_edge_key_seq <- function(
    mapping_dir = "/home/hashjamm/results/disease_network/hr_mapping_results_v10_recovered/",
    hr_results_path = "/home/hashjamm/results/disease_network/hr_results_v10/total_hr_results_10.parquet",
    fu = 10
) {
    cat("\n=== HR 결과 검증 시작 ===\n")
    cat(sprintf("매핑 디렉토리: %s\n", mapping_dir))
    cat(sprintf("HR 결과 파일: %s\n", hr_results_path))
    cat(sprintf("FU: %d\n", fu))
    
    # repaired 파일 경로
    repaired_pids_file <- file.path(mapping_dir, sprintf("repaired_edge_pids_mapping_%d.parquet", fu))
    repaired_index_file <- file.path(mapping_dir, sprintf("repaired_edge_index_key_seq_mapping_%d.parquet", fu))
    repaired_key_file <- file.path(mapping_dir, sprintf("repaired_edge_key_seq_mapping_%d.parquet", fu))
    
    # repaired 파일들에서 key 추출
    # [에러 해결]
    # 문제: arrow::read_parquet()로 repaired 파일을 읽을 때 "IOError: List index overflow" 발생
    # 원인: Arrow의 리스트 배열 제한 (이전과 동일한 문제)
    # 해결: DuckDB를 사용하여 key 컬럼만 추출 (리스트 배열을 R 메모리로 가져오지 않음)
    #
    cat("\n--- repaired 파일에서 key 추출 중 ---\n")
    
    con_temp <- dbConnect(duckdb::duckdb(), ":memory:")
    
    tryCatch({
        # DuckDB로 key만 추출 (리스트 배열 제한 회피)
        repaired_pids_keys <- dbGetQuery(con_temp, 
            sprintf("SELECT DISTINCT key FROM read_parquet('%s')", repaired_pids_file))$key
        cat(sprintf("  repaired_edge_pids_mapping: %d개 key\n", length(repaired_pids_keys)))
        
        repaired_index_keys <- dbGetQuery(con_temp, 
            sprintf("SELECT DISTINCT key FROM read_parquet('%s')", repaired_index_file))$key
        cat(sprintf("  repaired_edge_index_key_seq_mapping: %d개 key\n", length(repaired_index_keys)))
        
        repaired_key_keys <- dbGetQuery(con_temp, 
            sprintf("SELECT DISTINCT key FROM read_parquet('%s')", repaired_key_file))$key
        cat(sprintf("  repaired_edge_key_seq_mapping: %d개 key\n", length(repaired_key_keys)))
    }, finally = {
        dbDisconnect(con_temp, shutdown = TRUE)
        rm(con_temp); gc()
    })
    
    # HR 결과에서 key 추출
    cat("\n--- HR 결과에서 key 추출 중 ---\n")
    hr_results <- arrow::read_parquet(hr_results_path)
    hr_keys <- paste(hr_results$cause_abb, hr_results$outcome_abb, hr_results$fu, sep = "_")
    cat(sprintf("  HR 결과: %d개 조합\n", length(unique(hr_keys))))
    
    # HR에는 있지만 edge_key_seq에는 없는 조합 찾기
    cat("\n--- HR에는 있지만 edge_key_seq에는 없는 조합 찾기 ---\n")
    hr_only_keys <- setdiff(hr_keys, repaired_key_keys)
    hr_only_unique_keys <- unique(hr_only_keys)
    cat(sprintf("  발견된 조합: %d개\n", length(hr_only_unique_keys)))
    
    if (length(hr_only_unique_keys) == 0) {
        cat("  ✓ 모든 HR 결과에 대해 edge_key_seq가 존재합니다.\n")
        return(invisible(NULL))
    }
    
    # 해당 조합들의 HR 값 분석
    cat("\n--- HR 값 분석 ---\n")
    hr_only_data <- hr_results %>%
        mutate(key = paste(cause_abb, outcome_abb, fu, sep = "_")) %>%
        filter(key %in% hr_only_unique_keys) %>%
        collect()
    
    cat(sprintf("  분석 대상: %d개 조합\n", nrow(hr_only_data)))
    
    # HR 값 통계
    cat("\n=== HR 값 통계 ===\n")
    hr_summary <- hr_only_data %>%
        summarise(
            min_hr = min(hr_values, na.rm = TRUE),
            max_hr = max(hr_values, na.rm = TRUE),
            mean_hr = mean(hr_values, na.rm = TRUE),
            median_hr = median(hr_values, na.rm = TRUE),
            q25_hr = quantile(hr_values, 0.25, na.rm = TRUE),
            q75_hr = quantile(hr_values, 0.75, na.rm = TRUE)
        ) %>%
        collect()
    
    cat(sprintf("  최소값: %f\n", hr_summary$min_hr))
    cat(sprintf("  최대값: %f\n", hr_summary$max_hr))
    cat(sprintf("  평균값: %f\n", hr_summary$mean_hr))
    cat(sprintf("  중앙값: %f\n", hr_summary$median_hr))
    cat(sprintf("  25%% 분위수: %f\n", hr_summary$q25_hr))  # %%로 % 이스케이프
    cat(sprintf("  75%% 분위수: %f\n", hr_summary$q75_hr))  # %%로 % 이스케이프
    
    # HR 값 분포
    cat("\n=== HR 값 분포 ===\n")
    hr_less_than_1 <- sum(hr_only_data$hr_values < 1, na.rm = TRUE)
    hr_equal_1 <- sum(hr_only_data$hr_values == 1, na.rm = TRUE)
    hr_greater_than_1 <- sum(hr_only_data$hr_values > 1, na.rm = TRUE)
    total_valid <- sum(!is.na(hr_only_data$hr_values))
    
    cat(sprintf("  HR < 1: %d개 (%.2f%%)\n", hr_less_than_1, 100 * hr_less_than_1 / total_valid))
    cat(sprintf("  HR = 1: %d개 (%.2f%%)\n", hr_equal_1, 100 * hr_equal_1 / total_valid))
    cat(sprintf("  HR > 1: %d개 (%.2f%%)\n", hr_greater_than_1, 100 * hr_greater_than_1 / total_valid))
    cat(sprintf("  NA: %d개\n", sum(is.na(hr_only_data$hr_values))))
    
    # 가설 검증
    cat("\n=== 가설 검증 ===\n")
    cat("가설: HR 결과는 있지만 edge_key_seq에는 없는 조합들은 HR < 1이어야 함\n")
    cat("  (case=1에서 event가 발생하지 않았으므로, case=0에서만 event 발생 → HR < 1)\n\n")
    
    if (hr_greater_than_1 > 0) {
        cat(sprintf("  ⚠ 가설과 다름: HR > 1인 경우가 %d개 발견됨\n", hr_greater_than_1))
        cat("\n  HR > 1인 경우 상세 (상위 20개):\n")
        hr_greater_data <- hr_only_data %>%
            filter(hr_values > 1) %>%
            select(cause_abb, outcome_abb, fu, hr_values) %>%
            arrange(desc(hr_values)) %>%
            head(20)
        print(hr_greater_data)
    } else {
        cat("  ✓ 가설과 일치: HR > 1인 경우 없음\n")
    }
    
    if (hr_equal_1 > 0) {
        cat(sprintf("\n  ℹ HR = 1인 경우: %d개 (양쪽 그룹 모두 event 없음 가능)\n", hr_equal_1))
    }
    
    # 메모리 정리
    rm(hr_results, hr_only_data, hr_summary); gc()
    
    cat("\n=== 검증 완료 ===\n")
    
    return(list(
        hr_only_count = length(hr_only_unique_keys),
        hr_less_than_1 = hr_less_than_1,
        hr_equal_1 = hr_equal_1,
        hr_greater_than_1 = hr_greater_than_1,
        hypothesis_supported = (hr_greater_than_1 == 0)
    ))
}

# 검증 실행
validate_hr_for_missing_edge_key_seq(fu = 10)
# === HR 결과 검증 시작 ===
# 매핑 디렉토리: /home/hashjamm/results/disease_network/hr_mapping_results_v10_recovered/
# HR 결과 파일: /home/hashjamm/results/disease_network/hr_results_v10/total_hr_results_10.parquet
# FU: 10

# --- repaired 파일에서 key 추출 중 ---
#   repaired_edge_pids_mapping: 945654개 key
#   repaired_edge_index_key_seq_mapping: 945654개 key
#   repaired_edge_key_seq_mapping: 677952개 key

# --- HR 결과에서 key 추출 중 ---
#   HR 결과: 945654개 조합

# --- HR에는 있지만 edge_key_seq에는 없는 조합 찾기 ---
#   발견된 조합: 267702개

# --- HR 값 분석 ---
#   분석 대상: 267702개 조합

# === HR 값 통계 ===
#   최소값: 0.000000
#   최대값: 1.000000
#   평균값: 0.000139
#   중앙값: 0.000000
#   25% 분위수: 0.000000
#   75% 분위수: 0.000000

# === HR 값 분포 ===
#   HR < 1: 258786개 (99.99%)
#   HR = 1: 36개 (0.01%)
#   HR > 1: 0개 (0.00%)
#   NA: 8880개

# === 가설 검증 ===
# 가설: HR 결과는 있지만 edge_key_seq에는 없는 조합들은 HR < 1이어야 함
#   (case=1에서 event가 발생하지 않았으므로, case=0에서만 event 발생 → HR < 1)

#   ✓ 가설과 일치: HR > 1인 경우 없음

#   ℹ HR = 1인 경우: 36개 (양쪽 그룹 모두 event 없음 가능)

# === 검증 완료 ===
# $hr_only_count
# [1] 267702

# $hr_less_than_1
# [1] 258786

# $hr_equal_1
# [1] 36

# $hr_greater_than_1
# [1] 0

# $hypothesis_supported
# [1] TRUE

# HR < 1 인 경우에 대한 개인적인 의견
# 물론 HR < 1 인 경우, 즉 case=0 인 경우에서만 outcome이 발생한 경우에 대해서는 key_seq 리스트를 control 그룹에 대해서
# 구해놓는 것 또한 문제가 있다. 왜냐하면 사실 지금은 아래와 같이 하고 있다.
# case == 1 인 경우에 대하여 edge_pids, edge_index_key_seq을 구해놓았었다.
# case == 1 & status == 1 인 경우에 대해서 edge_key_seq을 구해놓았었는데, 이걸 별도로 protective한 걸로 모아둬봤다 나머지가 모두 구조가 안맞다..
# 즉, risk 쪽으로 갈거면 risk만 봐야 맞고, protectvie한걸 보려면 아예 모든걸 바꿔야 한다.
# detail 하게 한 쪽으로만 진행하는게 맞다.
