# 기존 파일과 새로 생성된 파일 비교 스크립트
# DuckDB 직접 저장 방식으로 새로 생성된 파일이 기존과 동일한지 확인

library(arrow)

cat("=== 기존 파일과 새로 생성된 파일 비교 ===\n\n")

mapping_folder <- "/home/hashjamm/results/disease_network/hr_mapping_results_v10"
fu <- 10

# 기존 파일 (있다면)
existing_file <- file.path(mapping_folder, sprintf("edge_pids_mapping_%d.parquet", fu))

# 새로 생성될 파일 (테스트용으로 DuckDB 직접 생성)
test_file <- file.path(mapping_folder, "test_edge_pids_duckdb_direct.parquet")

cat("1. 파일 존재 확인\n")
cat(sprintf("  기존 파일: %s\n", ifelse(file.exists(existing_file), "존재", "없음")))
cat(sprintf("  테스트 파일: %s\n", ifelse(file.exists(test_file), "존재", "없음")))

if (file.exists(existing_file) && file.exists(test_file)) {
    cat("\n2. 파일 구조 비교\n")
    
    existing_data <- arrow::read_parquet(existing_file)
    test_data <- arrow::read_parquet(test_file)
    
    cat(sprintf("  기존 파일 행 수: %d\n", nrow(existing_data)))
    cat(sprintf("  테스트 파일 행 수: %d\n", nrow(test_data)))
    
    cat("\n3. 공통 key 비교\n")
    common_keys <- intersect(existing_data$key, test_data$key)
    only_existing <- setdiff(existing_data$key, test_data$key)
    only_test <- setdiff(test_data$key, existing_data$key)
    
    cat(sprintf("  공통 key 수: %d\n", length(common_keys)))
    cat(sprintf("  기존에만 있는 key 수: %d\n", length(only_existing)))
    cat(sprintf("  테스트에만 있는 key 수: %d\n", length(only_test)))
    
    if (length(common_keys) > 0) {
        cat("\n4. 공통 key의 values 비교\n")
        test_key <- common_keys[1]
        
        existing_values <- existing_data$values[existing_data$key == test_key][[1]]
        test_values <- test_data$values[test_data$key == test_key][[1]]
        
        cat(sprintf("  테스트 key: %s\n", test_key))
        cat(sprintf("  기존 values 길이: %d\n", length(existing_values)))
        cat(sprintf("  테스트 values 길이: %d\n", length(test_values)))
        
        if (length(existing_values) == length(test_values)) {
            existing_sorted <- sort(existing_values)
            test_sorted <- sort(test_values)
            
            if (identical(existing_sorted, test_sorted)) {
                cat("  ✓ values 일치\n")
            } else {
                cat("  ✗ values 불일치\n")
                diff_count <- sum(existing_sorted != test_sorted)
                cat(sprintf("    차이 개수: %d\n", diff_count))
            }
        } else {
            cat("  ✗ values 길이 불일치\n")
        }
    }
    
    cat("\n5. 통계 비교\n")
    existing_lengths <- sapply(existing_data$values, length)
    test_lengths <- sapply(test_data$values, length)
    
    cat("  기존 파일:\n")
    cat(sprintf("    평균 values 길이: %.2f\n", mean(existing_lengths)))
    cat(sprintf("    총 values 요소 수: %d\n", sum(existing_lengths)))
    
    cat("  테스트 파일:\n")
    cat(sprintf("    평균 values 길이: %.2f\n", mean(test_lengths)))
    cat(sprintf("    총 values 요소 수: %d\n", sum(test_lengths)))
    
} else {
    cat("\n비교할 파일이 없습니다.\n")
    cat("실제 실행 후 생성된 파일을 확인하세요.\n")
}

cat("\n=== 비교 완료 ===\n")

