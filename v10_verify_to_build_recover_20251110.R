library(duckdb)
library(DBI)
library(arrow)
library(dplyr)

cat("=== HR 결과, Stat Failed, System Failed, Completed Jobs 쌍 비교 분석 ===\n\n")

# 경로 설정
hr_results_folder <- "/home/hashjamm/results/disease_network/hr_results_v10"
stat_failed_db <- "/home/hashjamm/results/disease_network/hr_job_queue_db_v10/stat_failed_jobs.duckdb"
system_failed_db <- "/home/hashjamm/results/disease_network/hr_job_queue_db_v10/system_failed_jobs.duckdb"
completed_jobs_db <- "/home/hashjamm/results/disease_network/hr_job_queue_db_v10/completed_jobs.duckdb"

# 1. HR 결과 로드
cat("1. HR 결과 로드 중...\n")
hr_files <- list.files(hr_results_folder, pattern="total_hr_results.*\\.parquet", full.names=TRUE)

if (length(hr_files) == 0) {
    stop("HR 결과 파일을 찾을 수 없습니다.")
}

cat(sprintf("   발견된 HR 결과 파일: %d개\n", length(hr_files)))

hr_results_list <- list()
for (hr_file in hr_files) {
    cat(sprintf("   로드 중: %s\n", basename(hr_file)))
    hr_data <- arrow::read_parquet(hr_file)
    hr_results_list[[length(hr_results_list) + 1]] <- hr_data
}

hr_results <- bind_rows(hr_results_list)
hr_pairs <- hr_results %>%
    select(cause_abb, outcome_abb, fu) %>%
    distinct() %>%
    mutate(key = paste(cause_abb, outcome_abb, fu, sep = "_"))

cat(sprintf("   HR 결과 쌍 수: %d개\n", nrow(hr_pairs)))
cat(sprintf("   HR 결과 총 행 수: %d개\n", nrow(hr_results)))
cat("\n")

# 2. Stat Failed 로드
cat("2. Stat Failed 로드 중...\n")
if (!file.exists(stat_failed_db)) {
    stop(sprintf("Stat Failed DB 파일을 찾을 수 없습니다: %s", stat_failed_db))
}

con_stat <- dbConnect(duckdb::duckdb(), stat_failed_db, read_only = TRUE)
on.exit(dbDisconnect(con_stat, shutdown = TRUE), add = TRUE)

# 테이블 확인
tables <- dbListTables(con_stat)
cat(sprintf("   테이블 목록: %s\n", paste(tables, collapse = ", ")))

if ("stat_failures" %in% tables) {
    stat_failed <- dbGetQuery(con_stat, "SELECT DISTINCT cause_abb, outcome_abb, fu FROM stat_failures")
    stat_failed_pairs <- stat_failed %>%
        mutate(key = paste(cause_abb, outcome_abb, fu, sep = "_"))
    
    cat(sprintf("   Stat Failed 쌍 수: %d개\n", nrow(stat_failed_pairs)))
    
    # 전체 행 수 확인
    total_stat_failed <- dbGetQuery(con_stat, "SELECT COUNT(*) as cnt FROM stat_failures")$cnt
    cat(sprintf("   Stat Failed 총 행 수: %d개\n", total_stat_failed))
} else {
    cat("   'stat_failures' 테이블을 찾을 수 없습니다.\n")
    stat_failed_pairs <- data.frame(cause_abb = character(), outcome_abb = character(), fu = integer(), key = character())
}

cat("\n")

# 3. System Failed 로드
cat("3. System Failed 로드 중...\n")
if (!file.exists(system_failed_db)) {
    cat("   System Failed DB 파일을 찾을 수 없습니다. 건너뜁니다.\n")
    system_failed_pairs <- data.frame(cause_abb = character(), outcome_abb = character(), fu = integer(), key = character())
} else {
    con_system <- dbConnect(duckdb::duckdb(), system_failed_db, read_only = TRUE)
    on.exit(dbDisconnect(con_system, shutdown = TRUE), add = TRUE)
    
    # 테이블 확인
    tables_system <- dbListTables(con_system)
    cat(sprintf("   테이블 목록: %s\n", paste(tables_system, collapse = ", ")))
    
    if ("system_failures" %in% tables_system) {
        system_failed <- dbGetQuery(con_system, "SELECT DISTINCT cause_abb, outcome_abb, fu FROM system_failures")
        system_failed_pairs <- system_failed %>%
            mutate(key = paste(cause_abb, outcome_abb, fu, sep = "_"))
        
        cat(sprintf("   System Failed 쌍 수: %d개\n", nrow(system_failed_pairs)))
        
        # 전체 행 수 확인
        total_system_failed <- dbGetQuery(con_system, "SELECT COUNT(*) as cnt FROM system_failures")$cnt
        cat(sprintf("   System Failed 총 행 수: %d개\n", total_system_failed))
    } else {
        cat("   'system_failures' 테이블을 찾을 수 없습니다.\n")
        system_failed_pairs <- data.frame(cause_abb = character(), outcome_abb = character(), fu = integer(), key = character())
    }
}

cat("\n")

# 4. Completed Jobs 로드
cat("4. Completed Jobs 로드 중...\n")
if (!file.exists(completed_jobs_db)) {
    cat("   Completed Jobs DB 파일을 찾을 수 없습니다. 건너뜁니다.\n")
    completed_pairs <- data.frame(cause_abb = character(), outcome_abb = character(), fu = integer(), key = character())
} else {
    con_completed <- dbConnect(duckdb::duckdb(), completed_jobs_db, read_only = TRUE)
    on.exit(dbDisconnect(con_completed, shutdown = TRUE), add = TRUE)
    
    # 테이블 확인
    tables_completed <- dbListTables(con_completed)
    cat(sprintf("   테이블 목록: %s\n", paste(tables_completed, collapse = ", ")))
    
    if ("jobs" %in% tables_completed) {
        completed_jobs <- dbGetQuery(con_completed, "SELECT DISTINCT cause_abb, outcome_abb, fu FROM jobs")
        completed_pairs <- completed_jobs %>%
            mutate(key = paste(cause_abb, outcome_abb, fu, sep = "_"))
        
        cat(sprintf("   Completed Jobs 쌍 수: %d개\n", nrow(completed_pairs)))
        
        # 전체 행 수 확인
        total_completed <- dbGetQuery(con_completed, "SELECT COUNT(*) as cnt FROM jobs")$cnt
        cat(sprintf("   Completed Jobs 총 행 수: %d개\n", total_completed))
    } else {
        cat("   'jobs' 테이블을 찾을 수 없습니다.\n")
        completed_pairs <- data.frame(cause_abb = character(), outcome_abb = character(), fu = integer(), key = character())
    }
}

cat("\n")

# 5. 비교 분석
cat("=== 비교 분석 결과 ===\n\n")

# 모든 키 집합 준비
hr_keys <- hr_pairs$key
stat_failed_keys <- if (nrow(stat_failed_pairs) > 0) stat_failed_pairs$key else character()
system_failed_keys <- if (nrow(system_failed_pairs) > 0) system_failed_pairs$key else character()
completed_keys <- if (nrow(completed_pairs) > 0) completed_pairs$key else character()

# 전체 키 집합
all_keys <- unique(c(hr_keys, stat_failed_keys, system_failed_keys, completed_keys))

cat(sprintf("전체 고유 쌍 수: %d개\n\n", length(all_keys)))

# 각 집합별 개수
cat("=== 각 집합별 쌍 수 ===\n")
cat(sprintf("HR 결과: %d개\n", length(hr_keys)))
cat(sprintf("Stat Failed: %d개\n", length(stat_failed_keys)))
cat(sprintf("System Failed: %d개\n", length(system_failed_keys)))
cat(sprintf("Completed Jobs: %d개\n", length(completed_keys)))
cat("\n")

# 두 집합 간 겹침 분석
cat("=== 두 집합 간 겹침 분석 ===\n\n")

# HR vs Stat Failed
if (length(stat_failed_keys) > 0) {
    hr_stat_overlap <- intersect(hr_keys, stat_failed_keys)
    hr_only_stat <- setdiff(hr_keys, stat_failed_keys)
    stat_only_hr <- setdiff(stat_failed_keys, hr_keys)
    
    cat("1. HR 결과 vs Stat Failed:\n")
    cat(sprintf("   - 겹침: %d개\n", length(hr_stat_overlap)))
    cat(sprintf("   - HR만: %d개\n", length(hr_only_stat)))
    cat(sprintf("   - Stat Failed만: %d개\n", length(stat_only_hr)))
    cat("\n")
}

# HR vs Completed
if (length(completed_keys) > 0) {
    hr_completed_overlap <- intersect(hr_keys, completed_keys)
    hr_only_completed <- setdiff(hr_keys, completed_keys)
    completed_only_hr <- setdiff(completed_keys, hr_keys)
    
    cat("2. HR 결과 vs Completed Jobs:\n")
    cat(sprintf("   - 겹침: %d개\n", length(hr_completed_overlap)))
    cat(sprintf("   - HR만: %d개\n", length(hr_only_completed)))
    cat(sprintf("   - Completed만: %d개\n", length(completed_only_hr)))
    cat("\n")
}

# Stat Failed vs Completed
if (length(stat_failed_keys) > 0 && length(completed_keys) > 0) {
    stat_completed_overlap <- intersect(stat_failed_keys, completed_keys)
    stat_only_completed <- setdiff(stat_failed_keys, completed_keys)
    completed_only_stat <- setdiff(completed_keys, stat_failed_keys)
    
    cat("3. Stat Failed vs Completed Jobs:\n")
    cat(sprintf("   - 겹침: %d개\n", length(stat_completed_overlap)))
    cat(sprintf("   - Stat Failed만: %d개\n", length(stat_only_completed)))
    cat(sprintf("   - Completed만: %d개\n", length(completed_only_stat)))
    cat("\n")
}

# System Failed와 다른 집합 간 겹침 분석
cat("=== System Failed와 다른 집합 간 겹침 분석 ===\n\n")

if (length(system_failed_keys) > 0) {
    hr_system_overlap <- intersect(hr_keys, system_failed_keys)
    stat_system_overlap <- intersect(stat_failed_keys, system_failed_keys)
    completed_system_overlap <- intersect(completed_keys, system_failed_keys)
    
    cat("System Failed vs 다른 집합:\n")
    cat(sprintf("   - HR 결과와 겹침: %d개\n", length(hr_system_overlap)))
    cat(sprintf("   - Stat Failed와 겹침: %d개\n", length(stat_system_overlap)))
    cat(sprintf("   - Completed Jobs와 겹침: %d개\n", length(completed_system_overlap)))
    cat("\n")
}

# 네 집합 모두 겹침 분석
cat("=== 네 집합 모두 겹침 분석 ===\n\n")

if (length(stat_failed_keys) > 0 && length(completed_keys) > 0) {
    # 네 집합 모두 겹침
    all_four <- intersect(intersect(intersect(hr_keys, stat_failed_keys), system_failed_keys), completed_keys)
    
    # 세 집합만 겹침 (각 조합)
    hr_stat_system <- setdiff(intersect(intersect(hr_keys, stat_failed_keys), system_failed_keys), completed_keys)
    hr_stat_completed <- setdiff(intersect(intersect(hr_keys, stat_failed_keys), completed_keys), system_failed_keys)
    hr_system_completed <- setdiff(intersect(intersect(hr_keys, system_failed_keys), completed_keys), stat_failed_keys)
    stat_system_completed <- setdiff(intersect(intersect(stat_failed_keys, system_failed_keys), completed_keys), hr_keys)
    
    # 두 집합만 겹침
    hr_stat_only <- setdiff(intersect(hr_keys, stat_failed_keys), union(system_failed_keys, completed_keys))
    hr_system_only <- setdiff(intersect(hr_keys, system_failed_keys), union(stat_failed_keys, completed_keys))
    hr_completed_only <- setdiff(intersect(hr_keys, completed_keys), union(stat_failed_keys, system_failed_keys))
    stat_system_only <- setdiff(intersect(stat_failed_keys, system_failed_keys), union(hr_keys, completed_keys))
    stat_completed_only <- setdiff(intersect(stat_failed_keys, completed_keys), union(hr_keys, system_failed_keys))
    system_completed_only <- setdiff(intersect(system_failed_keys, completed_keys), union(hr_keys, stat_failed_keys))
    
    # 한 집합만 (다른 세 집합에는 없음)
    hr_only_all <- setdiff(setdiff(setdiff(hr_keys, stat_failed_keys), system_failed_keys), completed_keys)
    stat_only_all <- setdiff(setdiff(setdiff(stat_failed_keys, hr_keys), system_failed_keys), completed_keys)
    system_only_all <- setdiff(setdiff(setdiff(system_failed_keys, hr_keys), stat_failed_keys), completed_keys)
    completed_only_all <- setdiff(setdiff(setdiff(completed_keys, hr_keys), stat_failed_keys), system_failed_keys)
    
    cat("=== 집합 조합별 쌍 수 ===\n")
    cat(sprintf("1. 네 집합 모두 겹침: %d개\n", length(all_four)))
    cat(sprintf("\n2. 세 집합만 겹침:\n"))
    cat(sprintf("   - HR + Stat Failed + System Failed: %d개\n", length(hr_stat_system)))
    cat(sprintf("   - HR + Stat Failed + Completed: %d개\n", length(hr_stat_completed)))
    cat(sprintf("   - HR + System Failed + Completed: %d개\n", length(hr_system_completed)))
    cat(sprintf("   - Stat Failed + System Failed + Completed: %d개\n", length(stat_system_completed)))
    cat(sprintf("\n3. 두 집합만 겹침:\n"))
    cat(sprintf("   - HR + Stat Failed만: %d개\n", length(hr_stat_only)))
    cat(sprintf("   - HR + System Failed만: %d개\n", length(hr_system_only)))
    cat(sprintf("   - HR + Completed만: %d개\n", length(hr_completed_only)))
    cat(sprintf("   - Stat Failed + System Failed만: %d개\n", length(stat_system_only)))
    cat(sprintf("   - Stat Failed + Completed만: %d개\n", length(stat_completed_only)))
    cat(sprintf("   - System Failed + Completed만: %d개\n", length(system_completed_only)))
    cat(sprintf("\n4. 한 집합만:\n"))
    cat(sprintf("   - HR만: %d개\n", length(hr_only_all)))
    cat(sprintf("   - Stat Failed만: %d개\n", length(stat_only_all)))
    cat(sprintf("   - System Failed만: %d개\n", length(system_only_all)))
    cat(sprintf("   - Completed만: %d개\n", length(completed_only_all)))
    cat("\n")
    
    # "~만" 쌍 수가 모두 0인 이유 분석
    cat("=== '~만' 쌍 수 분석 ===\n")
    cat("'~만' 쌍 수가 모두 0인 이유:\n")
    cat(sprintf("- HR만: %d개 → HR 결과 중 다른 집합에 없는 쌍\n", length(hr_only_all)))
    cat(sprintf("- Stat Failed만: %d개 → Stat Failed 중 다른 집합에 없는 쌍\n", length(stat_only_all)))
    cat(sprintf("- System Failed만: %d개 → System Failed 중 다른 집합에 없는 쌍\n", length(system_only_all)))
    cat(sprintf("- Completed만: %d개 → Completed 중 다른 집합에 없는 쌍\n", length(completed_only_all)))
    
    if (length(hr_only_all) == 0 && length(stat_only_all) == 0 && 
        length(system_only_all) == 0 && length(completed_only_all) == 0) {
        cat("\n→ 모든 쌍이 최소 2개 이상의 집합에 포함되어 있습니다.\n")
        cat("→ 이는 다음을 의미할 수 있습니다:\n")
        cat("  1. HR 결과의 모든 쌍이 Completed에도 포함됨\n")
        cat("  2. Stat Failed의 모든 쌍이 Completed에도 포함됨\n")
        cat("  3. System Failed의 모든 쌍이 Completed에도 포함됨\n")
        cat("  4. Completed는 다른 집합들의 합집합\n")
    }
    cat("\n")
    
    # 네 집합 모두 겹치는 쌍 상세 정보
    if (length(all_four) > 0) {
        cat("=== 네 집합 모두 겹치는 쌍 (처음 20개) ===\n")
        all_four_pairs <- hr_pairs %>%
            filter(key %in% all_four) %>%
            head(20)
        print(all_four_pairs[, c("cause_abb", "outcome_abb", "fu")])
        
        if (length(all_four) > 20) {
            cat(sprintf("\n... 외 %d개 쌍\n", length(all_four) - 20))
        }
        cat("\n")
    }
    
    # HR + Stat Failed만 있는 쌍 (System Failed와 Completed에는 없음)
    if (length(hr_stat_only) > 0) {
        cat("=== HR + Stat Failed만 있는 쌍 (Completed 없음, 처음 20개) ===\n")
        hr_stat_only_pairs <- hr_pairs %>%
            filter(key %in% hr_stat_only) %>%
            head(20)
        print(hr_stat_only_pairs[, c("cause_abb", "outcome_abb", "fu")])
        
        if (length(hr_stat_only) > 20) {
            cat(sprintf("\n... 외 %d개 쌍\n", length(hr_stat_only) - 20))
        }
        cat("\n")
    }
    
    # HR + Completed만 있는 쌍 (Stat Failed에는 없음)
    if (length(hr_completed_only) > 0) {
        cat("=== HR + Completed만 있는 쌍 (Stat Failed 없음, 처음 20개) ===\n")
        hr_completed_only_pairs <- hr_pairs %>%
            filter(key %in% hr_completed_only) %>%
            head(20)
        print(hr_completed_only_pairs[, c("cause_abb", "outcome_abb", "fu")])
        
        if (length(hr_completed_only) > 20) {
            cat(sprintf("\n... 외 %d개 쌍\n", length(hr_completed_only) - 20))
        }
        cat("\n")
    }
    
    # Stat Failed + Completed만 있는 쌍 (HR에는 없음)
    if (length(stat_completed_only) > 0) {
        cat("=== Stat Failed + Completed만 있는 쌍 (HR 없음, 처음 20개) ===\n")
        if (nrow(stat_failed_pairs) > 0) {
            stat_completed_only_pairs <- stat_failed_pairs %>%
                filter(key %in% stat_completed_only) %>%
                head(20)
            print(stat_completed_only_pairs[, c("cause_abb", "outcome_abb", "fu")])
        } else {
            stat_completed_only_pairs <- completed_pairs %>%
                filter(key %in% stat_completed_only) %>%
                head(20)
            print(stat_completed_only_pairs[, c("cause_abb", "outcome_abb", "fu")])
        }
        
        if (length(stat_completed_only) > 20) {
            cat(sprintf("\n... 외 %d개 쌍\n", length(stat_completed_only) - 20))
        }
        cat("\n")
    }
    
    # 요약 테이블
    cat("=== 요약 테이블 ===\n")
    summary_table <- data.frame(
        구분 = c(
            "HR 결과 전체",
            "Stat Failed 전체",
            "System Failed 전체",
            "Completed Jobs 전체",
            "HR + Stat Failed 겹침",
            "HR + System Failed 겹침",
            "HR + Completed 겹침",
            "Stat Failed + System Failed 겹침",
            "Stat Failed + Completed 겹침",
            "System Failed + Completed 겹침",
            "네 집합 모두 겹침",
            "HR만",
            "Stat Failed만",
            "System Failed만",
            "Completed만"
        ),
        쌍_수 = c(
            length(hr_keys),
            length(stat_failed_keys),
            length(system_failed_keys),
            length(completed_keys),
            length(intersect(hr_keys, stat_failed_keys)),
            length(intersect(hr_keys, system_failed_keys)),
            length(intersect(hr_keys, completed_keys)),
            length(intersect(stat_failed_keys, system_failed_keys)),
            length(intersect(stat_failed_keys, completed_keys)),
            length(intersect(system_failed_keys, completed_keys)),
            length(all_four),
            length(hr_only_all),
            length(stat_only_all),
            length(system_only_all),
            length(completed_only_all)
        )
    )
    print(summary_table)
    
    # 검증: 합계 확인
    cat("\n=== 검증 ===\n")
    hr_plus_stat <- length(hr_keys) + length(stat_failed_keys)
    hr_plus_stat_plus_system <- hr_plus_stat + length(system_failed_keys)
    cat(sprintf("HR + Stat Failed 합계: %d개\n", hr_plus_stat))
    cat(sprintf("HR + Stat Failed + System Failed 합계: %d개\n", hr_plus_stat_plus_system))
    cat(sprintf("Completed Jobs: %d개\n", length(completed_keys)))
    cat(sprintf("차이: %d개\n", length(completed_keys) - hr_plus_stat_plus_system))
    
    # System Failed가 겹치는지 확인
    hr_stat_overlap_count <- length(intersect(hr_keys, stat_failed_keys))
    hr_system_overlap_count <- length(intersect(hr_keys, system_failed_keys))
    stat_system_overlap_count <- length(intersect(stat_failed_keys, system_failed_keys))
    
    cat("\n=== 겹침 확인 ===\n")
    cat(sprintf("HR와 Stat Failed 겹침: %d개\n", hr_stat_overlap_count))
    cat(sprintf("HR와 System Failed 겹침: %d개\n", hr_system_overlap_count))
    cat(sprintf("Stat Failed와 System Failed 겹침: %d개\n", stat_system_overlap_count))
    
    if (hr_plus_stat_plus_system == length(completed_keys) && 
        hr_stat_overlap_count == 0 && hr_system_overlap_count == 0 && stat_system_overlap_count == 0) {
        cat("\n✓ 완벽하게 일치합니다!\n")
        cat("  Completed Jobs = HR 결과 + Stat Failed + System Failed (서로소 집합)\n")
    } else {
        cat("\n⚠ 차이가 있거나 겹침이 있습니다. 추가 조사가 필요합니다.\n")
    }
    
} else {
    cat("Stat Failed 또는 Completed Jobs 데이터가 없어 세 집합 비교를 수행할 수 없습니다.\n")
    if (length(stat_failed_keys) == 0 && length(completed_keys) == 0) {
        cat(sprintf("HR 결과만 존재: %d개 쌍\n", length(hr_keys)))
    }
}

cat("\n=== 분석 완료 ===\n")
