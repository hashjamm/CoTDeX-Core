#!/bin/bash
# ============================================================================
# HR-RR Mapping Validation 자동 재시작 래퍼 스크립트
# 
# [실행 방법]
#   1. sudoers 설정 (한 번만, 완전 자동화를 위해 필수):
#      echo "hashjamm ALL=(ALL) NOPASSWD: /usr/bin/systemd-run" | sudo tee /etc/sudoers.d/systemd-run
#      sudo chmod 0440 /etc/sudoers.d/systemd-run
#   
#   2. 스크립트 실행 (fu 파라미터 불필요, 내부적으로 fu=1 사용):
#      ./hr_rr_mapping_validation_manager_v2.sh
#   
#   3. 백그라운드 실행 (프로그래스 바 확인 가능):
#      # tmux 세션에서 실행 (권장)
#      tmux new-session -d -s validation './hr_rr_mapping_validation_manager_v2.sh'
#      
#      # 프로그래스 바 확인 (다른 터미널에서)
#      tmux attach-session -t validation
#      
#      # tmux 세션에서 나가기 (프로세스는 계속 실행): Ctrl+B, D
#      # 세션 종료: tmux kill-session -t validation
#   
#   4. nohup 백그라운드 실행 (프로그래스 바 확인 불가):
#      nohup ./hr_rr_mapping_validation_manager_v2.sh > /dev/null 2>&1 &
#      # 프로그래스 바는 로그 파일에서만 확인 가능 (tail -f)
# 
# [필수 패키지]
#   conda install -c conda-forge duckdb
# ============================================================================

# 설정
CONDA_ENV="disease_network_data"  # Conda 가상환경 이름
R_SCRIPT="/home/hashjamm/codes/disease_network/hr_rr_mapping_validation_engine_v3.R"
MAX_RESTARTS=100
RESTART_COUNT=0
RESTART_DELAY=60  # 재시작 간 대기 시간 (초)

# fu 파라미터 설정 (내부적으로 고정값 1 사용)
# - 결과가 fu 값과 무관하므로 고정값 사용
# - 코드 내부에서 diff 계산에 필요하지만, 결과는 fu 값과 무관함
FU=1
export FU  # R 스크립트에서 사용할 수 있도록 export

# 전역 변수: 현재 실행 중인 R 프로세스 PID (cleanup에서 사용)
CURRENT_R_PID=""

# 전역 변수: 실행 시작 시점의 완료 작업 수 (DB에서 조회)
START_COMPLETED_JOBS=0

# 전역 변수: 실행 시작 시점의 타임스탬프 (ETA 계산용)
START_TIME=0

# 로그 파일 설정 (고정 경로 사용)
LOG_DIR="/home/hashjamm/results/disease_network"
SCRIPT_BASENAME=$(basename "$0" .sh)
LOG_FILE="$LOG_DIR/${SCRIPT_BASENAME}_history.log"
export LOG_FILE  # 서브셸에서 사용할 수 있도록 export
export LOG_DIR   # 서브셸에서 사용할 수 있도록 export
export SCRIPT_BASENAME  # 서브셸에서 사용할 수 있도록 export

# 진행 상황 추적 관련 설정 (고정 경로 사용)
DB_COMPLETED_FILE="/home/hashjamm/results/disease_network/hr_rr_mapping_validation_job_queue_db/completed_jobs.duckdb"
DB_COMPLETED_FOLDER="/home/hashjamm/results/disease_network/hr_rr_mapping_validation_job_queue_db/completed_jobs/"
export DB_COMPLETED_FILE  # 서브셸에서 사용할 수 있도록 export
export DB_COMPLETED_FOLDER  # 서브셸에서 사용할 수 있도록 export

# 함수: 로그 출력 (터미널과 파일 모두)
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

# 함수: 완료된 작업 수 계산 (중앙 DB + 청크 파일)
calculate_completed_jobs() {
    local completed_jobs=0
    
    # conda 환경의 duckdb 경로 확인 (서브셸에서 PATH 상속 문제 방지)
    local duckdb_cmd="duckdb"
    if [ ! -z "$CONDA_ENV_PATH" ] && [ -f "$CONDA_ENV_PATH/bin/duckdb" ]; then
        duckdb_cmd="$CONDA_ENV_PATH/bin/duckdb"
    fi
    
    # 1. 중앙 DB에서 완료 작업 수
    if [ -f "$DB_COMPLETED_FILE" ]; then
        local result=$(echo -e ".mode csv\n.header off\nSELECT COUNT(*) FROM jobs;" | $duckdb_cmd --readonly "$DB_COMPLETED_FILE" 2>/dev/null | head -1 | sed 's/[^0-9]//g')
        if [ ! -z "$result" ] && [[ "$result" =~ ^[0-9]+$ ]]; then
            completed_jobs=$result
        fi
    fi
    
    # 2. 청크 파일들에서 완료 작업 수 합산
    if [ -d "$DB_COMPLETED_FOLDER" ]; then
        # process substitution 사용하여 서브셸 문제 방지
        while IFS= read -r chunk_file; do
            if [ -f "$chunk_file" ]; then
                local chunk_count=$(echo -e ".mode csv\n.header off\nSELECT COUNT(*) FROM jobs;" | $duckdb_cmd --readonly "$chunk_file" 2>/dev/null | head -1 | sed 's/[^0-9]//g')
                if [ ! -z "$chunk_count" ] && [[ "$chunk_count" =~ ^[0-9]+$ ]]; then
                    completed_jobs=$((completed_jobs + chunk_count))
                fi
            fi
        done < <(find "$DB_COMPLETED_FOLDER" -name "completed_chunk_*.duckdb" 2>/dev/null)
    fi
    
    echo "$completed_jobs"
}

# 함수: 로그에서 특정 배치 번호까지의 작업 수 합계 계산
calculate_previous_batch_jobs() {
    local log_file=$1
    local up_to_batch=$2
    
    if [ -z "$up_to_batch" ] || [ "$up_to_batch" -le 1 ]; then
        echo "0"
        return 0
    fi
    
    local previous_jobs=0
    local batch_num=1
    
    # 로그에서 각 배치의 작업 수를 순차적으로 추출
    while IFS= read -r line; do
        if [ "$batch_num" -lt "$up_to_batch" ]; then
            # "[배치 X/Y] 작은 청크 기반 할당: N개 작업" 형식에서 작업 수 추출
            local job_count=$(echo "$line" | grep -o "[0-9]*개 작업" | grep -o "[0-9]*" | head -1)
            if [ ! -z "$job_count" ] && [[ "$job_count" =~ ^[0-9]+$ ]]; then
                previous_jobs=$((previous_jobs + job_count))
            fi
            batch_num=$((batch_num + 1))
        else
            break
        fi
    done < <(grep "\[배치 [0-9]*/[0-9]*\] 작은 청크 기반 할당: [0-9]*개 작업" "$log_file" 2>/dev/null)
    
    echo "$previous_jobs"
}

# 함수: 시스템 리소스 정보 수집
get_system_resources() {
    # CPU 사용률 (전체)
    local cpu_usage=$(top -bn1 | grep "Cpu(s)" | sed "s/.*, *\([0-9.]*\)%* id.*/\1/" | awk '{print 100 - $1}')
    if [ -z "$cpu_usage" ]; then
        cpu_usage=$(grep 'cpu ' /proc/stat | awk '{usage=($2+$4)*100/($2+$3+$4+$5)} END {print usage}')
    fi
    cpu_usage=${cpu_usage:-0}
    cpu_usage=$(printf "%.1f" "$cpu_usage" 2>/dev/null || echo "0")
    
    # 메모리 정보
    local mem_info=$(free -h | grep "^Mem:")
    local mem_total=$(echo "$mem_info" | awk '{print $2}')
    local mem_used=$(echo "$mem_info" | awk '{print $3}')
    local mem_percent=$(free | grep "^Mem:" | awk '{printf "%.1f", $3/$2 * 100}')
    
    # 스왑 정보
    local swap_info=$(free -h | grep "^Swap:")
    local swap_total=$(echo "$swap_info" | awk '{print $2}')
    local swap_used=$(echo "$swap_info" | awk '{print $3}')
    local swap_percent=$(free | grep "^Swap:" | awk '{if ($2 == "0") print "0"; else printf "%.1f", $3/$2 * 100}')
    
    # Load average
    local load_avg=$(uptime | awk -F'load average:' '{print $2}' | awk '{print $1}' | sed 's/,//')
    load_avg=${load_avg:-0.00}
    
    # R 프로세스 메모리 사용량 (hr_rr_mapping_validation_engine.R 메인 프로세스만)
    local r_mem_kb=$(ps aux | grep -E "hr_rr_mapping_validation_engine.R" | grep -v "worker.rank" | grep -v grep | awk '{sum+=$6} END {print sum}')
    local r_mem_gb="0.0"
    if [ ! -z "$r_mem_kb" ] && [ "$r_mem_kb" -gt 0 ]; then
        # bc가 없어도 작동하도록 awk로 계산
        r_mem_gb=$(awk "BEGIN {printf \"%.2f\", $r_mem_kb / 1024 / 1024}" 2>/dev/null || echo "0.0")
    fi
    
    echo "CPU:${cpu_usage}%|MEM:${mem_used}/${mem_total}(${mem_percent}%)|Swap:${swap_used}/${swap_total}(${swap_percent}%)|Load:${load_avg}|R_MEM:${r_mem_gb}GB"
}

# 함수: ETA 계산
calculate_eta() {
    local new_completed_jobs=$1
    local total_jobs=$2
    
    if [ -z "$START_TIME" ] || [ "$START_TIME" -eq 0 ]; then
        echo "N/A"
        return
    fi
    
    if [ "$new_completed_jobs" -le 0 ] || [ "$total_jobs" -le 0 ]; then
        echo "N/A"
        return
    fi
    
    local current_time=$(date +%s)
    local elapsed=$((current_time - START_TIME))
    
    if [ "$elapsed" -le 0 ]; then
        echo "N/A"
        return
    fi
    
    # 작업 속도 (작업/초) - bc 없이 작동하도록 awk 사용
    local rate=$(awk "BEGIN {printf \"%.4f\", $new_completed_jobs / $elapsed}" 2>/dev/null)
    if [ -z "$rate" ] || [ "$(awk "BEGIN {print ($rate <= 0)}" 2>/dev/null)" = "1" ]; then
        echo "N/A"
        return
    fi
    
    # 남은 작업 수
    local remaining=$((total_jobs - new_completed_jobs))
    if [ "$remaining" -le 0 ]; then
        echo "완료"
        return
    fi
    
    # 예상 남은 시간 (초) - bc 없이 작동하도록 awk 사용
    local eta_seconds=$(awk "BEGIN {printf \"%.0f\", $remaining / $rate}" 2>/dev/null)
    if [ -z "$eta_seconds" ] || [ "$eta_seconds" -le 0 ]; then
        echo "N/A"
        return
    fi
    
    # 시간 포맷팅 (일시분초)
    local days=$((eta_seconds / 86400))
    local hours=$(((eta_seconds % 86400) / 3600))
    local minutes=$(((eta_seconds % 3600) / 60))
    local seconds=$((eta_seconds % 60))
    
    if [ "$days" -gt 0 ]; then
        echo "${days}일 ${hours}시간"
    elif [ "$hours" -gt 0 ]; then
        echo "${hours}시간 ${minutes}분"
    elif [ "$minutes" -gt 0 ]; then
        echo "${minutes}분 ${seconds}초"
    else
        echo "${seconds}초"
    fi
}

# 전역 변수: 프로그래스 바 초기화 여부
PROGRESS_BAR_INITIALIZED=0

# 함수: 상세 진행 상황 표시 (배치 레벨 + 배치 내부)
show_detailed_progress() {
    local total_jobs=$1
    local log_file=$2
    
    if [ -z "$total_jobs" ] || [ "$total_jobs" -eq 0 ]; then
        return 0
    fi
    
    # 처음 호출 시 한 줄의 공간을 확보하고 커서를 첫 줄로 올리기
    if [ "$PROGRESS_BAR_INITIALIZED" -eq 0 ]; then
        # 한 줄 확보
        printf "\n"
        # 한 줄 위로 올라가서 첫 줄로 복귀
        printf "\033[1A\r"
        PROGRESS_BAR_INITIALIZED=1
    fi
    
    # 완료된 작업 수 계산 (전체 완료 수)
    local completed_jobs=$(calculate_completed_jobs)
    
    # 실행 시작 시점의 완료 작업 수 사용 (DB에서 조회한 값)
    # START_COMPLETED_JOBS가 설정되지 않았으면 0으로 처리
    local pre_completed=${START_COMPLETED_JOBS:-0}
    
    # 남은 작업 중 새로 완료된 작업 수 계산
    # total_jobs는 이미 로그에서 "총 X개 작업 시작"으로 파싱된 남은 작업 수
    local new_completed_jobs=0
    if [ "$completed_jobs" -gt "$pre_completed" ]; then
        new_completed_jobs=$((completed_jobs - pre_completed))
    fi
    
    # 배치 정보 파싱
    local batch_info=$(parse_batch_info_from_log "$log_file")
    local total_batches_from_log=$(echo "$batch_info" | cut -d'|' -f1)  # 로그에서 파싱한 전체 배치 수
    local batch_job_count=$(echo "$batch_info" | cut -d'|' -f4)  # 현재 배치에 실제 할당된 작업 수
    
    # batch_size 가져오기 (배치당 최대 작업 수)
    local batch_size=0
    # 방법 1: 배치가 여러 개일 때 첫 번째 배치의 batch_job_count가 batch_size와 같을 가능성이 높음
    if [ ! -z "$total_batches_from_log" ] && [ "$total_batches_from_log" -gt 1 ]; then
        # 첫 번째 배치 정보 찾기 - 최신 실행의 로그만 읽기 (tail 사용)
        local first_batch_line=$(grep "\[배치 1/" "$log_file" 2>/dev/null | grep "작은 청크 기반 할당" | tail -1)
        if [ ! -z "$first_batch_line" ]; then
            batch_size=$(echo "$first_batch_line" | grep -o "[0-9]*개 작업" | grep -o "[0-9]*" | head -1)
        fi
    fi
    # 방법 2: 배치가 1개이거나 추정 실패 시 R 스크립트에서 파싱
    if [ -z "$batch_size" ] || [ "$batch_size" -eq 0 ]; then
        batch_size=$(parse_batch_size_from_r_script "$R_SCRIPT")
    fi
    
    # 전체 배치 수 계산 (전체 작업 수 기준으로 계산)
    # total_batches_from_log는 "남은 작업 수" 기준이므로, 전체 작업 수 기준으로 재계산 필요
    local total_batches=0
    if [ "$batch_size" -gt 0 ]; then
        # 전체 작업 수 = 시작 시점 완료 작업 수 + 남은 작업 수
        local total_all_jobs=$((pre_completed + total_jobs))
        if [ "$total_all_jobs" -gt 0 ]; then
            total_batches=$((total_all_jobs / batch_size))
            if [ $((total_all_jobs % batch_size)) -gt 0 ]; then
                total_batches=$((total_batches + 1))
            fi
        fi
    fi
    
    # 전체 완료 작업 수 기준으로 완료된 배치 수 계산 (batch_size 사용)
    # 배치 번호는 전체 완료 작업 수 기준으로 계산 (new_completed_jobs가 아닌 completed_jobs 사용)
    local completed_batches=0
    if [ "$batch_size" -gt 0 ] && [ "$completed_jobs" -gt 0 ]; then
        completed_batches=$((completed_jobs / batch_size))
    fi
    
    # 현재 배치 번호 = 완료된 배치 수 (0부터 시작)
    local current_batch=$completed_batches
    
    # 전체 진행률 계산 (남은 작업 기준)
    local overall_pct=0
    if [ "$total_jobs" -gt 0 ]; then
        overall_pct=$((new_completed_jobs * 100 / total_jobs))
    fi
    
    # 배치 정보가 있으면 상세 프로그래스 바 표시
    if [ ! -z "$total_batches" ] && [ "$total_batches" -gt 0 ] && [ "$batch_size" -gt 0 ]; then
        # 배치 레벨 프로그래스 바 (남은 작업 기준)
        local batch_bar_width=10
        local batch_pct=0
        if [ "$total_batches" -gt 0 ]; then
            batch_pct=$((completed_batches * 100 / total_batches))
            if [ "$batch_pct" -gt 100 ]; then batch_pct=100; fi
        fi
        
        local batch_filled=$((batch_pct * batch_bar_width / 100))
        if [ "$batch_filled" -gt "$batch_bar_width" ]; then batch_filled=$batch_bar_width; fi
        local batch_bar=""
        for ((i=0; i<batch_filled; i++)); do batch_bar="${batch_bar}="; done
        for ((i=batch_filled; i<batch_bar_width; i++)); do batch_bar="${batch_bar}-"; done
        
        # 현재 배치 내부 진행률 계산
        # [전체] 부분: 전체 완료 작업 수(completed_jobs) 기준
        # [현재 배치] 부분: 이번 실행에서 새로 완료된 작업 수(new_completed_jobs) 기준
        # 
        # [현재 배치]는 단순히 이번 실행에서 새로 완료된 작업 수를 표시
        # 예: 재시작 시점에 794,238개 완료, 지금 803,968개 완료 → [현재 배치] 작업 9,730/100,000
        local batch_internal_completed=$new_completed_jobs
        
        # 배치 내부 진행률 계산
        local batch_internal_pct=0
        local batch_internal_bar_width=20
        if [ ! -z "$batch_job_count" ] && [ "$batch_job_count" -gt 0 ]; then
            batch_internal_pct=$((batch_internal_completed * 100 / batch_job_count))
            if [ "$batch_internal_pct" -gt 100 ]; then batch_internal_pct=100; fi
            # 배치 내부 완료 작업 수도 100%를 넘지 않도록 제한
            if [ "$batch_internal_completed" -gt "$batch_job_count" ]; then
                batch_internal_completed=$batch_job_count
            fi
        fi
        
        local batch_internal_filled=$((batch_internal_pct * batch_internal_bar_width / 100))
        if [ "$batch_internal_filled" -gt "$batch_internal_bar_width" ]; then batch_internal_filled=$batch_internal_bar_width; fi
        local batch_internal_bar=""
        for ((i=0; i<batch_internal_filled; i++)); do batch_internal_bar="${batch_internal_bar}="; done
        for ((i=batch_internal_filled; i<batch_internal_bar_width; i++)); do batch_internal_bar="${batch_internal_bar}-"; done
        
        # 상세 프로그래스 바 출력 (남은 작업 기준) - 첫 번째 줄 내용 준비
        # [전체] 배치 부분의 %는 배치 레벨 진행률(batch_pct) 사용
        local first_line="[전체] 배치 $current_batch/$total_batches [$batch_bar] $batch_pct% | [현재 배치] 작업 $batch_internal_completed/$batch_job_count [$batch_internal_bar] $batch_internal_pct%"
    else
        # 배치 정보가 없으면 기존 방식으로 표시 (남은 작업 기준) - 첫 번째 줄 내용 준비
        local progress_pct=$overall_pct
        local bar_width=20
        local filled=$((new_completed_jobs * bar_width / total_jobs))
        if [ "$filled" -gt "$bar_width" ]; then filled=$bar_width; fi
        local bar=""
        for ((i=0; i<filled; i++)); do bar="${bar}="; done
        for ((i=filled; i<bar_width; i++)); do bar="${bar}-"; done
        
        local first_line="[작업 진행] [$bar] $new_completed_jobs/$total_jobs 완료 ($progress_pct%)"
    fi
    
    # 리소스 정보 및 ETA를 두 번째 줄로 출력
    local resources=$(get_system_resources)
    local cpu_usage=$(echo "$resources" | cut -d'|' -f1 | cut -d':' -f2)
    local mem_info=$(echo "$resources" | cut -d'|' -f2 | cut -d':' -f2)
    local swap_info=$(echo "$resources" | cut -d'|' -f3 | cut -d':' -f2)
    local load_avg=$(echo "$resources" | cut -d'|' -f4 | cut -d':' -f2)
    local r_mem=$(echo "$resources" | cut -d'|' -f5 | cut -d':' -f2)
    
    local eta=$(calculate_eta "$new_completed_jobs" "$total_jobs")
    local second_line="[리소스] CPU: $cpu_usage | MEM: $mem_info | Swap: $swap_info | Load: $load_avg | R_MEM: $r_mem | ETA: $eta"
    
    # 한 줄에 모든 정보를 합쳐서 출력 (백그라운드 서브셸에서도 작동하도록)
    # \r로 줄 시작으로 이동 후 현재 줄을 지우고(\033[K) 한 줄로 출력
    # /dev/tty로 리다이렉션되어 있으므로 명시적으로 /dev/tty로 출력
    local combined_line="${first_line} | ${second_line}"
    if [ -c /dev/tty ] 2>/dev/null; then
        printf "\r\033[K%s" "$combined_line" > /dev/tty 2>&1
    else
        printf "\r\033[K%s" "$combined_line"
    fi
}

# 함수: 진행 상황 표시 (작업 단위) - 기존 호환성 유지
show_job_progress() {
    local total_jobs=$1
    local log_file="${2:-$LOG_FILE}"
    
    if [ -z "$total_jobs" ] || [ "$total_jobs" -eq 0 ]; then
        return 0
    fi
    
    # 상세 프로그래스 바 사용
    show_detailed_progress "$total_jobs" "$log_file"
}

# 함수: 로그에서 전체 작업 수 파싱
parse_total_jobs_from_log() {
    local log_file=$1
    # R 스크립트 출력: "전체 %d개 중, 기완료 %d개 제외, 총 %d개 작업 시작"
    # "총" 다음 숫자를 추출 (최신 실행 기록 사용)
    grep -o "총 [0-9]*개 작업 시작" "$log_file" 2>/dev/null | tail -1 | grep -o "[0-9]*" | head -1
}

# 함수: 로그에서 배치 정보 파싱
parse_batch_info_from_log() {
    local log_file=$1
    
    # 전체 배치 수 파싱: "--- 총 %d개 배치 처리 시작" - 최신 실행의 로그만 읽기 (tail 사용)
    local total_batches=$(grep -o "총 [0-9]*개 배치 처리 시작" "$log_file" 2>/dev/null | grep -o "[0-9]*" | tail -1)
    
    # 현재 배치 정보 파싱: "[배치 %d/%d] 작은 청크 기반 할당: %d개 작업"
    local batch_line=$(grep "\[배치 [0-9]*/[0-9]*\] 작은 청크 기반 할당" "$log_file" 2>/dev/null | tail -1)
    
    local current_batch=""
    local batch_total=""
    local batch_job_count=""
    
    if [ ! -z "$batch_line" ]; then
        # 배치 번호 추출: "[배치 3/10]" -> "3"
        current_batch=$(echo "$batch_line" | grep -o "\[배치 [0-9]*" | grep -o "[0-9]*")
        # 전체 배치 수 추출: "[배치 3/10]" -> "10"
        batch_total=$(echo "$batch_line" | grep -o "/[0-9]*\]" | grep -o "[0-9]*")
        # 배치 내 작업 수 추출: "%d개 작업 →" -> 숫자
        batch_job_count=$(echo "$batch_line" | grep -o "[0-9]*개 작업" | grep -o "[0-9]*" | head -1)
    fi
    
    # 결과 출력: "total_batches|current_batch|batch_total|batch_job_count"
    echo "${total_batches:-}|${current_batch:-}|${batch_total:-}|${batch_job_count:-}"
}

# 함수: R 스크립트에서 batch_size 파싱
parse_batch_size_from_r_script() {
    local r_script=$1
    # "batch_size = [숫자]" 패턴 찾기 - 마지막 매칭(실제 사용값)을 가져오기
    # head -1은 함수 정의의 기본값(500)을 가져오지만, tail -1은 main() 호출 시 실제 사용값(100000)을 가져옴
    local batch_size=$(grep -o "batch_size[[:space:]]*=[[:space:]]*[0-9]*" "$r_script" 2>/dev/null | grep -o "[0-9]*" | tail -1)
    echo "${batch_size:-100000}"  # 기본값 100000
}

# 함수: 정상 종료 처리
cleanup() {
    log ""
    log "===================== SCRIPT INTERRUPTED ====================="
    log "스크립트 종료 요청 수신 (Ctrl+C 또는 종료 신호)"
    
    # 방법 1: 저장된 PID가 있으면 우선 사용 (Rscript가 실제 R 프로세스의 부모일 수 있음)
    if [ ! -z "$CURRENT_R_PID" ]; then
        # Rscript의 자식 프로세스(실제 R 프로세스) 찾기
        R_PID=$(pgrep -P $CURRENT_R_PID | head -1)
        if [ ! -z "$R_PID" ]; then
            log "저장된 PID ($CURRENT_R_PID)의 자식 프로세스 (PID: $R_PID) 종료 중..."
            kill $R_PID 2>/dev/null
        fi
        # Rscript 프로세스도 종료 시도
        if kill -0 $CURRENT_R_PID 2>/dev/null; then
            kill $CURRENT_R_PID 2>/dev/null
        fi
    fi
    
    # 방법 2: 프로세스 패턴으로 직접 찾기 (실제 R 프로세스: R --file=hr_rr_mapping_validation_engine.R)
    R_PID=$(pgrep -f "hr_rr_mapping_validation_engine.R" | grep -v "worker.rank" | head -1)
    if [ ! -z "$R_PID" ] && kill -0 $R_PID 2>/dev/null; then
        log "실행 중인 메인 R 프로세스 (PID: $R_PID) 종료 중..."
        # 메인 프로세스 종료 시 자식 프로세스(future 워커)도 함께 종료됨
        # SIGTERM으로 정상 종료 시도 (트랜잭션 롤백 및 정리 작업 시간 확보)
        kill -TERM $R_PID 2>/dev/null
        sleep 5  # 배치 처리 중단 및 트랜잭션 롤백 시간 확보
        # 정상 종료 확인
        if kill -0 $R_PID 2>/dev/null; then
            log "정상 종료 실패, 강제 종료 중..."
            kill -9 $R_PID 2>/dev/null
        else
            log "메인 R 프로세스 정상 종료 완료"
        fi
    fi
    
    # systemd-run scope 종료 (systemd-run으로 실행된 경우)
    if [ ! -z "$RSCRIPT_PID" ]; then
        # systemd-run scope 찾기
        SCOPE_NAME=$(systemctl list-units --type=scope --state=running 2>/dev/null | grep -E "validation|run-r[0-9a-f]" | awk '{print $1}' | head -1)
        if [ ! -z "$SCOPE_NAME" ]; then
            log "systemd scope ($SCOPE_NAME) 종료 중..."
            sudo systemctl stop "$SCOPE_NAME" 2>/dev/null || true
            sleep 2
        fi
    fi
    
    # future 워커 프로세스 확인 및 종료 (메인 프로세스가 종료되지 않은 경우 대비)
    WORKER_COUNT=$(pgrep -f "worker.rank.*parallelly.parent" | wc -l)
    if [ "$WORKER_COUNT" -gt 0 ]; then
        log "future 워커 프로세스 $WORKER_COUNT개 발견, 종료 중..."
        pkill -f "worker.rank.*parallelly.parent" 2>/dev/null
        sleep 1
        # 강제 종료
        pkill -9 -f "worker.rank.*parallelly.parent" 2>/dev/null
    fi
    
    # systemd-run 프로세스 종료
    if [ ! -z "$RSCRIPT_PID" ] && kill -0 $RSCRIPT_PID 2>/dev/null; then
        log "systemd-run 프로세스 (PID: $RSCRIPT_PID) 종료 중..."
        kill -TERM $RSCRIPT_PID 2>/dev/null
        sleep 2
        if kill -0 $RSCRIPT_PID 2>/dev/null; then
            kill -9 $RSCRIPT_PID 2>/dev/null
        fi
    fi
    
    # R 프로세스 그룹 전체 종료 (최종 안전장치)
    pkill -P $$ -f "hr_rr_mapping_validation_engine.R" 2>/dev/null
    
    # systemd scope 재확인 및 강제 종료 (최종)
    SCOPE_NAME=$(systemctl list-units --type=scope --state=running 2>/dev/null | grep -E "validation|run-r[0-9a-f]" | awk '{print $1}' | head -1)
    if [ ! -z "$SCOPE_NAME" ]; then
        log "남아있는 systemd scope ($SCOPE_NAME) 강제 종료 중..."
        sudo systemctl kill -s KILL "$SCOPE_NAME" 2>/dev/null || true
    fi
    
    log "로그 파일: $LOG_FILE"
    log "=========================================================="
    exit 0
}

# 신호 처리 설정 (Ctrl+C, 종료 신호)
trap cleanup SIGINT SIGTERM

# Conda 환경 활성화
if [ -z "$CONDA_DEFAULT_ENV" ] || [ "$CONDA_DEFAULT_ENV" != "$CONDA_ENV" ]; then
    # CONDA_BASE 찾기 (필수)
    if [ -z "$CONDA_BASE" ]; then
        CONDA_BASE=$(conda info --base 2>/dev/null)
        if [ -z "$CONDA_BASE" ]; then
            # conda 명령어를 찾을 수 없는 경우, 일반적인 conda 경로 시도
            if [ -d "$HOME/anaconda3" ]; then
                CONDA_BASE="$HOME/anaconda3"
            elif [ -d "$HOME/miniconda3" ]; then
                CONDA_BASE="$HOME/miniconda3"
            else
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: conda를 찾을 수 없습니다. conda가 설치되어 있고 PATH에 추가되어 있는지 확인하세요." >&2
                exit 1
            fi
        fi
    fi
    
    # conda 초기화 (아직 초기화되지 않은 경우)
    if [ -z "$CONDA_SHLVL" ] && [ -f "$CONDA_BASE/etc/profile.d/conda.sh" ]; then
        source "$CONDA_BASE/etc/profile.d/conda.sh"
    fi
    
    # conda 환경 경로 확인 및 PATH 설정
    CONDA_ENV_PATH="$CONDA_BASE/envs/$CONDA_ENV"
    if [ ! -d "$CONDA_ENV_PATH" ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: conda 환경 '$CONDA_ENV'의 경로를 찾을 수 없습니다: $CONDA_ENV_PATH" >&2
        exit 1
    fi
    
    # 환경의 bin 경로를 PATH 앞에 추가
    export PATH="$CONDA_ENV_PATH/bin:$PATH"
    export CONDA_DEFAULT_ENV="$CONDA_ENV"
    export CONDA_PREFIX="$CONDA_ENV_PATH"
fi

# 시작 메시지
log "=========================================================="
log "HR-RR Mapping Validation 자동 재시작 래퍼 시작"
log "=========================================================="
log "Conda 환경: ${CONDA_DEFAULT_ENV:-$CONDA_ENV}"
log "R 스크립트: $R_SCRIPT"
log "로그 파일: $LOG_FILE"
log "최대 재시작 횟수: $MAX_RESTARTS"
log "재시작 간 대기 시간: ${RESTART_DELAY}초"
log "=========================================================="
log ""

# 로그 디렉토리 존재 확인
if [ ! -d "$LOG_DIR" ]; then
    # tee를 사용하지 않고 직접 에러 출력 후 종료
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: 로그 디렉토리를 찾을 수 없습니다: $LOG_DIR" >&2
    exit 1
fi

# R 스크립트 파일 존재 확인
if [ ! -f "$R_SCRIPT" ]; then
    log "ERROR: R 스크립트를 찾을 수 없습니다: $R_SCRIPT"
    exit 1
fi

# sudo 권한 확인 (sudoers 설정이 되어 있으면 비밀번호 없이 실행 가능)
# sudoers에 NOPASSWD 설정이 되어 있지 않으면 비밀번호 입력이 필요합니다
if ! sudo -n true 2>/dev/null; then
    log "sudo 권한 확인 중... (비밀번호 입력이 필요할 수 있습니다)"
    if ! sudo -v; then
        log "ERROR: sudo 권한 확인 실패. 시스템 관리자 권한이 필요합니다."
        log "완전 자동화를 위해 sudoers에 NOPASSWD 설정을 권장합니다."
        exit 1
    fi
    log "sudo 권한 확인 완료 (타임스탬프 활성화)"
else
    log "sudo 권한 확인 완료 (NOPASSWD 설정됨)"
fi

# 메인 루프
while [ $RESTART_COUNT -lt $MAX_RESTARTS ]; do
    log ""
    log "=========================================================="
    log "사이클 시작: 재시작 횟수 $RESTART_COUNT / $MAX_RESTARTS"
    log "=========================================================="
    
    # 실행 시작 시점의 완료 작업 수를 DB에서 조회하여 저장
    if [ "$RESTART_COUNT" -eq 0 ]; then
        # 첫 실행 시에만 조회 (재시작 시에는 유지)
        START_COMPLETED_JOBS=$(calculate_completed_jobs)
        START_TIME=$(date +%s)  # ETA 계산용 시작 시간
        log "실행 시작 시점 완료 작업 수: $START_COMPLETED_JOBS"
        log "실행 시작 시간: $(date -d "@$START_TIME" '+%Y-%m-%d %H:%M:%S')"
    fi
    
    # R 스크립트 실행 (메모리 제한 적용: 100G)
    log "R 스크립트 실행 중... (메모리 제한: 100G)"
    # systemd-run을 사용하여 메모리 제한 설정 (시스템 다운 방지)
    # sudo를 사용하여 시스템 레벨로 실행 (비밀번호 없이 실행하려면 sudoers 설정 필요)
    # conda 환경의 Rscript를 직접 사용하여 환경 문제 해결
    # Rscript를 백그라운드로 실행하고 출력을 로그 파일로만 리다이렉션 (터미널은 sh 프로그레스 바만 표시)
    # 주의: sudoers에 NOPASSWD 설정이 필요합니다 (아래 주석 참조)
    # fu 파라미터는 환경 변수로 전달 (R 스크립트에서 Sys.getenv("FU")로 읽을 수 있음)
    RSCRIPT_CMD="$CONDA_ENV_PATH/bin/Rscript"
    if [ ! -f "$RSCRIPT_CMD" ]; then
        log "ERROR: Rscript를 찾을 수 없습니다: $RSCRIPT_CMD"
        exit 1
    fi
    sudo systemd-run --scope -p MemoryMax=100G -E FU="$FU" "$RSCRIPT_CMD" "$R_SCRIPT" >> "$LOG_FILE" 2>&1 &
    RSCRIPT_PID=$!
    # systemd-run은 wrapper 프로세스이므로, 실제 R 프로세스를 찾아야 함
    CURRENT_R_PID=$RSCRIPT_PID  # 일단 wrapper PID 저장, 아래에서 실제 PID 찾기
    
    # 실제 R 프로세스 PID 찾기 (systemd-run scope 내에서 실행되므로 프로세스 패턴으로 찾기)
    sleep 1  # systemd scope 생성 및 R 프로세스가 시작될 시간 확보
    # systemd-run scope 내에서 실행되므로, 프로세스 패턴으로 직접 찾기
    ACTUAL_R_PID=$(pgrep -f "hr_rr_mapping_validation_engine.R" | grep -v "worker.rank" | head -1)
    if [ ! -z "$ACTUAL_R_PID" ]; then
        CURRENT_R_PID=$ACTUAL_R_PID  # 실제 R 프로세스 PID로 업데이트
    else
        # 프로세스 패턴으로 찾지 못한 경우, systemd-run의 자식 프로세스 확인
        ACTUAL_R_PID=$(pgrep -P $RSCRIPT_PID | head -1)
        if [ ! -z "$ACTUAL_R_PID" ]; then
            CURRENT_R_PID=$ACTUAL_R_PID  # 대체 방법으로 PID 업데이트
        fi
    fi
    
    # [신규] 진행 상황 추적 시작
    TOTAL_JOBS=0
    PROGRESS_TRACK_PID=""
    
    # R 스크립트 실행 중 진행 상황 모니터링 (백그라운드)
    (
        # stdout/stderr를 명시적으로 터미널로 리다이렉션 (프로그레스 바 출력용)
        # 백그라운드 서브셸에서도 작동하도록 조건 완화 ([ -t 1 ] 제거)
        if [ -c /dev/tty ] 2>/dev/null; then
            exec > /dev/tty 2>&1
        fi
        
        # TERM 환경 변수 확인 및 설정 (dumb 터미널 방지)
        if [ -z "$TERM" ] || [ "$TERM" = "dumb" ]; then
            # xterm-256color 또는 screen-256color로 설정 (tmux 호환)
            if [ -n "$TMUX" ]; then
                export TERM="screen-256color"
            else
                export TERM="xterm-256color"
            fi
        fi
        
        # 서브셸에서 변수 상속 확인 (필요시 재설정)
        # 변수들이 export되지 않았을 수 있으므로 재설정
        if [ -z "$DB_COMPLETED_FILE" ]; then
            DB_COMPLETED_FILE="/home/hashjamm/results/disease_network/hr_rr_mapping_validation_job_queue_db/completed_jobs.duckdb"
        fi
        if [ -z "$DB_COMPLETED_FOLDER" ]; then
            DB_COMPLETED_FOLDER="/home/hashjamm/results/disease_network/hr_rr_mapping_validation_job_queue_db/completed_jobs/"
        fi
        if [ -z "$LOG_FILE" ]; then
            LOG_DIR="/home/hashjamm/results/disease_network"
            SCRIPT_BASENAME="hr_rr_mapping_validation_manager_v2"
            LOG_FILE="$LOG_DIR/${SCRIPT_BASENAME}_history.log"
        fi
        if [ -z "$CONDA_ENV_PATH" ] && [ ! -z "$CONDA_BASE" ] && [ ! -z "$CONDA_ENV" ]; then
            CONDA_ENV_PATH="$CONDA_BASE/envs/$CONDA_ENV"
        fi
        # 재시작 시 START_COMPLETED_JOBS를 현재 완료 작업 수로 재설정
        # (재시작 시점의 완료 작업 수를 기준으로 진행률 계산)
        START_COMPLETED_JOBS=$(calculate_completed_jobs)
        
        # 전체 작업 수를 로그에서 파싱할 때까지 대기 (최대 30초)
        max_wait=30
        waited=0
        while [ $waited -lt $max_wait ]; do
            TOTAL_JOBS=$(parse_total_jobs_from_log "$LOG_FILE")
            if [ ! -z "$TOTAL_JOBS" ] && [ "$TOTAL_JOBS" -gt 0 ]; then
                break
            fi
            sleep 1
            waited=$((waited + 1))
        done
        
        # 전체 작업 수를 찾았으면 주기적으로 진행 상황 표시
        if [ ! -z "$TOTAL_JOBS" ] && [ "$TOTAL_JOBS" -gt 0 ]; then
            # R 프로세스가 실행 중인 동안 진행 상황 추적
            # systemd-run으로 실행된 경우 실제 R 프로세스를 직접 확인
            # 프로세스 패턴: hr_rr_mapping_validation_engine_v3.R 또는 hr_rr_mapping_validation_engine.R 모두 매칭
            while pgrep -f "hr_rr_mapping_validation_engine" | grep -v "worker.rank" | grep -v grep > /dev/null 2>&1; do
                # show_detailed_progress 함수가 자체적으로 한 줄을 지우고 출력하므로
                # 여기서는 추가적인 줄 지우기 없이 함수만 호출
                show_job_progress "$TOTAL_JOBS" "$LOG_FILE"
                # stdout 버퍼링 방지 (즉시 출력을 위해 flush)
                # /dev/tty로 리다이렉션되어 있으므로 명시적으로 flush
                printf "" > /dev/tty 2>/dev/null || true
                sleep 30  # 30초마다 업데이트 (IO 부담 감소)
            done
            # 마지막 한 번 더 표시 (100% 완료) 후 줄바꿈
            show_job_progress "$TOTAL_JOBS" "$LOG_FILE"
            # 한 줄을 모두 출력하고 줄바꿈
            echo ""  # 줄바꿈
        else
            # 로그 파싱 실패 시 기본 메시지 표시
            printf "[작업 진행] 전체 작업 수 확인 중... (작업은 정상 진행 중)\n"
        fi
    ) &
    PROGRESS_TRACK_PID=$!
    
    # R 프로세스 종료 대기 (Rscript PID를 기다리되, 실제로는 R 프로세스가 종료될 때까지 대기)
    wait $RSCRIPT_PID 2>/dev/null
    EXIT_CODE=$?
    
    # 진행 상황 추적 프로세스 종료 대기 (최대 1초)
    if [ ! -z "$PROGRESS_TRACK_PID" ]; then
        wait $PROGRESS_TRACK_PID 2>/dev/null
    fi
    
    log ""
    log "R 스크립트 종료 코드: $EXIT_CODE"
    
    # 종료 코드 확인
    if [[ $EXIT_CODE -eq 0 ]]; then
        # 정상 종료 (모든 작업 완료)
        log ""
        log "=========================================================="
        log "✓ 모든 작업이 완료되었습니다!"
        log "=========================================================="
        log "총 재시작 횟수: $RESTART_COUNT"
        log "로그 파일: $LOG_FILE"
        log "=========================================================="
        exit 0
    else
        # 재시작 필요 (남은 작업 있음 또는 예상치 못한 오류)
        # EXIT_CODE == 1: 정상적인 재시작 요청 (남은 작업 있음)
        # EXIT_CODE != 1: 예상치 못한 오류 (메모리 과부하, 시스템 오류 등)
        #                  → 재시작하여 작업 복구 시도
        RESTART_COUNT=$((RESTART_COUNT + 1))
        
        if [ $RESTART_COUNT -ge $MAX_RESTARTS ]; then
            log ""
            log "=========================================================="
            log "⚠️  최대 재시작 횟수($MAX_RESTARTS)에 도달했습니다."
            log "=========================================================="
            log "안전을 위해 자동 재시작을 중단합니다."
            log "로그 파일을 확인하여 남은 작업을 수동으로 확인하세요."
            log "로그 파일: $LOG_FILE"
            log "=========================================================="
            exit 1
        fi
        
        if [[ $EXIT_CODE -eq 1 ]]; then
            log "남은 작업이 있어 재시작이 필요합니다."
        else
            log "예상치 못한 오류 발생 (종료 코드: $EXIT_CODE) - 메모리 과부하 또는 시스템 오류 가능성"
            log "자동 재시작하여 작업 복구를 시도합니다."
        fi
        log "${RESTART_DELAY}초 후 자동 재시작... (Ctrl+C로 중단 가능)"
        sleep $RESTART_DELAY
    fi
done

# 최대 재시작 횟수 도달 (이 코드는 도달하지 않아야 함)
log "ERROR: 예상치 못한 루프 종료"
exit 1

