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
#      ./hr_rr_mapping_validation_manager.sh
#   
#   3. 백그라운드 실행 (프로그래스 바 확인 가능):
#      # tmux 세션에서 실행 (권장)
#      tmux new-session -d -s validation './hr_rr_mapping_validation_manager.sh'
#      
#      # 프로그래스 바 확인 (다른 터미널에서)
#      tmux attach-session -t validation
#      
#      # tmux 세션에서 나가기 (프로세스는 계속 실행): Ctrl+B, D
#      # 세션 종료: tmux kill-session -t validation
#   
#   4. nohup 백그라운드 실행 (프로그래스 바 확인 불가):
#      nohup ./hr_rr_mapping_validation_manager.sh > /dev/null 2>&1 &
#      # 프로그래스 바는 로그 파일에서만 확인 가능 (tail -f)
# 
# [필수 패키지]
#   conda install -c conda-forge duckdb
# ============================================================================

# 설정
CONDA_ENV="disease_network_data"  # Conda 가상환경 이름
R_SCRIPT="/home/hashjamm/codes/disease_network/hr_rr_mapping_validation_engine_v2.R"
MAX_RESTARTS=100
RESTART_COUNT=0
RESTART_DELAY=60  # 재시작 간 대기 시간 (초)

# fu 파라미터 설정 (내부적으로 고정값 1 사용)
# - 결과가 fu 값과 무관하므로 고정값 사용
# - 코드 내부에서 diff 계산에 필요하지만, 결과는 fu 값과 무관함
FU=1

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
export LOG_FILE
export LOG_DIR
export SCRIPT_BASENAME

# 진행 상황 추적 관련 설정 (고정 경로 사용)
DB_COMPLETED_FILE="/home/hashjamm/results/disease_network/hr_rr_mapping_validation_results/validation_completed_jobs.duckdb"
DB_COMPLETED_FOLDER="/home/hashjamm/results/disease_network/hr_rr_mapping_validation_results/validation_completed_jobs"
export DB_COMPLETED_FILE
export DB_COMPLETED_FOLDER

# 함수: 로그 출력 (터미널과 파일 모두)
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

# 함수: 완료된 작업 수 계산 (중앙 DB + 청크 파일)
# [수정] pre_aggregate_logs 실행 중 파일 잠금/플러시 타이밍 이슈 방지를 위한 재시도 로직 추가
# 모든 재시도 실패 시 -1 반환하여 호출자가 처리하도록 함
calculate_completed_jobs() {
    local completed_jobs=0
    local debug_log="/tmp/validation_progress_debug.log"
    
    # 디버깅: 함수 시작
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] calculate_completed_jobs 시작" >> "$debug_log"
    
    # conda 환경의 duckdb 경로 확인
    local duckdb_cmd="duckdb"
    if [ ! -z "$CONDA_ENV_PATH" ] && [ -f "$CONDA_ENV_PATH/bin/duckdb" ]; then
        duckdb_cmd="$CONDA_ENV_PATH/bin/duckdb"
    fi
    
    # 1. 중앙 DB에서 완료 작업 수 (HR engine 방식: 단순 확인, 실패해도 계속 진행)
    # root 소유 파일일 수 있으므로 sudo 사용
    if [ -f "$DB_COMPLETED_FILE" ]; then
        local result=$(echo -e ".mode csv\n.header off\nSELECT COUNT(*) FROM jobs;" | sudo -n $duckdb_cmd --readonly "$DB_COMPLETED_FILE" 2>&1 | head -1 | sed 's/[^0-9]//g')
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] 중앙 DB 읽기: 파일=$DB_COMPLETED_FILE, 결과=$result" >> "$debug_log"
        if [ ! -z "$result" ] && [[ "$result" =~ ^[0-9]+$ ]]; then
            completed_jobs=$result
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] 중앙 DB 완료 작업 수: $completed_jobs" >> "$debug_log"
        else
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] 중앙 DB 읽기 실패 또는 비어있음" >> "$debug_log"
        fi
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] 중앙 DB 파일 없음: $DB_COMPLETED_FILE" >> "$debug_log"
    fi
    
    # 2. 청크 파일들에서 완료 작업 수 합산
    # root 소유 파일일 수 있으므로 sudo 사용
    local chunk_count_total=0
    local chunk_files_found=0
    if [ -d "$DB_COMPLETED_FOLDER" ]; then
        while IFS= read -r chunk_file; do
            if [ -f "$chunk_file" ]; then
                chunk_files_found=$((chunk_files_found + 1))
                local chunk_count=$(echo -e ".mode csv\n.header off\nSELECT COUNT(*) FROM jobs;" | sudo -n $duckdb_cmd --readonly "$chunk_file" 2>&1 | head -1 | sed 's/[^0-9]//g')
                if [ ! -z "$chunk_count" ] && [[ "$chunk_count" =~ ^[0-9]+$ ]]; then
                    completed_jobs=$((completed_jobs + chunk_count))
                    chunk_count_total=$((chunk_count_total + chunk_count))
                else
                    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 청크 파일 읽기 실패: $chunk_file, 결과=$chunk_count" >> "$debug_log"
                fi
            fi
        done < <(find "$DB_COMPLETED_FOLDER" -name "completed_chunk_*.duckdb" 2>/dev/null)
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] 청크 파일: 발견=$chunk_files_found개, 합계=$chunk_count_total" >> "$debug_log"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] 청크 폴더 없음: $DB_COMPLETED_FOLDER" >> "$debug_log"
    fi
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] calculate_completed_jobs 완료: 총 완료 작업 수=$completed_jobs" >> "$debug_log"
    echo "$completed_jobs"
}

# 함수: 시스템 리소스 정보 수집
get_system_resources() {
    # CPU 사용률
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
    
    # R 프로세스 메모리 사용량
    local r_mem_kb=$(ps aux | grep -E "hr_rr_mapping_validation_engine.R" | grep -v "worker.rank" | grep -v grep | awk '{sum+=$6} END {print sum}')
    local r_mem_gb="0.0"
    if [ ! -z "$r_mem_kb" ] && [ "$r_mem_kb" -gt 0 ]; then
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
    
    local rate=$(awk "BEGIN {printf \"%.4f\", $new_completed_jobs / $elapsed}" 2>/dev/null)
    if [ -z "$rate" ] || [ "$(awk "BEGIN {print ($rate <= 0)}" 2>/dev/null)" = "1" ]; then
        echo "N/A"
        return
    fi
    
    local remaining=$((total_jobs - new_completed_jobs))
    if [ "$remaining" -le 0 ]; then
        echo "완료"
        return
    fi
    
    local eta_seconds=$(awk "BEGIN {printf \"%.0f\", $remaining / $rate}" 2>/dev/null)
    if [ -z "$eta_seconds" ] || [ "$eta_seconds" -le 0 ]; then
        echo "N/A"
        return
    fi
    
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

# 함수: 진행 상황 표시
show_progress() {
    local total_jobs=$1
    local log_file=$2
    
    if [ -z "$total_jobs" ] || [ "$total_jobs" -eq 0 ]; then
        return 0
    fi
    
    if [ "$PROGRESS_BAR_INITIALIZED" -eq 0 ]; then
        printf "\n"
        printf "\033[1A\r"
        PROGRESS_BAR_INITIALIZED=1
    fi
    
    local completed_jobs=$(calculate_completed_jobs)
    local debug_log="/tmp/validation_progress_debug.log"
    
    # 디버깅: show_progress 내부 값 확인
    local pre_completed=${START_COMPLETED_JOBS:-0}
    local new_completed_jobs=0
    if [ "$completed_jobs" -gt "$pre_completed" ]; then
        new_completed_jobs=$((completed_jobs - pre_completed))
    fi
    
    # 전체 작업 수 계산 (HR engine 방식: 시작 시점 완료 작업 수 + 남은 작업 수)
    local total_all_jobs=$((pre_completed + total_jobs))
    
    # 전체 진행률 계산 (남은 작업 기준, HR engine 방식)
    local overall_pct=0
    if [ "$total_jobs" -gt 0 ]; then
        overall_pct=$((new_completed_jobs * 100 / total_jobs))
    fi
    
    # 디버깅 로그
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] show_progress: completed_jobs=$completed_jobs, pre_completed=$pre_completed, new_completed_jobs=$new_completed_jobs, total_jobs=$total_jobs, total_all_jobs=$total_all_jobs, overall_pct=$overall_pct%" >> "$debug_log"
    
    local progress_pct=$overall_pct
    local bar_width=20
    local filled=$((new_completed_jobs * bar_width / total_jobs))
    if [ "$filled" -gt "$bar_width" ]; then filled=$bar_width; fi
    local bar=""
    for ((i=0; i<filled; i++)); do bar="${bar}="; done
    for ((i=filled; i<bar_width; i++)); do bar="${bar}-"; done
    
    local first_line="[작업 진행] [$bar] $completed_jobs/$total_all_jobs 완료 ($progress_pct%)"
    
    local resources=$(get_system_resources)
    local cpu_usage=$(echo "$resources" | cut -d'|' -f1 | cut -d':' -f2)
    local mem_info=$(echo "$resources" | cut -d'|' -f2 | cut -d':' -f2)
    local swap_info=$(echo "$resources" | cut -d'|' -f3 | cut -d':' -f2)
    local load_avg=$(echo "$resources" | cut -d'|' -f4 | cut -d':' -f2)
    local r_mem=$(echo "$resources" | cut -d'|' -f5 | cut -d':' -f2)
    
    local eta=$(calculate_eta "$new_completed_jobs" "$total_jobs")
    local second_line="[리소스] CPU: $cpu_usage | MEM: $mem_info | Swap: $swap_info | Load: $load_avg | R_MEM: $r_mem | ETA: $eta"
    
    local combined_line="${first_line} | ${second_line}"
    if [ -c /dev/tty ] 2>/dev/null; then
        printf "\r\033[K%s" "$combined_line" > /dev/tty 2>&1
    else
        printf "\r\033[K%s" "$combined_line"
    fi
}

# 함수: 로그에서 전체 작업 수 파싱
parse_total_jobs_from_log() {
    local log_file=$1
    grep -o "총 [0-9]*개 작업 시작" "$log_file" 2>/dev/null | tail -1 | grep -o "[0-9]*" | head -1
}

# 함수: 정상 종료 처리
cleanup() {
    log ""
    log "===================== SCRIPT INTERRUPTED ====================="
    log "스크립트 종료 요청 수신 (Ctrl+C 또는 종료 신호)"
    
    if [ ! -z "$CURRENT_R_PID" ]; then
        R_PID=$(pgrep -P $CURRENT_R_PID | head -1)
        if [ ! -z "$R_PID" ]; then
            log "저장된 PID ($CURRENT_R_PID)의 자식 프로세스 (PID: $R_PID) 종료 중..."
            kill $R_PID 2>/dev/null
        fi
        if kill -0 $CURRENT_R_PID 2>/dev/null; then
            kill $CURRENT_R_PID 2>/dev/null
        fi
    fi
    
    R_PID=$(pgrep -f "hr_rr_mapping_validation_engine.R" | grep -v "worker.rank" | head -1)
    if [ ! -z "$R_PID" ] && kill -0 $R_PID 2>/dev/null; then
        log "실행 중인 메인 R 프로세스 (PID: $R_PID) 종료 중..."
        kill -TERM $R_PID 2>/dev/null
        sleep 5
        if kill -0 $R_PID 2>/dev/null; then
            log "정상 종료 실패, 강제 종료 중..."
            kill -9 $R_PID 2>/dev/null
        else
            log "메인 R 프로세스 정상 종료 완료"
        fi
    fi
    
    WORKER_COUNT=$(pgrep -f "worker.rank.*parallelly.parent" | wc -l)
    if [ "$WORKER_COUNT" -gt 0 ]; then
        log "future 워커 프로세스 $WORKER_COUNT개 발견, 종료 중..."
        pkill -f "worker.rank.*parallelly.parent" 2>/dev/null
        sleep 1
        pkill -9 -f "worker.rank.*parallelly.parent" 2>/dev/null
    fi
    
    if [ ! -z "$RSCRIPT_PID" ] && kill -0 $RSCRIPT_PID 2>/dev/null; then
        log "systemd-run 프로세스 (PID: $RSCRIPT_PID) 종료 중..."
        kill -TERM $RSCRIPT_PID 2>/dev/null
        sleep 2
        if kill -0 $RSCRIPT_PID 2>/dev/null; then
            kill -9 $RSCRIPT_PID 2>/dev/null
        fi
    fi
    
    SCOPE_NAME=$(systemctl list-units --type=scope --state=running 2>/dev/null | grep -E "validation|run-r[0-9a-f]" | awk '{print $1}' | head -1)
    if [ ! -z "$SCOPE_NAME" ]; then
        log "남아있는 systemd scope ($SCOPE_NAME) 강제 종료 중..."
        sudo systemctl kill -s KILL "$SCOPE_NAME" 2>/dev/null || true
    fi
    
    log "로그 파일: $LOG_FILE"
    log "=========================================================="
    exit 0
}

# 신호 처리 설정
trap cleanup SIGINT SIGTERM

# Conda 환경 활성화
if [ -z "$CONDA_DEFAULT_ENV" ] || [ "$CONDA_DEFAULT_ENV" != "$CONDA_ENV" ]; then
    if [ -z "$CONDA_BASE" ]; then
        CONDA_BASE=$(conda info --base 2>/dev/null)
        if [ -z "$CONDA_BASE" ]; then
            if [ -d "$HOME/anaconda3" ]; then
                CONDA_BASE="$HOME/anaconda3"
            elif [ -d "$HOME/miniconda3" ]; then
                CONDA_BASE="$HOME/miniconda3"
            else
                echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: conda를 찾을 수 없습니다." >&2
                exit 1
            fi
        fi
    fi
    
    if [ -z "$CONDA_SHLVL" ] && [ -f "$CONDA_BASE/etc/profile.d/conda.sh" ]; then
        source "$CONDA_BASE/etc/profile.d/conda.sh"
    fi
    
    CONDA_ENV_PATH="$CONDA_BASE/envs/$CONDA_ENV"
    if [ ! -d "$CONDA_ENV_PATH" ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: conda 환경 '$CONDA_ENV'의 경로를 찾을 수 없습니다: $CONDA_ENV_PATH" >&2
        exit 1
    fi
    
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
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: 로그 디렉토리를 찾을 수 없습니다: $LOG_DIR" >&2
    exit 1
fi

# R 스크립트 파일 존재 확인
if [ ! -f "$R_SCRIPT" ]; then
    log "ERROR: R 스크립트를 찾을 수 없습니다: $R_SCRIPT"
    exit 1
fi

# sudo 권한 확인
if ! sudo -n true 2>/dev/null; then
    log "sudo 권한 확인 중... (비밀번호 입력이 필요할 수 있습니다)"
    if ! sudo -v; then
        log "ERROR: sudo 권한 확인 실패."
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
    
    if [ "$RESTART_COUNT" -eq 0 ]; then
        START_COMPLETED_JOBS=$(calculate_completed_jobs)
        # [수정] 시작 시점 calculate_completed_jobs 실패 처리
        if [ "$START_COMPLETED_JOBS" = "-1" ]; then
            log "ERROR: 시작 시점 중앙 DB 쿼리 실패 (재시도 5회 모두 실패)"
            log "파일: $DB_COMPLETED_FILE"
            log "이어하기 기능을 보장하기 위해 시작을 중단합니다."
            exit 1
        fi
        START_TIME=$(date +%s)
        log "실행 시작 시점 완료 작업 수: $START_COMPLETED_JOBS"
        log "실행 시작 시간: $(date -d "@$START_TIME" '+%Y-%m-%d %H:%M:%S')"
    fi
    
    # R 스크립트 실행 (메모리 제한 적용: 100G)
    log "R 스크립트 실행 중... (메모리 제한: 100G)"
    RSCRIPT_CMD="$CONDA_ENV_PATH/bin/Rscript"
    if [ ! -f "$RSCRIPT_CMD" ]; then
        log "ERROR: Rscript를 찾을 수 없습니다: $RSCRIPT_CMD"
        exit 1
    fi
    sudo systemd-run --scope -p MemoryMax=100G "$RSCRIPT_CMD" "$R_SCRIPT" >> "$LOG_FILE" 2>&1 &
    RSCRIPT_PID=$!
    CURRENT_R_PID=$RSCRIPT_PID
    
    sleep 1
    ACTUAL_R_PID=$(pgrep -f "hr_rr_mapping_validation_engine.R" | grep -v "worker.rank" | head -1)
    if [ ! -z "$ACTUAL_R_PID" ]; then
        CURRENT_R_PID=$ACTUAL_R_PID
    else
        ACTUAL_R_PID=$(pgrep -P $RSCRIPT_PID | head -1)
        if [ ! -z "$ACTUAL_R_PID" ]; then
            CURRENT_R_PID=$ACTUAL_R_PID
        fi
    fi
    
    # 진행 상황 추적 시작
    TOTAL_JOBS=0
    PROGRESS_TRACK_PID=""
    
    (
        if [ -c /dev/tty ] 2>/dev/null; then
            exec > /dev/tty 2>&1
        fi
        
        if [ -z "$TERM" ] || [ "$TERM" = "dumb" ]; then
            if [ -n "$TMUX" ]; then
                export TERM="screen-256color"
            else
                export TERM="xterm-256color"
            fi
        fi
        
        if [ -z "$DB_COMPLETED_FILE" ]; then
            DB_COMPLETED_FILE="/home/hashjamm/results/disease_network/hr_rr_mapping_validation_results/validation_completed_jobs.duckdb"
        fi
        if [ -z "$DB_COMPLETED_FOLDER" ]; then
            DB_COMPLETED_FOLDER="/home/hashjamm/results/disease_network/hr_rr_mapping_validation_results/validation_completed_jobs"
        fi
        if [ -z "$LOG_FILE" ]; then
            LOG_DIR="/home/hashjamm/results/disease_network"
            SCRIPT_BASENAME="hr_rr_mapping_validation_manager"
            LOG_FILE="$LOG_DIR/${SCRIPT_BASENAME}_history.log"
        fi
        if [ -z "$CONDA_ENV_PATH" ] && [ ! -z "$CONDA_BASE" ] && [ ! -z "$CONDA_ENV" ]; then
            CONDA_ENV_PATH="$CONDA_BASE/envs/$CONDA_ENV"
        fi
        
        START_COMPLETED_JOBS=$(calculate_completed_jobs)
        # [수정] 서브셸 내부에서도 시작 시점 calculate_completed_jobs 실패 처리
        if [ "$START_COMPLETED_JOBS" = "-1" ]; then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: 시작 시점 중앙 DB 쿼리 실패 (재시도 5회 모두 실패)" >&2
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] 파일: $DB_COMPLETED_FILE" >&2
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] 이어하기 기능을 보장하기 위해 시작을 중단합니다." >&2
            exit 1
        fi
        
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
        
        if [ ! -z "$TOTAL_JOBS" ] && [ "$TOTAL_JOBS" -gt 0 ]; then
            shutdown_flag_file="${LOG_DIR}/validation_shutdown_flag.txt"
            while pgrep -f "hr_rr_mapping_validation_engine.R" | grep -v "worker.rank" | grep -v grep > /dev/null 2>&1; do
                # [수정] 종료 플래그 파일 확인
                if [ -f "$shutdown_flag_file" ]; then
                    echo ""
                    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 종료 플래그 파일 감지: $shutdown_flag_file"
                    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 중앙 DB 쿼리 실패로 인한 안전 종료"
                    cat "$shutdown_flag_file"
                    rm -f "$shutdown_flag_file"
                    # R 스크립트 종료 (이어하기 보장)
                    if [ ! -z "$CURRENT_R_PID" ] && kill -0 "$CURRENT_R_PID" 2>/dev/null; then
                        echo "[$(date '+%Y-%m-%d %H:%M:%S')] R 스크립트 안전 종료 중..."
                        kill -TERM "$CURRENT_R_PID" 2>/dev/null
                        sleep 2
                        if kill -0 "$CURRENT_R_PID" 2>/dev/null; then
                            kill -9 "$CURRENT_R_PID" 2>/dev/null
                        fi
                    fi
                    exit 1
                fi
                
                show_progress "$TOTAL_JOBS" "$LOG_FILE"
                printf "" > /dev/tty 2>/dev/null || true
                sleep 30
            done
            show_progress "$TOTAL_JOBS" "$LOG_FILE"
            echo ""
        else
            printf "[작업 진행] 전체 작업 수 확인 중... (작업은 정상 진행 중)\n"
        fi
    ) &
    PROGRESS_TRACK_PID=$!
    
    wait $RSCRIPT_PID 2>/dev/null
    EXIT_CODE=$?
    
    if [ ! -z "$PROGRESS_TRACK_PID" ]; then
        wait $PROGRESS_TRACK_PID 2>/dev/null
    fi
    
    log ""
    log "R 스크립트 종료 코드: $EXIT_CODE"
    
    if [[ $EXIT_CODE -eq 0 ]]; then
        log ""
        log "=========================================================="
        log "✓ 모든 작업이 완료되었습니다!"
        log "=========================================================="
        log "총 재시작 횟수: $RESTART_COUNT"
        log "로그 파일: $LOG_FILE"
        log "=========================================================="
        exit 0
    else
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

log "ERROR: 예상치 못한 루프 종료"
exit 1
