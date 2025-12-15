#!/usr/bin/env python3
"""
고급 tmux 모니터링 시스템
- 기존 LOG_FILE 읽기 전용 (기존 프로세스에 영향 없음)
- pipe-pane 로그 실시간 모니터링
- WebSocket 기반 실시간 대시보드
- 고급 시각화 및 분석

================================================================================
사용 방법
================================================================================

1. pipe-pane 로깅 시작 (한 번만 실행, tmux 세션에서)
   tmux pipe-pane -t validation -o 'cat >> /tmp/tmux_monitor/validation.log'

2. 서버 실행
   python3 tmux_monitor_advanced.py > /tmp/tmux_monitor/server.log 2>&1 &
   
   또는 백그라운드로:
   nohup python3 tmux_monitor_advanced.py > /tmp/tmux_monitor/server.log 2>&1 &

3. 브라우저 접속
   로컬 접속 (서버에 직접 로그인된 경우):
     bash /home/hashjamm/codes/disease_network/open_monitor.sh
     또는
     xdg-open http://localhost:5000
   
   원격 접속 (다른 컴퓨터에서):
     ssh -L 5000:localhost:5000 사용자명@서버IP
     그 후 브라우저에서 http://localhost:5000 접속

4. 서버 종료
   pkill -f tmux_monitor_advanced.py
   또는
   kill $(cat /tmp/tmux_monitor/server.pid)

================================================================================
보안 설정
================================================================================
- 서버는 localhost(127.0.0.1)만 리스닝 (외부 직접 접속 불가)
- 원격 접속은 SSH 터널링 필요
- 기존 프로세스에 영향 없음 (읽기 전용 로그 사용)

================================================================================
파일 위치
================================================================================
- 서버 코드: /home/hashjamm/codes/disease_network/tmux_monitor_advanced.py
- pipe-pane 로그: /tmp/tmux_monitor/validation.log
- 서버 로그: /tmp/tmux_monitor/server.log
- 기존 로그: /home/hashjamm/results/disease_network/hr_rr_mapping_validation_manager_v2_history.log

자세한 내용은 TMUX_MONITOR_README.md 참고
"""
from flask import Flask, render_template_string, jsonify, request
from flask_socketio import SocketIO, emit
import re
import json
import os
import time
from datetime import datetime, timedelta
from collections import deque, defaultdict
import threading
import subprocess
from pathlib import Path
import statistics

# DuckDB import
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False
    print("[WARNING] duckdb 모듈을 찾을 수 없습니다. DuckDB 기능이 비활성화됩니다.")

# psutil import (선택사항, 없어도 시스템 명령어로 대체 가능)
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

app = Flask(__name__)
app.config['SECRET_KEY'] = 'tmux-monitor-secret-key-2025'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# 설정
TMUX_SESSION = "validation"
LOG_DIR = "/tmp/tmux_monitor"
PIPE_LOG = f"{LOG_DIR}/{TMUX_SESSION}.log"
STRUCTURED_LOG = "/home/hashjamm/results/disease_network/hr_rr_mapping_validation_manager_v2_history.log"

# 프로젝트 설정 저장 경로
PROJECTS_CONFIG_FILE = os.path.expanduser("~/.monitor_projects.json")

# 데이터 저장소
stats_history = deque(maxlen=5000)  # 최근 5000개 데이터
error_history = deque(maxlen=1000)
alert_history = deque(maxlen=100)  # 알림 히스토리
performance_metrics = {
    'start_time': None,
    'completed_jobs': 0,
    'total_jobs': 0,
    'current_speed': 0.0,
    'avg_speed': 0.0,
    'peak_speed': 0.0,
    'min_speed': 0.0,
    'processing_times': deque(maxlen=1000),  # 처리 시간 히스토리
}

def parse_structured_log():
    """
    구조화된 로그 파일에서 데이터 추출 (읽기 전용)
    
    기존 프로세스가 작성하는 로그 파일을 읽어서 통계 정보를 추출합니다.
    기존 프로세스에 영향을 주지 않도록 읽기 전용으로 접근합니다.
    
    Returns:
        dict: 파싱된 통계 정보 (완료 작업 수, 전체 작업 수, 에러 등)
        None: 로그 파일이 없거나 읽을 수 없는 경우
    """
    if not os.path.exists(STRUCTURED_LOG):
        return None
    
    try:
        with open(STRUCTURED_LOG, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            if not lines:
                return None
            
            # 마지막 부분에서 정보 추출
            last_lines = ''.join(lines[-200:])
            
            stats = {}
            
            # 완료 작업 수 파싱
            completed_match = re.search(r'완료된 작업:\s*(\d+)', last_lines)
            if completed_match:
                stats['completed'] = int(completed_match.group(1))
            
            # 전체 작업 수 파싱
            total_match = re.search(r'전체 작업:\s*(\d+)', last_lines)
            if total_match:
                stats['total'] = int(total_match.group(1))
            
            # 진행률 계산
            if 'completed' in stats and 'total' in stats and stats['total'] > 0:
                stats['progress'] = int((stats['completed'] / stats['total']) * 100)
            
            # 에러 추출
            error_lines = [l for l in lines[-1000:] if any(keyword in l.upper() for keyword in ['ERROR', 'FAILED', '오류', '실패', 'ERROR:'])]
            stats['error_count'] = len(error_lines)
            stats['recent_errors'] = error_lines[-10:] if error_lines else []
            
            return stats
    except Exception as e:
        print(f"[WARNING] 구조화된 로그 파싱 오류: {e}")
        return None

def parse_pipe_log_line(line):
    """
    pipe-pane 로그에서 실시간 정보 추출
    
    tmux pipe-pane으로 복사된 화면 출력에서 프로그래스 바, 처리 속도,
    ETA 등의 정보를 정규식으로 추출합니다.
    
    Args:
        line (str): pipe-pane 로그의 한 줄
        
    Returns:
        dict: 추출된 통계 정보 (progress, completed, total, speed, eta 등)
        None: 파싱할 정보가 없는 경우
    """
    stats = {}
    
    # 프로그래스 바 파싱 (다양한 형식 지원)
    progress_patterns = [
        r'Progress:\s*\[.*?\]\s*(\d+)%',
        r'(\d+)%\s*완료',
        r'진행률:\s*(\d+)%',
        r'\[.*?\]\s*(\d+)%',  # 일반적인 진행률 패턴
    ]
    
    for pattern in progress_patterns:
        match = re.search(pattern, line)
        if match:
            try:
                stats['progress'] = int(match.group(1))
                break
            except:
                continue
    
    # 완료/전체 작업 수
    completed_match = re.search(r'Completed:\s*(\d+)/(\d+)', line)
    if completed_match:
        stats['completed'] = int(completed_match.group(1))
        stats['total'] = int(completed_match.group(2))
    
    # 완료된 작업 / 전체 작업 (한국어)
    completed_kr_match = re.search(r'완료된 작업:\s*(\d+).*?전체 작업:\s*(\d+)', line)
    if completed_kr_match:
        stats['completed'] = int(completed_kr_match.group(1))
        stats['total'] = int(completed_kr_match.group(2))
    
    # 처리 속도
    speed_patterns = [
        r'Speed:\s*([\d.]+)\s*jobs/sec',
        r'속도:\s*([\d.]+)',
        r'(\d+\.\d+)\s*jobs/sec',
    ]
    
    for pattern in speed_patterns:
        match = re.search(pattern, line)
        if match:
            try:
                stats['speed'] = float(match.group(1))
                break
            except:
                continue
    
    # ETA
    eta_match = re.search(r'ETA:\s*([^\n]+)', line)
    if eta_match:
        stats['eta'] = eta_match.group(1).strip()
    
    # 타임스탬프
    timestamp_match = re.search(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]', line)
    if timestamp_match:
        stats['timestamp'] = timestamp_match.group(1)
    else:
        stats['timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 에러 감지
    if any(keyword in line.upper() for keyword in ['ERROR', 'FAILED', '오류', '실패', 'ERROR:']):
        stats['error'] = True
        error_history.append({
            'message': line.strip()[:200],  # 길이 제한
            'timestamp': stats['timestamp']
        })
    
    if stats:
        stats['raw_line'] = line.strip()[:500]  # 길이 제한
        return stats
    return None

def tail_pipe_log():
    """
    pipe-pane 로그를 실시간으로 읽어서 WebSocket으로 전송
    
    파일 끝에서부터 실시간으로 로그를 읽어서 파싱하고,
    WebSocket을 통해 클라이언트에게 전송합니다.
    백그라운드 스레드에서 실행됩니다.
    """
    # 현재 프로젝트 설정 가져오기 (동적)
    project_config = get_current_project_config()
    pipe_log_path = project_config.get('pipe_log', PIPE_LOG)
    tmux_session = project_config.get('tmux_session', TMUX_SESSION)
    
    if not os.path.exists(pipe_log_path):
        print(f"[INFO] pipe-pane 로그 파일이 없습니다: {pipe_log_path}")
        print(f"[INFO] tmux 세션에서 다음 명령을 실행하세요:")
        print(f"      tmux pipe-pane -t {tmux_session} -o 'cat >> {pipe_log_path}'")
        return
    
    print(f"[INFO] pipe-pane 로그 모니터링 시작: {pipe_log_path} (프로젝트: {_current_project_name or 'default'})")
    
    with open(pipe_log_path, 'r', encoding='utf-8', errors='ignore') as f:
        # 파일 끝으로 이동
        try:
            f.seek(0, 2)
        except:
            pass
        
        while True:
            try:
                line = f.readline()
                if line:
                    parsed = parse_pipe_log_line(line)
                    if parsed:
                        stats_history.append(parsed)
                        update_performance_metrics(parsed)
                        socketio.emit('log_update', parsed)
                    else:
                        # 파싱되지 않은 일반 로그도 전송
                        socketio.emit('raw_log', {'line': line.strip()})
                else:
                    time.sleep(0.1)  # 100ms 간격
            except Exception as e:
                print(f"[ERROR] 로그 읽기 오류: {e}")
                time.sleep(1)

def update_performance_metrics(stats):
    """성능 메트릭 업데이트"""
    if performance_metrics['start_time'] is None:
        performance_metrics['start_time'] = datetime.now()
    
    if 'completed' in stats:
        performance_metrics['completed_jobs'] = stats['completed']
    
    if 'total' in stats:
        performance_metrics['total_jobs'] = stats['total']
    
    if 'speed' in stats:
        speed = stats['speed']
        performance_metrics['current_speed'] = speed
        
        # 평균 속도 계산
        speeds = [s.get('speed', 0) for s in stats_history if 'speed' in s]
        if speeds:
            performance_metrics['avg_speed'] = sum(speeds) / len(speeds)
            performance_metrics['peak_speed'] = max(speeds)

# 시스템 메트릭 수집 설정 (전역 변수로 이동)
DB_COMPLETED_FILE = "/home/hashjamm/results/disease_network/hr_rr_mapping_validation_job_queue_db/completed_jobs.duckdb"
DB_COMPLETED_FOLDER = "/home/hashjamm/results/disease_network/hr_rr_mapping_validation_job_queue_db/completed_jobs/"
DB_SYSTEM_FAILED_FILE = "/home/hashjamm/results/disease_network/hr_rr_mapping_validation_job_queue_db/system_failed_jobs.duckdb"
DB_SYSTEM_FAILED_FOLDER = "/home/hashjamm/results/disease_network/hr_rr_mapping_validation_job_queue_db/system_failed_jobs/"
MATCHED_PARQUET_FOLDER = "/home/hashjamm/project_data/disease_network/matched_date_parquet/"
RESULTS_MAPPING_FOLDER = "/home/hashjamm/results/disease_network/hr_rr_mapping_validation_results/"

# 알림 임계값 설정
ALERT_THRESHOLDS = {
    'cpu_usage': 90.0,
    'memory_usage': 80.0,
    'disk_usage': 90.0,
    'disk_free_percent': 10.0,
    'error_rate': 5.0,
    'load_average': 100.0,
}

class ComprehensiveMetricsCollector:
    """
    포괄적인 메트릭 수집 클래스
    시스템 직접 추출 방식으로 모든 모니터링 데이터를 수집
    """
    def __init__(self):
        project_config = get_current_project_config()
        self.db_completed_file = project_config.get('db_completed_file', DB_COMPLETED_FILE)
        self.db_completed_folder = project_config.get('db_completed_folder', DB_COMPLETED_FOLDER)
        self.db_system_failed_file = project_config.get('db_system_failed_file', DB_SYSTEM_FAILED_FILE)
        self.db_system_failed_folder = project_config.get('db_system_failed_folder', DB_SYSTEM_FAILED_FOLDER)
        # 프로젝트별 경로 사용 (없으면 기본값)
        self.matched_parquet_folder = project_config.get('matched_parquet_folder', MATCHED_PARQUET_FOLDER)
        self.results_mapping_folder = project_config.get('results_mapping_folder', RESULTS_MAPPING_FOLDER)
        self.results_hr_folder = project_config.get('results_hr_folder', RESULTS_MAPPING_FOLDER)
        
        self.last_completed_count = 0
        self.last_check_time = None
        self.total_jobs_cache = None
        self.start_time = None
        
    def get_completed_jobs_count(self):
        """완료된 작업 수 조회 (DuckDB)"""
        if not DUCKDB_AVAILABLE:
            return 0
        
        # 매번 현재 프로젝트 설정 가져오기
        project_config = get_current_project_config()
        db_completed_file = project_config.get('db_completed_file', self.db_completed_file)
        db_completed_folder = project_config.get('db_completed_folder', self.db_completed_folder)
        
        total_completed = 0
        
        # 중앙 DB 확인
        if os.path.exists(db_completed_file):
            try:
                conn = duckdb.connect(db_completed_file, read_only=True)
                result = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()
                if result:
                    total_completed = result[0]
                conn.close()
            except Exception as e:
                print(f"[WARNING] 중앙 DB 조회 오류: {e}")
        
        # 청크 파일들 확인
        if os.path.exists(db_completed_folder):
            for chunk_file in Path(db_completed_folder).glob("completed_chunk_*.duckdb"):
                try:
                    conn = duckdb.connect(str(chunk_file), read_only=True)
                    result = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()
                    if result:
                        total_completed += result[0]
                    conn.close()
                except Exception as e:
                    continue
        
        return total_completed
    
    def get_total_jobs(self):
        """전체 작업 수 조회 (시스템에서 직접 계산)"""
        # 매번 현재 프로젝트 설정 가져오기 (프로젝트 전환 시 경로 변경 반영)
        project_config = get_current_project_config()
        matched_parquet_folder = project_config.get('matched_parquet_folder', self.matched_parquet_folder)
        
        # 프로젝트가 변경되었으면 캐시 초기화
        if matched_parquet_folder != self.matched_parquet_folder:
            self.total_jobs_cache = None
            self.matched_parquet_folder = matched_parquet_folder
        
        if self.total_jobs_cache is not None:
            return self.total_jobs_cache
        
        # matched_parquet 폴더에서 질병 코드 추출
        disease_codes = []
        if os.path.exists(matched_parquet_folder):
            try:
                for file in os.listdir(matched_parquet_folder):
                    if file.startswith("matched_") and file.endswith(".parquet"):
                        code = file.replace("matched_", "").replace(".parquet", "").upper()
                        disease_codes.append(code)
                
                disease_codes = sorted(set(disease_codes))
                
                if disease_codes:
                    # 전체 작업 수 = n × (n - 1) (cause_abb != outcome_abb 조건)
                    self.total_jobs_cache = len(disease_codes) * (len(disease_codes) - 1)
                    return self.total_jobs_cache
            except Exception as e:
                print(f"[WARNING] 전체 작업 수 계산 오류: {e}")
        
        return None
    
    def get_failed_jobs_count(self):
        """실패한 작업 수 조회 (DuckDB)"""
        if not DUCKDB_AVAILABLE:
            return 0
        
        # 매번 현재 프로젝트 설정 가져오기
        project_config = get_current_project_config()
        db_system_failed_file = project_config.get('db_system_failed_file', self.db_system_failed_file)
        db_system_failed_folder = project_config.get('db_system_failed_folder', self.db_system_failed_folder)
        
        total_failed = 0
        
        # 중앙 DB 확인
        if os.path.exists(db_system_failed_file):
            try:
                conn = duckdb.connect(db_system_failed_file, read_only=True)
                result = conn.execute("SELECT COUNT(*) FROM system_failures").fetchone()
                if result:
                    total_failed = result[0]
                conn.close()
            except Exception as e:
                pass
        
        # 청크 파일들 확인
        if os.path.exists(db_system_failed_folder):
            for chunk_file in Path(db_system_failed_folder).glob("system_failed_chunk_*.duckdb"):
                try:
                    conn = duckdb.connect(str(chunk_file), read_only=True)
                    result = conn.execute("SELECT COUNT(*) FROM system_failures").fetchone()
                    if result:
                        total_failed += result[0]
                    conn.close()
                except Exception as e:
                    continue
        
        return total_failed
    
    def calculate_processing_speed(self):
        """처리 속도 계산 (jobs/min)"""
        current_completed = self.get_completed_jobs_count()
        current_time = time.time()
        
        if self.last_check_time is None:
            self.last_completed_count = current_completed
            self.last_check_time = current_time
            if self.start_time is None:
                self.start_time = current_time
            return 0.0
        
        time_diff = current_time - self.last_check_time
        if time_diff == 0:
            return 0.0
        
        jobs_diff = current_completed - self.last_completed_count
        speed_per_sec = jobs_diff / time_diff if jobs_diff > 0 else 0.0
        speed_per_min = speed_per_sec * 60  # 1분당으로 변환
        
        self.last_completed_count = current_completed
        self.last_check_time = current_time
        
        return speed_per_min
    
    def calculate_eta(self, completed, total, speed):
        """예상 완료 시간 계산"""
        if total is None or completed is None or speed <= 0:
            return "계산 중..."
        
        remaining = total - completed
        if remaining <= 0:
            return "완료됨"
        
        seconds_remaining = remaining / speed
        
        # 초를 일/시간/분으로 변환
        days = int(seconds_remaining // 86400)
        hours = int((seconds_remaining % 86400) // 3600)
        minutes = int((seconds_remaining % 3600) // 60)
        
        if days > 0:
            return f"{days}일 {hours}시간"
        elif hours > 0:
            return f"{hours}시간 {minutes}분"
        else:
            return f"{minutes}분"
    
    def get_elapsed_time(self):
        """경과 시간 계산"""
        if self.start_time is None:
            return "0분"
        
        elapsed = time.time() - self.start_time
        days = int(elapsed // 86400)
        hours = int((elapsed % 86400) // 3600)
        minutes = int((elapsed % 3600) // 60)
        
        if days > 0:
            return f"{days}일 {hours}시간"
        elif hours > 0:
            return f"{hours}시간 {minutes}분"
        else:
            return f"{minutes}분"
    
    def get_processed_files_count(self):
        """처리된 결과 파일 수 조회"""
        # 매번 현재 프로젝트 설정 가져오기
        project_config = get_current_project_config()
        results_mapping_folder = project_config.get('results_mapping_folder', self.results_mapping_folder)
        results_hr_folder = project_config.get('results_hr_folder', self.results_hr_folder)
        
        # results_mapping_folder 또는 results_hr_folder 확인
        target_folder = results_mapping_folder if os.path.exists(results_mapping_folder) else results_hr_folder
        
        if not os.path.exists(target_folder):
            return 0
        
        try:
            # map_chunk 또는 hr_chunk 파일 수 확인
            map_count = len(list(Path(target_folder).glob("map_chunk_*.duckdb")))
            hr_count = len(list(Path(target_folder).glob("hr_chunk_*.duckdb")))
            return map_count + hr_count
        except Exception as e:
            return 0

class ErrorAnalyzer:
    """
    에러 통계 및 분류 분석 클래스
    """
    def __init__(self):
        project_config = get_current_project_config()
        self.db_system_failed_file = project_config.get('db_system_failed_file', DB_SYSTEM_FAILED_FILE)
        self.db_system_failed_folder = project_config.get('db_system_failed_folder', DB_SYSTEM_FAILED_FOLDER)
    
    def get_error_statistics(self):
        """에러 통계 조회"""
        if not DUCKDB_AVAILABLE:
            return {
                'total_errors': 0,
                'error_rate': 0.0,
                'error_types': {},
                'recent_errors': []
            }
        
        # 매번 현재 프로젝트 설정 가져오기
        project_config = get_current_project_config()
        db_system_failed_file = project_config.get('db_system_failed_file', self.db_system_failed_file)
        db_system_failed_folder = project_config.get('db_system_failed_folder', self.db_system_failed_folder)
        
        total_errors = 0
        error_types = defaultdict(int)
        recent_errors = []
        
        # 중앙 DB 확인
        if os.path.exists(db_system_failed_file):
            try:
                conn = duckdb.connect(db_system_failed_file, read_only=True)
                errors = conn.execute("SELECT cause_abb, outcome_abb, fu, error_msg, timestamp FROM system_failures ORDER BY timestamp DESC LIMIT 100").fetchall()
                total_errors = conn.execute("SELECT COUNT(*) FROM system_failures").fetchone()[0]
                
                for error in errors:
                    error_msg = error[3] if len(error) > 3 else ""
                    # 에러 유형 분류
                    if 'memory' in error_msg.lower() or '메모리' in error_msg:
                        error_types['memory'] += 1
                    elif 'timeout' in error_msg.lower() or '타임아웃' in error_msg:
                        error_types['timeout'] += 1
                    elif 'disk' in error_msg.lower() or '디스크' in error_msg:
                        error_types['disk'] += 1
                    else:
                        error_types['other'] += 1
                    
                    if len(recent_errors) < 10:
                        recent_errors.append({
                            'cause_abb': error[0],
                            'outcome_abb': error[1],
                            'fu': error[2],
                            'error_msg': error_msg[:200],
                            'timestamp': str(error[4]) if len(error) > 4 else ""
                        })
                
                conn.close()
            except Exception as e:
                print(f"[WARNING] 에러 통계 조회 오류: {e}")
        
        # 청크 파일들 확인
        if os.path.exists(db_system_failed_folder):
            for chunk_file in Path(db_system_failed_folder).glob("system_failed_chunk_*.duckdb"):
                try:
                    conn = duckdb.connect(str(chunk_file), read_only=True)
                    count = conn.execute("SELECT COUNT(*) FROM system_failures").fetchone()[0]
                    total_errors += count
                    conn.close()
                except Exception as e:
                    continue
        
        return {
            'total_errors': total_errors,
            'error_types': dict(error_types),
            'recent_errors': recent_errors
        }
    
    def calculate_error_rate(self, total_jobs, failed_jobs):
        """에러율 계산"""
        if total_jobs and total_jobs > 0:
            return (failed_jobs / total_jobs) * 100
        return 0.0

class ProcessMonitor:
    """
    프로세스 상태 모니터링 클래스
    """
    def __init__(self, process_pattern=None):
        if process_pattern:
            self.process_pattern = process_pattern
        else:
            project_config = get_current_project_config()
            self.process_pattern = project_config.get('process_pattern', 'hr_rr_mapping_validation_engine')
    
    def update_pattern(self, process_pattern):
        """프로세스 패턴 업데이트"""
        self.process_pattern = process_pattern
    
    def get_process_count(self):
        """실행 중인 프로세스 수 조회"""
        # 매번 현재 프로젝트 설정 가져오기
        project_config = get_current_project_config()
        process_pattern = project_config.get('process_pattern', self.process_pattern)
        
        try:
            result = subprocess.run(
                ['pgrep', '-f', process_pattern],
                capture_output=True,
                text=True,
                timeout=2
            )
            if result.returncode == 0:
                pids = result.stdout.strip().split('\n')
                return len([p for p in pids if p])
            return 0
        except Exception as e:
            return 0
    
    def get_process_status(self):
        """프로세스 상태 조회"""
        # 매번 현재 프로젝트 설정 가져오기
        project_config = get_current_project_config()
        process_pattern = project_config.get('process_pattern', self.process_pattern)
        
        try:
            result = subprocess.run(
                ['ps', 'aux'],
                capture_output=True,
                text=True,
                timeout=2
            )
            
            processes = []
            for line in result.stdout.split('\n'):
                if process_pattern in line and 'grep' not in line:
                    parts = line.split()
                    if len(parts) >= 11:
                        processes.append({
                            'pid': parts[1],
                            'cpu': parts[2],
                            'mem': parts[3],
                            'status': parts[7] if len(parts) > 7 else 'R',
                            'command': ' '.join(parts[10:])[:100]
                        })
            
            return processes
        except Exception as e:
            return []
    
    def get_process_memory_usage(self):
        """프로세스별 메모리 사용량 조회"""
        processes = self.get_process_status()
        total_memory_mb = 0
        
        for proc in processes:
            try:
                mem_percent = float(proc['mem'])
                # 전체 메모리에서 비율 계산 (대략적)
                if PSUTIL_AVAILABLE:
                    total_mem = psutil.virtual_memory().total / (1024 * 1024)  # MB
                    total_memory_mb += (mem_percent / 100) * total_mem
            except:
                pass
        
        return total_memory_mb
    
    def get_core_progress(self, total_jobs, completed_jobs, cpu_per_core=None):
        """실제 CPU 코어별 진행 상황 조회"""
        import os
        import math
        
        # 시스템의 실제 CPU 코어 수 확인
        try:
            if PSUTIL_AVAILABLE:
                cpu_count = psutil.cpu_count(logical=True)  # 논리 코어 수
            else:
                cpu_count = os.cpu_count() or 1
        except:
            cpu_count = os.cpu_count() or 1
        
        if cpu_count == 0:
            return []
        
        # 각 코어의 CPU 사용률 가져오기
        if cpu_per_core is None:
            if PSUTIL_AVAILABLE:
                try:
                    # 첫 호출로 초기화 후 실제 측정
                    psutil.cpu_percent(interval=0.1, percpu=True)
                    cpu_per_core = psutil.cpu_percent(interval=0.1, percpu=True)
                except:
                    cpu_per_core = [0.0] * cpu_count
            else:
                cpu_per_core = [0.0] * cpu_count
        
        # 코어 수와 CPU 사용률 배열 길이 맞추기
        cpu_per_core = list(cpu_per_core)[:cpu_count]
        while len(cpu_per_core) < cpu_count:
            cpu_per_core.append(0.0)
        
        # 각 코어에서 실행 중인 프로세스 수 확인
        processes = self.get_process_status()
        processes_per_core = [0.0] * cpu_count
        
        # 프로세스의 CPU affinity 확인 (가능한 경우)
        if PSUTIL_AVAILABLE:
            for proc_info in processes:
                try:
                    pid = int(proc_info['pid'])
                    proc = psutil.Process(pid)
                    # CPU affinity 확인
                    try:
                        cpu_affinity = proc.cpu_affinity()
                        if cpu_affinity:
                            # 각 프로세스를 해당 코어에 분배
                            for cpu_id in cpu_affinity:
                                if 0 <= cpu_id < cpu_count:
                                    processes_per_core[cpu_id] += 1.0 / len(cpu_affinity)
                        else:
                            # affinity를 확인할 수 없으면 전체 코어에 균등 분배
                            for i in range(cpu_count):
                                processes_per_core[i] += 1.0 / cpu_count
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        # 프로세스가 종료되었거나 접근 불가
                        pass
                except (ValueError, psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        
        # 전체 CPU 사용률 합계 계산 (평균이 아니라 합계)
        # 주의: cpu_per_core는 각 코어의 사용률(%)이므로, 96개 코어면 최대 9600%가 될 수 있음
        # 하지만 실제로는 각 코어가 0-100% 범위이므로, 평균을 구하려면 코어 수로 나눠야 함
        avg_cpu_usage = sum(cpu_per_core) / cpu_count if cpu_count > 0 else 0
        
        # 각 코어의 작업 부하 비율 계산
        # CPU 사용률이 0이면 균등 분배 가정
        core_progress = []
        for core_id in range(cpu_count):
            cpu_usage = cpu_per_core[core_id]
            
            # CPU 사용률이 있는 코어들만 고려하여 작업 분배 비율 계산
            # 전체 작업을 CPU 사용률에 비례하여 분배
            if avg_cpu_usage > 0:
                # 각 코어의 CPU 사용률 비율
                cpu_ratio = cpu_usage / (avg_cpu_usage * cpu_count) if avg_cpu_usage > 0 else 1.0 / cpu_count
            else:
                # CPU 사용률이 모두 0이면 균등 분배
                cpu_ratio = 1.0 / cpu_count
            
            # 각 코어가 처리한 작업 수 추정
            # CPU 사용률이 높을수록 더 많은 작업을 처리했다고 가정
            if avg_cpu_usage > 0:
                estimated_completed = completed_jobs * (cpu_usage / (avg_cpu_usage * cpu_count)) if avg_cpu_usage > 0 else completed_jobs / cpu_count
            else:
                estimated_completed = completed_jobs / cpu_count
            
            # 각 코어가 담당해야 할 작업 수 (전체 작업을 코어 수로 나눔)
            estimated_total = total_jobs / cpu_count if total_jobs else 0
            # 작업 완료율 계산
            estimated_progress = (estimated_completed / estimated_total * 100) if estimated_total > 0 else 0
            
            core_progress.append({
                'core_id': core_id + 1,
                'cpu_usage': round(cpu_usage, 1),
                'processes_count': round(processes_per_core[core_id], 1),
                'estimated_completed': int(estimated_completed),
                'estimated_progress': round(min(100, estimated_progress), 1),
                'cpu_ratio': round(cpu_ratio * 100, 1)  # 전체 작업 중 이 코어가 담당하는 비율
            })
        
        return core_progress

class RestartTracker:
    """
    재시작 정보 추적 클래스
    """
    def __init__(self):
        self.structured_log = STRUCTURED_LOG
    
    def get_restart_count(self):
        """재시작 횟수 조회"""
        if not os.path.exists(self.structured_log):
            return 0
        
        try:
            with open(self.structured_log, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                # "사이클 시작: 재시작 횟수 X / 100" 패턴 찾기
                matches = re.findall(r'사이클 시작: 재시작 횟수\s+(\d+)', content)
                if matches:
                    return max([int(m) for m in matches])
                return 0
        except Exception as e:
            return 0
    
    def get_restart_reasons(self):
        """재시작 원인 조회"""
        if not os.path.exists(self.structured_log):
            return []
        
        reasons = []
        try:
            with open(self.structured_log, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
                for i, line in enumerate(lines):
                    if '재시작' in line and ('원인' in line or '이유' in line or '남은 작업' in line):
                        # 다음 몇 줄도 확인
                        context = ' '.join(lines[max(0, i-2):i+3])
                        reasons.append({
                            'timestamp': re.search(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]', line),
                            'reason': line.strip()[:200]
                        })
                        if len(reasons) >= 10:
                            break
        except Exception as e:
            pass
        
        return reasons

def get_comprehensive_resources():
    """포괄적인 시스템 리소스 조회"""
    resources = {}
    
    try:
        # CPU 사용률
        if PSUTIL_AVAILABLE:
            # interval=0.1은 너무 짧아서 정확하지 않을 수 있음
            # 첫 호출은 None을 반환할 수 있으므로 두 번 호출하여 정확도 향상
            psutil.cpu_percent(interval=0.1)  # 첫 호출 (초기화)
            cpu_percent = psutil.cpu_percent(interval=0.1)  # 실제 측정
            psutil.cpu_percent(interval=0.1, percpu=True)  # 첫 호출 (초기화)
            cpu_per_core = psutil.cpu_percent(interval=0.1, percpu=True)  # 실제 측정
            resources['cpu_usage'] = cpu_percent
            resources['cpu_per_core'] = cpu_per_core
        else:
            cpu_result = subprocess.run(
                ['top', '-bn1'], capture_output=True, text=True, timeout=2
            )
            cpu_match = re.search(r'%Cpu\(s\):\s*([\d.]+)', cpu_result.stdout)
            resources['cpu_usage'] = float(cpu_match.group(1)) if cpu_match else 0.0
        
        # 메모리 사용률
        if PSUTIL_AVAILABLE:
            mem = psutil.virtual_memory()
            swap = psutil.swap_memory()
            resources['memory_total_mb'] = mem.total / (1024 * 1024)
            resources['memory_used_mb'] = mem.used / (1024 * 1024)
            resources['memory_percent'] = mem.percent
            resources['swap_total_mb'] = swap.total / (1024 * 1024)
            resources['swap_used_mb'] = swap.used / (1024 * 1024)
            resources['swap_percent'] = swap.percent
        else:
            mem_result = subprocess.run(
                ['free', '-m'], capture_output=True, text=True, timeout=2
            )
            mem_match = re.search(r'Mem:\s*(\d+)\s+(\d+)', mem_result.stdout)
            if mem_match:
                resources['memory_total_mb'] = int(mem_match.group(1))
                resources['memory_used_mb'] = int(mem_match.group(2))
                resources['memory_percent'] = (int(mem_match.group(2)) / int(mem_match.group(1))) * 100
        
        # 디스크 I/O
        if PSUTIL_AVAILABLE:
            disk_io = psutil.disk_io_counters()
            if disk_io:
                resources['disk_read_mb'] = disk_io.read_bytes / (1024 * 1024)
                resources['disk_write_mb'] = disk_io.write_bytes / (1024 * 1024)
        else:
            try:
                iostat_result = subprocess.run(
                    ['iostat', '-x', '1', '1'], capture_output=True, text=True, timeout=3
                )
                # iostat 파싱 (간단한 버전)
                resources['disk_read_mb'] = 0
                resources['disk_write_mb'] = 0
            except:
                resources['disk_read_mb'] = 0
                resources['disk_write_mb'] = 0
        
        # 디스크 공간
        if PSUTIL_AVAILABLE:
            disk = psutil.disk_usage('/')
            resources['disk_total_gb'] = disk.total / (1024 * 1024 * 1024)
            resources['disk_used_gb'] = disk.used / (1024 * 1024 * 1024)
            resources['disk_free_gb'] = disk.free / (1024 * 1024 * 1024)
            resources['disk_percent'] = disk.percent
        else:
            df_result = subprocess.run(
                ['df', '-h', '/'], capture_output=True, text=True, timeout=2
            )
            # df 파싱
            resources['disk_percent'] = 0
        
        # 시스템 부하
        try:
            with open('/proc/loadavg', 'r') as f:
                load_avg = f.read().split()
                resources['load_1min'] = float(load_avg[0])
                resources['load_5min'] = float(load_avg[1])
                resources['load_15min'] = float(load_avg[2])
        except:
            resources['load_1min'] = 0.0
            resources['load_5min'] = 0.0
            resources['load_15min'] = 0.0
        
        resources['timestamp'] = datetime.now().isoformat()
        return resources
        
    except Exception as e:
        print(f"[ERROR] 리소스 조회 오류: {e}")
        return None

def check_alerts(metrics, resources):
    """알림 조건 확인"""
    alerts = []
    
    if resources:
        # CPU 알림
        if resources.get('cpu_usage', 0) > ALERT_THRESHOLDS['cpu_usage']:
            alerts.append({
                'type': 'cpu_high',
                'level': 'warning',
                'message': f"CPU 사용률이 {resources['cpu_usage']:.1f}%로 높습니다.",
                'value': resources['cpu_usage']
            })
        
        # 메모리 알림
        if resources.get('memory_percent', 0) > ALERT_THRESHOLDS['memory_usage']:
            alerts.append({
                'type': 'memory_high',
                'level': 'warning',
                'message': f"메모리 사용률이 {resources['memory_percent']:.1f}%로 높습니다.",
                'value': resources['memory_percent']
            })
        
        # 디스크 공간 알림
        if resources.get('disk_percent', 0) > ALERT_THRESHOLDS['disk_usage']:
            alerts.append({
                'type': 'disk_high',
                'level': 'critical',
                'message': f"디스크 사용률이 {resources['disk_percent']:.1f}%로 높습니다.",
                'value': resources['disk_percent']
            })
        
        disk_free_percent = 100 - resources.get('disk_percent', 0)
        if disk_free_percent < ALERT_THRESHOLDS['disk_free_percent']:
            alerts.append({
                'type': 'disk_low',
                'level': 'critical',
                'message': f"디스크 여유 공간이 {disk_free_percent:.1f}%로 부족합니다.",
                'value': disk_free_percent
            })
        
        # 시스템 부하 알림
        if resources.get('load_1min', 0) > ALERT_THRESHOLDS['load_average']:
            alerts.append({
                'type': 'load_high',
                'level': 'warning',
                'message': f"시스템 부하가 {resources['load_1min']:.2f}로 높습니다.",
                'value': resources['load_1min']
            })
    
    # 에러율 알림
    if metrics.get('error_rate', 0) > ALERT_THRESHOLDS['error_rate']:
        alerts.append({
            'type': 'error_rate_high',
            'level': 'critical',
            'message': f"에러율이 {metrics['error_rate']:.1f}%로 높습니다.",
            'value': metrics['error_rate']
        })
    
    return alerts

def collect_all_metrics():
    """모든 메트릭 수집 및 전송"""
    collector = ComprehensiveMetricsCollector()
    error_analyzer = ErrorAnalyzer()
    process_monitor = ProcessMonitor()
    restart_tracker = RestartTracker()
    
    # 작업 진행 상황
    completed = collector.get_completed_jobs_count()
    total = collector.get_total_jobs()
    failed = collector.get_failed_jobs_count()
    speed = collector.calculate_processing_speed()
    
    progress = (completed / total * 100) if total and total > 0 else 0
    eta = collector.calculate_eta(completed, total, speed) if speed > 0 else "계산 중..."
    elapsed = collector.get_elapsed_time()
    
    # 성능 메트릭
    speeds = [s.get('speed', 0) for s in stats_history if 'speed' in s and s['speed'] > 0]
    avg_speed = statistics.mean(speeds) if speeds else 0.0
    max_speed = max(speeds) if speeds else 0.0
    min_speed = min(speeds) if speeds else 0.0
    
    # 에러 통계
    error_stats = error_analyzer.get_error_statistics()
    error_rate = error_analyzer.calculate_error_rate(total, failed) if total else 0.0
    
    # 프로세스 상태
    process_count = process_monitor.get_process_count()
    process_status = process_monitor.get_process_status()
    process_memory = process_monitor.get_process_memory_usage()
    
    # 코어별 진행 상황
    core_progress = process_monitor.get_core_progress(total, completed)
    
    # 재시작 정보
    restart_count = restart_tracker.get_restart_count()
    
    # 시스템 리소스
    resources = get_comprehensive_resources()
    
    # 처리된 파일 수
    processed_files = collector.get_processed_files_count()
    
    # 통합 메트릭
    all_metrics = {
        # 작업 진행 상황
        'completed': completed,
        'total': total,
        'failed': failed,
        'progress': round(progress, 1),
        'speed': round(speed, 2),
        'avg_speed': round(avg_speed, 2),
        'max_speed': round(max_speed, 2),
        'min_speed': round(min_speed, 2),
        'eta': eta,
        'elapsed_time': elapsed,
        'processed_files': processed_files,
        
        # 에러 통계
        'error_count': error_stats['total_errors'],
        'error_rate': round(error_rate, 2),
        'error_types': error_stats['error_types'],
        'recent_errors': error_stats['recent_errors'],
        
        # 프로세스 상태
        'process_count': process_count,
        'process_status': process_status,
        'process_memory_mb': round(process_memory, 2),
        
        # 코어별 진행 상황
        'core_progress': core_progress,
        
        # 재시작 정보
        'restart_count': restart_count,
        
        # 시스템 리소스
        'resources': resources,
        
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # 알림 확인
    alerts = check_alerts(all_metrics, resources)
    if alerts:
        all_metrics['alerts'] = alerts
        for alert in alerts:
            alert_history.append(alert)
            socketio.emit('alert', alert)
    
    return all_metrics

# 전역 메트릭 수집기 인스턴스 (속도 계산을 위해 상태 유지 필요)
_global_collector = None
_global_error_analyzer = None
_global_process_monitor = None
_global_restart_tracker = None

# 현재 프로젝트 설정 (동적 변경 가능)
_current_project_name = None
_current_project_config = None

def get_current_project_config():
    """현재 프로젝트 설정 가져오기 (동적)"""
    global _current_project_name, _current_project_config
    
    # Flask request context에서 프로젝트 이름 가져오기
    try:
        from flask import has_request_context, request
        if has_request_context():
            project_name = request.args.get('project')
            if project_name:
                projects_config = load_projects_config()
                if project_name in projects_config.get('projects', {}):
                    _current_project_name = project_name
                    _current_project_config = projects_config['projects'][project_name]
                    return _current_project_config
    except:
        pass
    
    # 캐시된 설정이 있으면 사용
    if _current_project_name and _current_project_config:
        return _current_project_config
    
    # 기본 프로젝트 사용
    projects_config = load_projects_config()
    default_project = projects_config.get('default_project', 'validation')
    if default_project in projects_config.get('projects', {}):
        _current_project_name = default_project
        _current_project_config = projects_config['projects'][default_project]
        return _current_project_config
    
    # 기본값 반환
    return {
        'tmux_session': TMUX_SESSION,
        'pipe_log': PIPE_LOG,
        'structured_log': STRUCTURED_LOG,
        'db_completed_file': DB_COMPLETED_FILE,
        'db_completed_folder': DB_COMPLETED_FOLDER,
        'db_system_failed_file': DB_SYSTEM_FAILED_FILE,
        'db_system_failed_folder': DB_SYSTEM_FAILED_FOLDER,
        'process_pattern': 'hr_rr_mapping_validation_engine'
    }

def collect_and_send_metrics():
    """주기적으로 메트릭 수집 및 전송"""
    global _global_collector, _global_error_analyzer, _global_process_monitor, _global_restart_tracker
    
    print("[INFO] 포괄적 메트릭 수집 시작")
    
    # 전역 인스턴스 초기화 (상태 유지를 위해)
    if _global_collector is None:
        _global_collector = ComprehensiveMetricsCollector()
    if _global_error_analyzer is None:
        _global_error_analyzer = ErrorAnalyzer()
    if _global_process_monitor is None:
        _global_process_monitor = ProcessMonitor()
    if _global_restart_tracker is None:
        _global_restart_tracker = RestartTracker()
    
    while True:
        try:
            # 전역 인스턴스 사용
            collector = _global_collector
            error_analyzer = _global_error_analyzer
            process_monitor = _global_process_monitor
            restart_tracker = _global_restart_tracker
            
            # 작업 진행 상황
            completed = collector.get_completed_jobs_count()
            total = collector.get_total_jobs()
            failed = collector.get_failed_jobs_count()
            speed = collector.calculate_processing_speed()
            
            progress = (completed / total * 100) if total and total > 0 else 0
            eta = collector.calculate_eta(completed, total, speed) if speed > 0 else "계산 중..."
            elapsed = collector.get_elapsed_time()
            
            # 성능 메트릭
            speeds = [s.get('speed', 0) for s in stats_history if 'speed' in s and s['speed'] > 0]
            avg_speed = statistics.mean(speeds) if speeds else 0.0
            max_speed = max(speeds) if speeds else 0.0
            min_speed = min(speeds) if speeds else 0.0
            
            # 에러 통계
            error_stats = error_analyzer.get_error_statistics()
            error_rate = error_analyzer.calculate_error_rate(total, failed) if total else 0.0
            
            # 시스템 리소스 (먼저 가져와서 코어별 진행 상황 계산에 사용)
            resources = get_comprehensive_resources()
            
            # 프로세스 상태
            # 현재 프로젝트 설정으로 프로세스 모니터 업데이트
            project_config = get_current_project_config()
            if project_config.get('process_pattern'):
                process_monitor.update_pattern(project_config['process_pattern'])
            
            process_count = process_monitor.get_process_count()
            process_status = process_monitor.get_process_status()
            process_memory = process_monitor.get_process_memory_usage()
            
            # 코어별 진행 상황 (CPU 코어별 사용률 전달)
            cpu_per_core = resources.get('cpu_per_core') if resources else None
            core_progress = process_monitor.get_core_progress(total, completed, cpu_per_core)
            
            # 재시작 정보
            restart_count = restart_tracker.get_restart_count()
            
            # 처리된 파일 수
            processed_files = collector.get_processed_files_count()
            
            # 통합 메트릭
            metrics = {
                # 작업 진행 상황
                'completed': completed,
                'total': total,
                'failed': failed,
                'progress': round(progress, 1),
                'speed': round(speed, 2),
                'avg_speed': round(avg_speed, 2),
                'max_speed': round(max_speed, 2),
                'min_speed': round(min_speed, 2),
                'eta': eta,
                'elapsed_time': elapsed,
                'processed_files': processed_files,
                
                # 에러 통계
                'error_count': error_stats['total_errors'],
                'error_rate': round(error_rate, 2),
                'error_types': error_stats['error_types'],
                'recent_errors': error_stats['recent_errors'],
                
                # 프로세스 상태
                'process_count': process_count,
                'process_status': process_status,
                'process_memory_mb': round(process_memory, 2),
                
                # 코어별 진행 상황
                'core_progress': core_progress,
                
                # 재시작 정보
                'restart_count': restart_count,
                
                # 시스템 리소스
                'resources': resources,
                
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            # 알림 확인
            alerts = check_alerts(metrics, resources)
            if alerts:
                metrics['alerts'] = alerts
                for alert in alerts:
                    alert_history.append(alert)
                    socketio.emit('alert', alert)
            
            # WebSocket으로 전송
            socketio.emit('metrics_update', metrics)
            
            # 히스토리 저장
            stats_history.append(metrics)
            
            # 성능 메트릭 업데이트
            if performance_metrics['start_time'] is None:
                performance_metrics['start_time'] = datetime.now()
            performance_metrics['completed_jobs'] = metrics['completed']
            performance_metrics['total_jobs'] = metrics['total'] or 0
            performance_metrics['current_speed'] = metrics['speed']
            performance_metrics['avg_speed'] = metrics['avg_speed']
            performance_metrics['peak_speed'] = metrics['max_speed']
            performance_metrics['min_speed'] = metrics['min_speed']
            
        except Exception as e:
            print(f"[ERROR] 메트릭 수집 오류: {e}")
            import traceback
            traceback.print_exc()
        
        time.sleep(5)  # 5초마다 업데이트

def get_system_resources():
    """시스템 리소스 사용량 조회 (기존 호환성 유지)"""
    resources = get_comprehensive_resources()
    if resources:
        # 로드 평균 비율 계산
        load_1min = resources.get('load_1min', 0)
        cpu_count = 96  # 시스템 코어 수
        load_ratio = (load_1min / cpu_count * 100) if cpu_count > 0 else 0
        
        return {
            'load_1min': load_1min,
            'load_ratio': load_ratio,
            'cpu_usage': resources.get('cpu_usage', 0),  # 호환성 유지
            'memory_used_mb': resources.get('memory_used_mb', 0),
            'timestamp': resources.get('timestamp', datetime.now().isoformat())
        }
    return None

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>고급 tmux 모니터링 대시보드</title>
    <script src="https://cdn.socket.io/4.5.0/socket.io.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        :root {
            --bg-primary: #0a0e14;
            --bg-secondary: #1a1f2e;
            --bg-card: #1e2432;
            --bg-hover: #252b3a;
            --border-color: #2d3440;
            --text-primary: #e4e7eb;
            --text-secondary: #9ca3af;
            --text-muted: #6b7280;
            --accent-blue: #58a6ff;
            --accent-green: #3fb950;
            --accent-red: #f85149;
            --accent-orange: #f0883e;
            --accent-purple: #a5a5ff;
            --shadow-sm: 0 2px 4px rgba(0, 0, 0, 0.2);
            --shadow-md: 0 4px 6px rgba(0, 0, 0, 0.3);
            --shadow-lg: 0 8px 16px rgba(0, 0, 0, 0.4);
            --shadow-xl: 0 12px 24px rgba(0, 0, 0, 0.5);
        }
        
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        @keyframes slideUp {
            from { opacity: 0; transform: translateY(20px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.7; }
        }
        
        @keyframes shimmer {
            0% { background-position: -1000px 0; }
            100% { background-position: 1000px 0; }
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Helvetica Neue', sans-serif;
            background: linear-gradient(135deg, var(--bg-primary) 0%, #0f1419 100%);
            color: var(--text-primary);
            padding: 20px;
            min-height: 100vh;
            line-height: 1.6;
            letter-spacing: 0.01em;
        }
        .project-selector {
            position: fixed;
            top: 20px;
            right: 20px;
            z-index: 1000;
            display: flex;
            gap: 8px;
            align-items: center;
        }
        .project-selector select, .project-selector button {
            padding: 8px 12px;
            background: var(--bg-card);
            color: var(--text-primary);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            cursor: pointer;
            font-size: 14px;
        }
        .project-selector button {
            background: var(--accent-blue);
            color: white;
            border: none;
        }
        .project-selector button:hover {
            background: var(--accent-purple);
        }
        .modal {
            display: none;
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.8);
            z-index: 2000;
            overflow-y: auto;
        }
        .modal-content {
            max-width: 900px;
            margin: 50px auto;
            background: var(--bg-card);
            border-radius: 12px;
            padding: 24px;
            border: 1px solid var(--border-color);
        }
        .modal-content h2 {
            margin-bottom: 20px;
            color: var(--text-primary);
        }
        .form-group {
            margin-bottom: 16px;
        }
        .form-group label {
            display: block;
            margin-bottom: 8px;
            color: var(--text-secondary);
        }
        .form-group input {
            width: 100%;
            padding: 8px;
            background: var(--bg-secondary);
            color: var(--text-primary);
            border: 1px solid var(--border-color);
            border-radius: 6px;
        }
        .form-group .file-input-group {
            display: flex;
            gap: 8px;
        }
        .form-group .file-input-group input {
            flex: 1;
        }
        .form-group .file-input-group button {
            padding: 8px 16px;
            background: var(--accent-blue);
            color: white;
            border: none;
            border-radius: 6px;
            cursor: pointer;
        }
        .file-tree-container {
            max-height: 400px;
            overflow-y: auto;
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 6px;
            padding: 12px;
            margin-bottom: 16px;
        }
        .file-tree-item {
            padding: 4px 8px;
            cursor: pointer;
            border-radius: 4px;
            margin-bottom: 2px;
            user-select: none;
        }
        .file-tree-item:hover {
            background: var(--bg-hover);
        }
        .file-tree-item.directory {
            font-weight: 500;
        }
        .file-tree-item.file {
            padding-left: 24px;
        }
        .modal-actions {
            display: flex;
            gap: 8px;
            justify-content: flex-end;
        }
        .modal-actions button {
            padding: 8px 16px;
            border-radius: 6px;
            cursor: pointer;
            border: none;
        }
        .btn-cancel {
            background: var(--bg-secondary);
            color: var(--text-primary);
            border: 1px solid var(--border-color);
        }
        .btn-save {
            background: var(--accent-green);
            color: white;
        }
        .header {
            background: linear-gradient(135deg, var(--bg-card) 0%, var(--bg-secondary) 100%);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 24px;
            box-shadow: var(--shadow-lg);
            animation: fadeIn 0.6s ease-out;
            backdrop-filter: blur(10px);
        }
        .header h1 {
            background: linear-gradient(135deg, var(--accent-blue) 0%, var(--accent-purple) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 16px;
            font-size: 32px;
            font-weight: 700;
            letter-spacing: -0.02em;
        }
        .status-bar {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 16px;
            margin-bottom: 20px;
        }
        .stat-card {
            background: linear-gradient(135deg, var(--bg-card) 0%, var(--bg-secondary) 100%);
            border: 1px solid var(--border-color);
            border-radius: 10px;
            padding: 20px;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            box-shadow: var(--shadow-md);
            position: relative;
            overflow: hidden;
            animation: slideUp 0.5s ease-out backwards;
        }
        .stat-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 3px;
            background: linear-gradient(90deg, var(--accent-blue), var(--accent-green));
            opacity: 0;
            transition: opacity 0.3s;
        }
        .stat-card:hover {
            transform: translateY(-4px);
            box-shadow: var(--shadow-xl);
            border-color: var(--accent-blue);
        }
        .stat-card:hover::before {
            opacity: 1;
        }
        .stat-card:nth-child(1) { animation-delay: 0.1s; }
        .stat-card:nth-child(2) { animation-delay: 0.2s; }
        .stat-card:nth-child(3) { animation-delay: 0.3s; }
        .stat-card:nth-child(4) { animation-delay: 0.4s; }
        .stat-card:nth-child(5) { animation-delay: 0.5s; }
        .stat-card:nth-child(6) { animation-delay: 0.6s; }
        .stat-value {
            font-size: 32px;
            font-weight: 700;
            background: linear-gradient(135deg, var(--accent-blue) 0%, var(--accent-purple) 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            line-height: 1.2;
            transition: transform 0.3s;
        }
        .stat-card:hover .stat-value {
            transform: scale(1.05);
        }
        .stat-label {
            color: var(--text-secondary);
            font-size: 13px;
            margin-top: 8px;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        .progress-bar-container {
            background: var(--bg-secondary);
            border-radius: 10px;
            height: 36px;
            margin-top: 16px;
            overflow: hidden;
            border: 1px solid var(--border-color);
            box-shadow: inset 0 2px 4px rgba(0, 0, 0, 0.2);
            position: relative;
        }
        .progress-bar-fill {
            background: linear-gradient(90deg, var(--accent-blue) 0%, var(--accent-green) 50%, var(--accent-blue) 100%);
            background-size: 200% 100%;
            animation: shimmer 2s linear infinite;
            height: 100%;
            transition: width 0.6s cubic-bezier(0.4, 0, 0.2, 1);
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: 700;
            font-size: 14px;
            text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
            box-shadow: 0 2px 8px rgba(88, 166, 255, 0.4);
        }
        .dashboard {
            display: grid;
            grid-template-columns: 1fr;
            gap: 32px;
            margin-bottom: 32px;
        }
        .card {
            background: linear-gradient(135deg, var(--bg-card) 0%, var(--bg-secondary) 100%);
            border: 1px solid var(--border-color);
            border-radius: 12px;
            padding: 24px;
            box-shadow: var(--shadow-md);
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            animation: fadeIn 0.6s ease-out backwards;
            position: relative;
            overflow: hidden;
        }
        .card::after {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 2px;
            background: linear-gradient(90deg, var(--accent-blue), var(--accent-green), var(--accent-blue));
            background-size: 200% 100%;
            animation: shimmer 3s linear infinite;
        }
        .card:hover {
            transform: translateY(-2px);
            box-shadow: var(--shadow-lg);
            border-color: var(--accent-blue);
        }
        .card h3 {
            color: var(--accent-blue);
            margin-bottom: 20px;
            font-size: 18px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        .chart-container {
            position: relative;
            height: 400px;
            padding: 16px;
            overflow-x: auto;
            overflow-y: hidden;
            cursor: pointer;
            transition: transform 0.2s;
            width: 100%;
            background: var(--bg-secondary);
            border-radius: 8px;
            box-sizing: border-box;
        }
        .chart-container:hover {
            transform: scale(1.01);
        }
        .chart-container::-webkit-scrollbar {
            height: 10px;
        }
        .chart-container::-webkit-scrollbar-track {
            background: var(--bg-secondary);
            border-radius: 5px;
        }
        .chart-container::-webkit-scrollbar-thumb {
            background: var(--border-color);
            border-radius: 5px;
        }
        .chart-container::-webkit-scrollbar-thumb:hover {
            background: var(--accent-blue);
        }
        .chart-wrapper {
            min-width: 100%;
            width: 100%;
            height: 380px;
            position: relative;
        }
        .chart-container canvas {
            max-width: none !important;
            width: 100% !important;
            height: 380px !important;
            display: block;
        }
        #log-container {
            background: #000;
            padding: 16px;
            border-radius: 8px;
            height: 400px;
            overflow-y: auto;
            font-family: 'SF Mono', 'Monaco', 'Cascadia Code', 'Courier New', monospace;
            font-size: 12px;
            white-space: pre-wrap;
            line-height: 1.6;
            border: 1px solid var(--border-color);
            box-shadow: inset 0 2px 8px rgba(0, 0, 0, 0.3);
        }
        #log-container::-webkit-scrollbar {
            width: 8px;
        }
        #log-container::-webkit-scrollbar-track {
            background: var(--bg-secondary);
            border-radius: 4px;
        }
        #log-container::-webkit-scrollbar-thumb {
            background: var(--border-color);
            border-radius: 4px;
        }
        #log-container::-webkit-scrollbar-thumb:hover {
            background: var(--accent-blue);
        }
        .log-line {
            margin: 3px 0;
            padding: 4px 8px;
            border-radius: 4px;
            transition: background 0.2s;
        }
        .log-line:hover {
            background: rgba(88, 166, 255, 0.1);
        }
        .error-line {
            color: var(--accent-red);
            background: rgba(248, 81, 73, 0.05);
        }
        .progress-line {
            color: var(--accent-blue);
            font-weight: 600;
            background: rgba(88, 166, 255, 0.05);
        }
        .alert-panel {
            background: linear-gradient(135deg, rgba(248, 81, 73, 0.1) 0%, rgba(240, 136, 62, 0.1) 100%);
            border: 1px solid var(--accent-red);
            border-radius: 10px;
            padding: 16px;
            margin-bottom: 20px;
            max-height: 200px;
            overflow-y: auto;
            box-shadow: var(--shadow-md);
            animation: slideUp 0.4s ease-out, pulse 2s ease-in-out infinite;
        }
        .alert-panel h4 {
            display: flex;
            align-items: center;
            gap: 8px;
            margin-bottom: 12px;
        }
        .alert-item {
            padding: 12px;
            margin: 8px 0;
            border-radius: 8px;
            border-left: 4px solid;
            background: rgba(0, 0, 0, 0.2);
            transition: all 0.3s;
            animation: slideUp 0.3s ease-out;
        }
        .alert-item:hover {
            transform: translateX(4px);
            box-shadow: var(--shadow-sm);
        }
        .alert-warning {
            background: linear-gradient(90deg, rgba(240, 136, 62, 0.15) 0%, rgba(240, 136, 62, 0.05) 100%);
            border-color: var(--accent-orange);
        }
        .alert-critical {
            background: linear-gradient(90deg, rgba(248, 81, 73, 0.15) 0%, rgba(248, 81, 73, 0.05) 100%);
            border-color: var(--accent-red);
        }
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
            gap: 12px;
            margin-bottom: 20px;
        }
        .metric-item {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 8px;
            padding: 16px;
            transition: all 0.3s;
            box-shadow: var(--shadow-sm);
            animation: fadeIn 0.5s ease-out backwards;
        }
        .metric-item:nth-child(1) { animation-delay: 0.1s; }
        .metric-item:nth-child(2) { animation-delay: 0.2s; }
        .metric-item:nth-child(3) { animation-delay: 0.3s; }
        .metric-item:nth-child(4) { animation-delay: 0.4s; }
        .metric-item:nth-child(5) { animation-delay: 0.5s; }
        .metric-item:nth-child(6) { animation-delay: 0.6s; }
        .metric-item:nth-child(7) { animation-delay: 0.7s; }
        .metric-item:nth-child(8) { animation-delay: 0.8s; }
        .metric-item:hover {
            transform: translateY(-2px);
            box-shadow: var(--shadow-md);
            border-color: var(--accent-blue);
            background: var(--bg-card);
        }
        .metric-label {
            color: var(--text-secondary);
            font-size: 11px;
            margin-bottom: 8px;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.05em;
        }
        .metric-value {
            color: var(--accent-blue);
            font-size: 22px;
            font-weight: 700;
            transition: transform 0.3s;
        }
        .metric-item:hover .metric-value {
            transform: scale(1.1);
        }
        .error-list {
            max-height: 300px;
            overflow-y: auto;
            font-size: 12px;
        }
        .error-list::-webkit-scrollbar {
            width: 6px;
        }
        .error-list::-webkit-scrollbar-track {
            background: var(--bg-secondary);
        }
        .error-list::-webkit-scrollbar-thumb {
            background: var(--border-color);
            border-radius: 3px;
        }
        .error-item {
            padding: 12px;
            margin: 8px 0;
            background: linear-gradient(90deg, rgba(248, 81, 73, 0.1) 0%, var(--bg-secondary) 100%);
            border-radius: 8px;
            border-left: 4px solid var(--accent-red);
            transition: all 0.3s;
            animation: slideUp 0.3s ease-out;
        }
        .error-item:hover {
            transform: translateX(4px);
            background: linear-gradient(90deg, rgba(248, 81, 73, 0.15) 0%, var(--bg-card) 100%);
            box-shadow: var(--shadow-sm);
        }
        .error-item strong {
            color: var(--accent-red);
        }
        .error-item small {
            color: var(--text-muted);
            font-size: 10px;
        }
        
        /* 반응형 디자인 */
        @media (max-width: 768px) {
            body { padding: 12px; }
            .header { padding: 16px; }
            .header h1 { font-size: 24px; }
            .dashboard { grid-template-columns: 1fr; gap: 16px; }
            .status-bar { grid-template-columns: repeat(2, 1fr); gap: 12px; }
            .metrics-grid { grid-template-columns: repeat(2, 1fr); gap: 8px; }
            .stat-card { padding: 16px; }
            .stat-value { font-size: 24px; }
            .card { padding: 16px; }
            .chart-container { height: 250px; }
        }
        
        @media (max-width: 480px) {
            .status-bar { grid-template-columns: 1fr; }
            .metrics-grid { grid-template-columns: 1fr; }
        }
        
        /* 팝업 모달 스타일 */
        .chart-modal {
            display: none;
            position: fixed;
            z-index: 10000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            background: rgba(0, 0, 0, 0.85);
            backdrop-filter: blur(5px);
            animation: fadeIn 0.3s;
        }
        .chart-modal.active {
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .chart-modal-content {
            background: var(--bg-card);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 32px;
            max-width: 90%;
            max-height: 90%;
            width: 1200px;
            box-shadow: var(--shadow-xl);
            position: relative;
            animation: slideUp 0.4s ease-out;
        }
        .chart-modal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 24px;
        }
        .chart-modal-header h3 {
            color: var(--accent-blue);
            font-size: 24px;
            margin: 0;
        }
        .chart-modal-close {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            color: var(--text-primary);
            font-size: 24px;
            width: 40px;
            height: 40px;
            border-radius: 8px;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            transition: all 0.3s;
        }
        .chart-modal-close:hover {
            background: var(--accent-red);
            border-color: var(--accent-red);
            transform: rotate(90deg);
        }
        .chart-modal-body {
            height: 600px;
            overflow-x: auto;
            overflow-y: hidden;
        }
        .chart-modal-body canvas {
            max-width: none !important;
        }
        
        /* 코어별 진행 상황 스타일 */
        .core-progress-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-top: 20px;
        }
        .core-progress-card {
            background: var(--bg-secondary);
            border: 1px solid var(--border-color);
            border-radius: 10px;
            padding: 16px;
            transition: all 0.3s;
            box-shadow: var(--shadow-sm);
        }
        .core-progress-card:hover {
            transform: translateY(-2px);
            box-shadow: var(--shadow-md);
            border-color: var(--accent-blue);
        }
        .core-progress-label {
            color: var(--text-secondary);
            font-size: 12px;
            margin-bottom: 8px;
            font-weight: 500;
        }
        .core-progress-value {
            color: var(--accent-blue);
            font-size: 20px;
            font-weight: 700;
        }
        .core-progress-bar {
            margin-top: 12px;
            height: 8px;
            background: var(--bg-primary);
            border-radius: 4px;
            overflow: hidden;
        }
        .core-progress-fill {
            height: 100%;
            background: linear-gradient(90deg, var(--accent-blue), var(--accent-green));
            transition: width 0.6s cubic-bezier(0.4, 0, 0.2, 1);
        }
        .core-expand-button {
            background: var(--bg-card);
            border: 1px solid var(--accent-blue);
            color: var(--accent-blue);
            padding: 10px 20px;
            border-radius: 8px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 600;
            transition: all 0.3s;
            margin-top: 16px;
            width: 100%;
        }
        .core-expand-button:hover {
            background: var(--accent-blue);
            color: white;
            transform: translateY(-2px);
            box-shadow: var(--shadow-md);
        }
        .core-all-view {
            display: none;
        }
        .core-all-view.active {
            display: grid;
        }
    </style>
</head>
<body>
    <!-- 프로젝트 선택 UI -->
    <div class="project-selector">
        <select id="project-dropdown">
            <option value="">프로젝트 선택...</option>
        </select>
        <button id="add-project-btn">+ 새 프로젝트</button>
    </div>
    
    <!-- 프로젝트 등록 모달 -->
    <div id="project-modal" class="modal">
        <div class="modal-content" style="max-height: 90vh; overflow-y: auto;">
            <h2>프로젝트 등록</h2>
            
            <div class="form-group">
                <label>프로젝트 이름:</label>
                <input type="text" id="project-name" placeholder="예: hr_analysis_fu9">
            </div>
            
            <div class="form-group">
                <label>TMUX 세션 이름:</label>
                <div class="file-input-group">
                    <input type="text" id="tmux-session" placeholder="예: hr_validation">
                    <button id="auto-detect-btn" style="background: var(--accent-green);">🔍 자동 감지</button>
                </div>
            </div>
            
            <!-- 자동 감지된 설정 표시 영역 -->
            <div id="detected-config" style="display: none; margin-top: 20px; padding: 16px; background: var(--bg-secondary); border-radius: 8px; border: 1px solid var(--border-color);">
                <h3 style="margin-top: 0; margin-bottom: 12px; color: var(--accent-blue);">자동 감지된 설정</h3>
                <div id="detected-config-list" style="font-family: monospace; font-size: 12px; line-height: 1.8;">
                    <!-- 동적으로 채워짐 -->
                </div>
            </div>
            
            <div class="modal-actions" style="margin-top: 20px;">
                <button class="btn-cancel" id="cancel-project">취소</button>
                <button class="btn-save" id="save-project">등록</button>
            </div>
        </div>
    </div>
    
    <div class="header">
        <h1>🚀 포괄적 모니터링 대시보드</h1>
        
        <!-- 프로젝트 정보 표시 영역 -->
        <div id="project-info-panel" style="background: linear-gradient(135deg, var(--bg-secondary) 0%, var(--bg-card) 100%); border: 1px solid var(--border-color); border-radius: 8px; padding: 16px; margin-bottom: 16px; display: none; animation: fadeIn 0.3s ease-out;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                <div style="flex: 1;">
                    <div style="display: flex; align-items: center; gap: 8px;">
                        <span style="font-size: 18px;">📊</span>
                        <div>
                            <strong style="color: var(--accent-blue); font-size: 16px;">프로젝트:</strong> 
                            <span id="project-info-name" style="color: var(--text-primary); font-size: 16px; font-weight: 600;"></span>
                        </div>
                    </div>
                </div>
                <div style="font-size: 12px; color: var(--text-secondary); background: var(--bg-primary); padding: 6px 12px; border-radius: 6px;">
                    <span id="project-info-session"></span>
                </div>
            </div>
            <div style="padding: 10px 12px; background: rgba(248, 81, 73, 0.15); border-left: 3px solid var(--accent-red); border-radius: 4px; font-size: 13px; color: var(--accent-orange); display: flex; align-items: center; gap: 8px;">
                <span>⚠️</span>
                <div style="flex: 1;">
                    <strong>프로젝트 변경을 적용하려면 서버를 재시작하세요.</strong>
                    <div style="margin-top: 4px; font-family: monospace; font-size: 11px; color: var(--text-secondary);">
                        <code style="background: var(--bg-primary); padding: 4px 8px; border-radius: 4px; display: inline-block; margin-top: 4px;">pkill -f tmux_monitor_advanced.py && python3 tmux_monitor_advanced.py</code>
                    </div>
                </div>
            </div>
        </div>
        
        <div id="alert-panel" class="alert-panel" style="display: none;">
            <h4 style="color: var(--accent-red);">⚠️ 알림</h4>
            <div id="alert-list"></div>
        </div>
        <div class="status-bar">
            <div class="stat-card">
                <div class="stat-value" id="progress-value">0%</div>
                <div class="stat-label">📊 진행률</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="completed-value">0</div>
                <div class="stat-label">✅ 완료 작업</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="total-value">-</div>
                <div class="stat-label">📋 전체 작업</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="failed-value">0</div>
                <div class="stat-label">❌ 실패 작업</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="speed-value">0.0</div>
                <div class="stat-label">⚡ 처리 속도 (jobs/min)</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" id="eta-value" style="font-size: 18px;">계산 중...</div>
                <div class="stat-label">⏱️ 예상 완료 시간</div>
            </div>
        </div>
        <div class="progress-bar-container">
            <div class="progress-bar-fill" id="progress-bar" style="width: 0%">0%</div>
        </div>
        <div class="metrics-grid" style="margin-top: 20px;">
            <div class="metric-item">
                <div class="metric-label">⏰ 경과 시간</div>
                <div class="metric-value" id="elapsed-time">0분</div>
            </div>
            <div class="metric-item">
                <div class="metric-label">📈 평균 속도 (jobs/min)</div>
                <div class="metric-value" id="avg-speed">0.0</div>
            </div>
            <div class="metric-item">
                <div class="metric-label">⬆️ 최대 속도 (jobs/min)</div>
                <div class="metric-value" id="max-speed">0.0</div>
            </div>
            <div class="metric-item">
                <div class="metric-label">⬇️ 최소 속도 (jobs/min)</div>
                <div class="metric-value" id="min-speed">0.0</div>
            </div>
            <div class="metric-item">
                <div class="metric-label">🔴 에러 수</div>
                <div class="metric-value" id="error-count">0</div>
            </div>
            <div class="metric-item">
                <div class="metric-label">📉 에러율</div>
                <div class="metric-value" id="error-rate">0.0%</div>
            </div>
            <div class="metric-item">
                <div class="metric-label">🔄 프로세스 수</div>
                <div class="metric-value" id="process-count">0</div>
            </div>
            <div class="metric-item">
                <div class="metric-label">🔄 재시작 횟수</div>
                <div class="metric-value" id="restart-count">0</div>
            </div>
        </div>
    </div>
    
    <div class="dashboard">
        <div class="card">
            <h3>📊 진행률 추이</h3>
            <div class="chart-container" onclick="openChartModal('progressChart', '진행률 추이')">
                <div class="chart-wrapper">
                    <canvas id="progressChart"></canvas>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h3>⚡ 처리 속도 (jobs/min)</h3>
            <div class="chart-container" onclick="openChartModal('speedChart', '처리 속도 (jobs/min)')">
                <div class="chart-wrapper">
                    <canvas id="speedChart"></canvas>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h3>🔴 에러 발생 추이</h3>
            <div class="chart-container" onclick="openChartModal('errorChart', '에러 발생 추이')">
                <div class="chart-wrapper">
                    <canvas id="errorChart"></canvas>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h3>💻 시스템 리소스</h3>
            <div class="chart-container" onclick="openChartModal('resourceChart', '시스템 리소스 (로드 평균 + 메모리)')">
                <div class="chart-wrapper">
                    <canvas id="resourceChart"></canvas>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h3>⚡ 시스템 로드 평균</h3>
            <div class="chart-container" onclick="openChartModal('cpuChart', '시스템 로드 평균')">
                <div class="chart-wrapper">
                    <canvas id="cpuChart"></canvas>
                </div>
            </div>
        </div>
        
        <div class="card">
            <h3>💾 메모리 사용률</h3>
            <div class="chart-container" onclick="openChartModal('memoryChart', '메모리 사용률')">
                <div class="chart-wrapper">
                    <canvas id="memoryChart"></canvas>
                </div>
            </div>
        </div>
    </div>
    
    <div class="dashboard">
        <div class="card">
            <h3>📋 에러 통계</h3>
            <div id="error-types" style="margin-bottom: 15px;"></div>
            <h4 style="color: var(--text-secondary); margin-bottom: 10px; margin-top: 16px;">최근 에러</h4>
            <div class="error-list" id="recent-errors"></div>
        </div>
        
        <div class="card">
            <h3>🔄 프로세스 상태</h3>
            <div id="process-status"></div>
        </div>
    </div>
    
    <div class="card">
        <h3>🖥️ CPU 코어별 진행 상황</h3>
        <div class="core-progress-grid" id="core-progress-grid">
            <!-- 활성 CPU 코어별 진행 상황이 여기에 동적으로 추가됩니다 -->
        </div>
        <div id="core-all-view" class="core-all-view core-progress-grid">
            <!-- 전체 CPU 코어별 진행 상황 (열기 버튼 클릭 시 표시) -->
        </div>
        <button id="core-expand-btn" class="core-expand-button" onclick="toggleAllCores()" style="display: none;">
            전체 코어 보기
        </button>
    </div>
    
    <div class="card">
        <h3>📝 실시간 로그</h3>
        <div id="log-container"></div>
    </div>
    
    <!-- 차트 팝업 모달 -->
    <div id="chartModal" class="chart-modal" onclick="closeChartModal(event)">
        <div class="chart-modal-content" onclick="event.stopPropagation()">
            <div class="chart-modal-header">
                <h3 id="modalChartTitle">차트</h3>
                <button class="chart-modal-close" onclick="closeChartModal()">×</button>
            </div>
            <div class="chart-modal-body">
                <canvas id="modalChart"></canvas>
            </div>
        </div>
    </div>

    <script>
        // 숫자 카운트업 애니메이션 함수
        function animateValue(element, start, end, duration, suffix = '') {
            if (!element) return;
            const startTime = performance.now();
            const isNumber = typeof end === 'number';
            const endNum = isNumber ? end : parseFloat(end) || 0;
            
            function update(currentTime) {
                const elapsed = currentTime - startTime;
                const progress = Math.min(elapsed / duration, 1);
                const easeOutQuart = 1 - Math.pow(1 - progress, 4);
                const current = start + (endNum - start) * easeOutQuart;
                
                if (isNumber) {
                    if (suffix === '%') {
                        element.textContent = current.toFixed(1) + suffix;
                    } else if (suffix.includes('jobs/sec') || suffix.includes('속도')) {
                        element.textContent = current.toFixed(2) + (suffix ? ' ' + suffix : '');
                    } else {
                        element.textContent = Math.floor(current).toLocaleString() + (suffix ? ' ' + suffix : '');
                    }
                } else {
                    element.textContent = end;
                }
                
                if (progress < 1) {
                    requestAnimationFrame(update);
                } else {
                    element.textContent = end + (suffix ? ' ' + suffix : '');
                }
            }
            
            requestAnimationFrame(update);
        }
        
        // 이전 값 저장
        const previousValues = {};
        
        // 값 업데이트 헬퍼 함수
        function updateValueWithAnimation(elementId, newValue, suffix = '', duration = 800) {
            const element = document.getElementById(elementId);
            if (!element) return;
            
            const prevValue = previousValues[elementId] || 0;
            const numValue = typeof newValue === 'number' ? newValue : parseFloat(newValue) || 0;
            
            if (Math.abs(numValue - prevValue) > 0.01) {
                animateValue(element, prevValue, numValue, duration, suffix);
                previousValues[elementId] = numValue;
            } else {
                element.textContent = newValue + (suffix ? ' ' + suffix : '');
            }
        }
        
        // Chart.js 설정 (개선된 스타일)
        const chartOptions = {
            responsive: true, // 반응형으로 변경 (화면 너비 활용)
            maintainAspectRatio: false,
            animation: {
                duration: 750,
                easing: 'easeOutQuart'
            },
            plugins: {
                legend: { 
                    display: true,
                    position: 'top',
                    labels: { 
                        color: '#e4e7eb',
                        font: { size: 13, weight: '500' },
                        padding: 15,
                        usePointStyle: true,
                        boxWidth: 12
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(30, 36, 50, 0.95)',
                    titleColor: '#e4e7eb',
                    bodyColor: '#e4e7eb',
                    borderColor: '#2d3440',
                    borderWidth: 1,
                    padding: 12,
                    cornerRadius: 8,
                    displayColors: true
                }
            },
            scales: {
                x: { 
                    type: 'category',
                    ticks: { 
                        color: '#9ca3af',
                        font: { size: 12 },
                        maxRotation: 0,
                        minRotation: 0,
                        autoSkip: true,
                        maxTicksLimit: 15
                    }, 
                    grid: { 
                        color: '#2d3440',
                        lineWidth: 1
                    },
                    border: { color: '#2d3440' }
                },
                y: { 
                    ticks: { 
                        color: '#9ca3af',
                        font: { size: 12 },
                        padding: 8
                    }, 
                    grid: { 
                        color: '#2d3440',
                        lineWidth: 1
                    },
                    border: { color: '#2d3440' }
                }
            }
        };
        
        const progressChart = new Chart(document.getElementById('progressChart'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: '진행률 (%)',
                    data: [],
                    borderColor: '#58a6ff',
                    backgroundColor: 'rgba(88, 166, 255, 0.2)',
                    borderWidth: 2,
                    tension: 0.5,
                    fill: true,
                    pointRadius: 3,
                    pointHoverRadius: 5,
                    pointBackgroundColor: '#58a6ff',
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 2
                }]
            },
            options: { 
                ...chartOptions,
                interaction: {
                    intersect: false,
                    mode: 'index'
                },
                scales: { 
                    ...chartOptions.scales,
                    x: {
                        ...chartOptions.scales.x,
                        min: undefined,
                        max: undefined
                    },
                    y: { 
                        ...chartOptions.scales.y, 
                        max: 100,
                        min: 0
                    } 
                } 
            }
        });
        
        const speedChart = new Chart(document.getElementById('speedChart'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: '처리 속도 (jobs/min)',
                    data: [],
                    borderColor: '#3fb950',
                    backgroundColor: 'rgba(63, 185, 80, 0.2)',
                    borderWidth: 2,
                    tension: 0.5,
                    fill: true,
                    pointRadius: 3,
                    pointHoverRadius: 5,
                    pointBackgroundColor: '#3fb950',
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 2
                }]
            },
            options: {
                ...chartOptions,
                scales: {
                    ...chartOptions.scales,
                    x: {
                        ...chartOptions.scales.x,
                        min: undefined
                    }
                }
            }
        });
        
        const errorChart = new Chart(document.getElementById('errorChart'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: '에러 발생',
                    data: [],
                    borderColor: '#f85149',
                    backgroundColor: 'rgba(248, 81, 73, 0.2)',
                    borderWidth: 2,
                    tension: 0.5,
                    fill: true,
                    pointRadius: 3,
                    pointHoverRadius: 5,
                    pointBackgroundColor: '#f85149',
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 2
                }]
            },
            options: {
                ...chartOptions,
                interaction: {
                    intersect: false,
                    mode: 'index'
                },
                scales: {
                    ...chartOptions.scales,
                    x: {
                        ...chartOptions.scales.x,
                        min: undefined,
                        max: undefined
                    }
                }
            }
        });
        
        const resourceChart = new Chart(document.getElementById('resourceChart'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [
                    {
                        label: '로드 평균 비율 (%)',
                        data: [],
                        borderColor: '#f0883e',
                        backgroundColor: 'rgba(240, 136, 62, 0.2)',
                        borderWidth: 2,
                        tension: 0.5,
                        fill: true,
                        pointRadius: 3,
                        pointHoverRadius: 5,
                        pointBackgroundColor: '#f0883e',
                        pointBorderColor: '#ffffff',
                        pointBorderWidth: 2,
                        yAxisID: 'y'
                    },
                    {
                        label: '메모리 사용 (GB)',
                        data: [],
                        borderColor: '#a5a5ff',
                        backgroundColor: 'rgba(165, 165, 255, 0.2)',
                        borderWidth: 2,
                        tension: 0.5,
                        fill: true,
                        pointRadius: 3,
                        pointHoverRadius: 5,
                        pointBackgroundColor: '#a5a5ff',
                        pointBorderColor: '#ffffff',
                        pointBorderWidth: 2,
                        yAxisID: 'y1'
                    }
                ]
            },
            options: {
                ...chartOptions,
                interaction: {
                    intersect: false,
                    mode: 'index'
                },
                scales: {
                    ...chartOptions.scales,
                    x: {
                        ...chartOptions.scales.x,
                        min: undefined,
                        max: undefined
                    },
                    y: { 
                        ...chartOptions.scales.y, 
                        position: 'left',
                        max: 120,
                        min: 0,
                        title: {
                            display: true,
                            text: '로드 비율 (%)',
                            color: '#9ca3af'
                        }
                    },
                    y1: { 
                        ...chartOptions.scales.y, 
                        position: 'right', 
                        grid: { drawOnChartArea: false },
                        title: {
                            display: true,
                            text: '메모리 (GB)',
                            color: '#9ca3af'
                        }
                    }
                }
            }
        });
        
        // 시스템 코어 수 (로드 평균 비율 계산용)
        const SYSTEM_CORES = 96; // 실제 코어 수로 설정됨
        
        const cpuChart = new Chart(document.getElementById('cpuChart'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: '로드 평균 (1분)',
                    data: [],
                    borderColor: '#f0883e',
                    backgroundColor: 'rgba(240, 136, 62, 0.2)',
                    borderWidth: 2,
                    tension: 0.5,
                    fill: true,
                    pointRadius: 3,
                    pointHoverRadius: 5,
                    pointBackgroundColor: '#f0883e',
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 2
                }, {
                    label: '로드 평균 비율 (%)',
                    data: [],
                    borderColor: '#ff6b6b',
                    backgroundColor: 'rgba(255, 107, 107, 0.1)',
                    borderWidth: 2,
                    tension: 0.5,
                    fill: false,
                    pointRadius: 2,
                    pointHoverRadius: 4,
                    pointBackgroundColor: '#ff6b6b',
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 1,
                    yAxisID: 'y1'
                }]
            },
            options: { 
                ...chartOptions,
                interaction: {
                    intersect: false,
                    mode: 'index'
                },
                scales: { 
                    ...chartOptions.scales,
                    x: {
                        ...chartOptions.scales.x,
                        min: undefined,
                        max: undefined
                    },
                    y: { 
                        ...chartOptions.scales.y,
                        position: 'left',
                        title: {
                            display: true,
                            text: '로드 평균',
                            color: '#9ca3af'
                        },
                        max: SYSTEM_CORES * 1.2, // 코어 수의 120%까지 표시
                        min: 0
                    },
                    y1: {
                        ...chartOptions.scales.y,
                        position: 'right',
                        title: {
                            display: true,
                            text: '로드 비율 (%)',
                            color: '#9ca3af'
                        },
                        max: 120,
                        min: 0,
                        grid: {
                            drawOnChartArea: false
                        }
                    }
                } 
            }
        });
        
        const memoryChart = new Chart(document.getElementById('memoryChart'), {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: '메모리 사용률 (%)',
                    data: [],
                    borderColor: '#a5a5ff',
                    backgroundColor: 'rgba(165, 165, 255, 0.2)',
                    borderWidth: 2,
                    tension: 0.5,
                    fill: true,
                    pointRadius: 3,
                    pointHoverRadius: 5,
                    pointBackgroundColor: '#a5a5ff',
                    pointBorderColor: '#ffffff',
                    pointBorderWidth: 2
                }]
            },
            options: { 
                ...chartOptions, 
                scales: { 
                    ...chartOptions.scales, 
                    y: { 
                        ...chartOptions.scales.y, 
                        max: 100,
                        min: 0
                    } 
                } 
            }
        });
        
        // WebSocket 연결
        const socket = io();
        const logContainer = document.getElementById('log-container');
        let errorCount = 0;
        
        socket.on('connect', function() {
            console.log('WebSocket 연결됨');
            addLogLine('[시스템] 모니터링 대시보드에 연결되었습니다.', 'progress-line');
        });
        
        socket.on('log_update', function(data) {
            // 통계 업데이트
            if (data.progress !== undefined) {
                document.getElementById('progress-value').textContent = data.progress + '%';
                document.getElementById('progress-bar').style.width = data.progress + '%';
                document.getElementById('progress-bar').textContent = data.progress + '%';
                updateChart(progressChart, data.progress, data.timestamp);
            }
            
            if (data.completed !== undefined) {
                document.getElementById('completed-value').textContent = data.completed.toLocaleString();
            }
            
            if (data.speed !== undefined) {
                document.getElementById('speed-value').textContent = data.speed.toFixed(2);
                updateChart(speedChart, data.speed, data.timestamp);
            }
            
            if (data.eta !== undefined) {
                document.getElementById('eta-value').textContent = data.eta;
            }
            
            // 에러 카운트
            if (data.error) {
                errorCount++;
                updateChart(errorChart, errorCount, data.timestamp);
            }
            
            // 로그 추가
            if (data.raw_line) {
                addLogLine(data.raw_line, data.error ? 'error-line' : 'progress-line');
            }
        });
        
        socket.on('raw_log', function(data) {
            if (data && data.line) {
                addLogLine(data.line);
            }
        });
        
        socket.on('metrics_update', function(data) {
            // 작업 진행 상황 (애니메이션 적용)
            if (data.progress !== undefined) {
                updateValueWithAnimation('progress-value', data.progress, '%');
                const progressBar = document.getElementById('progress-bar');
                if (progressBar) {
                    progressBar.style.width = data.progress + '%';
                    progressBar.textContent = data.progress.toFixed(1) + '%';
                }
                updateChart(progressChart, data.progress, data.timestamp);
            }
            
            if (data.completed !== undefined) {
                updateValueWithAnimation('completed-value', data.completed);
            }
            
            if (data.total !== undefined && data.total !== null) {
                updateValueWithAnimation('total-value', data.total);
            }
            
            if (data.failed !== undefined) {
                updateValueWithAnimation('failed-value', data.failed);
            }
            
            if (data.speed !== undefined) {
                updateValueWithAnimation('speed-value', data.speed, 'jobs/min', 600);
                updateChart(speedChart, data.speed, data.timestamp);
            }
            
            if (data.avg_speed !== undefined) {
                updateValueWithAnimation('avg-speed', data.avg_speed, 'jobs/min', 600);
            }
            
            if (data.max_speed !== undefined) {
                updateValueWithAnimation('max-speed', data.max_speed, 'jobs/min', 600);
            }
            
            if (data.min_speed !== undefined) {
                updateValueWithAnimation('min-speed', data.min_speed, 'jobs/min', 600);
            }
            
            if (data.eta !== undefined) {
                const etaElement = document.getElementById('eta-value');
                if (etaElement) etaElement.textContent = data.eta;
            }
            
            if (data.elapsed_time !== undefined) {
                const elapsedElement = document.getElementById('elapsed-time');
                if (elapsedElement) elapsedElement.textContent = data.elapsed_time;
            }
            
            // 에러 통계
            if (data.error_count !== undefined) {
                updateValueWithAnimation('error-count', data.error_count);
            }
            
            if (data.error_rate !== undefined) {
                updateValueWithAnimation('error-rate', data.error_rate, '%');
                updateChart(errorChart, data.error_count || 0, data.timestamp);
            }
            
            if (data.error_types) {
                const errorTypesDiv = document.getElementById('error-types');
                errorTypesDiv.innerHTML = '';
                for (const [type, count] of Object.entries(data.error_types)) {
                    const span = document.createElement('span');
                    span.style.cssText = 'display: inline-block; margin: 5px; padding: 5px 10px; background: #21262d; border-radius: 4px;';
                    span.textContent = `${type}: ${count}`;
                    errorTypesDiv.appendChild(span);
                }
            }
            
            if (data.recent_errors && data.recent_errors.length > 0) {
                const recentErrorsDiv = document.getElementById('recent-errors');
                recentErrorsDiv.innerHTML = '';
                data.recent_errors.slice(0, 10).forEach(err => {
                    const div = document.createElement('div');
                    div.className = 'error-item';
                    div.innerHTML = `<strong>${err.cause_abb} → ${err.outcome_abb}</strong><br>${err.error_msg}<br><small>${err.timestamp}</small>`;
                    recentErrorsDiv.appendChild(div);
                });
            }
            
            // 프로세스 상태
            if (data.process_count !== undefined) {
                updateValueWithAnimation('process-count', data.process_count);
            }
            
            if (data.process_status && data.process_status.length > 0) {
                const processStatusDiv = document.getElementById('process-status');
                processStatusDiv.innerHTML = '';
                data.process_status.forEach((proc, index) => {
                    const div = document.createElement('div');
                    div.style.cssText = 'padding: 12px; margin: 8px 0; background: var(--bg-secondary); border-radius: 8px; border-left: 3px solid var(--accent-blue); transition: all 0.3s;';
                    div.style.animation = `slideUp 0.3s ease-out ${index * 0.1}s backwards`;
                    div.innerHTML = `<strong style="color: var(--accent-blue);">PID: ${proc.pid}</strong> | CPU: <span style="color: var(--accent-orange);">${proc.cpu}%</span> | MEM: <span style="color: var(--accent-purple);">${proc.mem}%</span> | Status: ${proc.status}<br><small style="color: var(--text-secondary);">${proc.command}</small>`;
                    div.addEventListener('mouseenter', function() {
                        this.style.transform = 'translateX(4px)';
                        this.style.boxShadow = 'var(--shadow-sm)';
                    });
                    div.addEventListener('mouseleave', function() {
                        this.style.transform = 'translateX(0)';
                        this.style.boxShadow = 'none';
                    });
                    processStatusDiv.appendChild(div);
                });
            }
            
            if (data.restart_count !== undefined) {
                updateValueWithAnimation('restart-count', data.restart_count);
            }
            
            // 코어별 진행 상황
            if (data.core_progress && Array.isArray(data.core_progress)) {
                const coreGrid = document.getElementById('core-progress-grid');
                const coreAllView = document.getElementById('core-all-view');
                const expandBtn = document.getElementById('core-expand-btn');
                
                coreGrid.innerHTML = '';
                coreAllView.innerHTML = '';
                
                // 활성 코어 필터링
                const activeCores = data.core_progress.filter(core => core.cpu_usage > 0.1 || core.processes_count > 0);
                const displayCores = activeCores.length > 0 ? activeCores : data.core_progress.slice(0, 10);
                
                // 코어 카드 생성 함수
                function createCoreCard(core, index) {
                    const card = document.createElement('div');
                    card.className = 'core-progress-card';
                    card.style.animation = `slideUp 0.3s ease-out ${index * 0.05}s backwards`;
                    
                    // CPU 사용률에 따른 색상
                    const cpuColor = core.cpu_usage > 80 ? 'var(--accent-red)' : 
                                    core.cpu_usage > 50 ? 'var(--accent-orange)' : 
                                    'var(--accent-blue)';
                    
                    card.innerHTML = `
                        <div class="core-progress-label">코어 ${core.core_id}</div>
                        <div class="core-progress-value" style="color: ${cpuColor}">${core.cpu_usage.toFixed(1)}%</div>
                        <div class="core-progress-bar">
                            <div class="core-progress-fill" style="width: ${core.cpu_usage}%"></div>
                        </div>
                        <div style="margin-top: 8px; font-size: 11px; color: var(--text-secondary);">
                            작업 완료율: ${core.estimated_progress.toFixed(1)}% | 
                            CPU 사용률: <span style="color: ${cpuColor}">${core.cpu_usage}%</span> | 
                            프로세스: ${Math.round(core.processes_count)} | 
                            완료 작업: ${core.estimated_completed.toLocaleString()}
                        </div>
                    `;
                    return card;
                }
                
                // 활성 코어만 표시
                displayCores.forEach((core, index) => {
                    coreGrid.appendChild(createCoreCard(core, index));
                });
                
                // 전체 코어 표시 (숨김 상태)
                data.core_progress.forEach((core, index) => {
                    coreAllView.appendChild(createCoreCard(core, index));
                });
                
                // 전체 코어가 활성 코어보다 많으면 "열기" 버튼 표시
                if (data.core_progress.length > displayCores.length) {
                    expandBtn.style.display = 'block';
                    expandBtn.textContent = `전체 코어 보기 (${data.core_progress.length}개)`;
                } else {
                    expandBtn.style.display = 'none';
                }
            }
            
            // 전체 코어 토글 함수
            window.toggleAllCores = function() {
                const coreGrid = document.getElementById('core-progress-grid');
                const coreAllView = document.getElementById('core-all-view');
                const expandBtn = document.getElementById('core-expand-btn');
                
                if (coreAllView.classList.contains('active')) {
                    // 닫기
                    coreAllView.classList.remove('active');
                    expandBtn.textContent = expandBtn.textContent.replace('닫기', '전체 코어 보기');
                    expandBtn.textContent = expandBtn.textContent.replace(/\(\d+개\)/, '') + ' (전체 코어 보기)';
                } else {
                    // 열기
                    coreAllView.classList.add('active');
                    expandBtn.textContent = '전체 코어 닫기';
                    // 스크롤을 위로 이동
                    coreAllView.scrollIntoView({ behavior: 'smooth', block: 'start' });
                }
            };
            
            // 시스템 리소스
            if (data.resources) {
                const res = data.resources;
                // 로드 평균 (1분) 표시
                if (res.load_1min !== undefined) {
                    const SYSTEM_CORES = 96; // 시스템 코어 수
                    const load_ratio = (res.load_1min / SYSTEM_CORES) * 100; // 로드 비율 (%)
                    updateChart(cpuChart, res.load_1min, data.timestamp, 0); // 로드 평균 값
                    updateChart(cpuChart, load_ratio, data.timestamp, 1); // 로드 비율 (%)
                    updateChart(resourceChart, load_ratio, data.timestamp, 0); // 리소스 차트에도 로드 비율 표시
                }
                if (res.memory_percent !== undefined) {
                    updateChart(memoryChart, res.memory_percent, data.timestamp);
                    updateChart(resourceChart, res.memory_used_mb / 1024, data.timestamp, 1);
                }
            }
        });
        
        socket.on('alert', function(alert) {
            const alertPanel = document.getElementById('alert-panel');
            const alertList = document.getElementById('alert-list');
            
            if (alertPanel.style.display === 'none') {
                alertPanel.style.display = 'block';
                alertPanel.style.animation = 'slideUp 0.4s ease-out';
            }
            
            const alertDiv = document.createElement('div');
            alertDiv.className = `alert-item alert-${alert.level}`;
            alertDiv.innerHTML = `<strong>${alert.level === 'critical' ? '🔴' : '⚠️'}</strong> ${alert.message}`;
            alertDiv.style.animation = 'slideUp 0.3s ease-out';
            alertList.insertBefore(alertDiv, alertList.firstChild);
            
            // 최근 10개만 유지
            while (alertList.children.length > 10) {
                alertList.removeChild(alertList.lastChild);
            }
        });
        
        // 모달용 차트 저장
        let modalChartInstance = null;
        
        // 차트 모달 열기
        function openChartModal(chartId, title) {
            const chartName = chartId + 'Chart';
            const fullData = fullChartData[chartName];
            
            if (!fullData || fullData.labels.length === 0) {
                console.warn('차트 데이터가 없습니다:', chartName);
                return;
            }
            
            const modal = document.getElementById('chartModal');
            const modalTitle = document.getElementById('modalChartTitle');
            const modalCanvas = document.getElementById('modalChart');
            
            modalTitle.textContent = title;
            modal.classList.add('active');
            
            // 기존 모달 차트 제거
            if (modalChartInstance) {
                modalChartInstance.destroy();
                modalChartInstance = null;
            }
            
            // 원본 차트 옵션 가져오기
            const originalChart = 
                chartId === 'progressChart' ? progressChart :
                chartId === 'speedChart' ? speedChart :
                chartId === 'errorChart' ? errorChart :
                chartId === 'resourceChart' ? resourceChart :
                chartId === 'cpuChart' ? cpuChart :
                chartId === 'memoryChart' ? memoryChart : null;
            
            if (!originalChart) {
                console.warn('원본 차트를 찾을 수 없습니다:', chartId);
                return;
            }
            
            // 모달 차트 생성 (전체 데이터 사용)
            modalChartInstance = new Chart(modalCanvas, {
                type: originalChart.config.type,
                data: {
                    labels: [...fullData.labels],
                    datasets: fullData.datasets.map(ds => ({
                        ...ds,
                        data: [...ds.data]
                    }))
                },
                options: {
                    ...originalChart.options,
            responsive: false, // 고정 크기 사용 (가로 스크롤을 위해)
            maintainAspectRatio: false,
                    plugins: {
                        ...originalChart.options.plugins,
                        tooltip: {
                            ...originalChart.options.plugins.tooltip,
                            mode: 'index',
                            intersect: false
                        }
                    },
                    scales: {
                        ...originalChart.options.scales,
                        x: {
                            ...originalChart.options.scales.x,
                            min: undefined,
                            max: undefined
                        }
                    }
                }
            });
        }
        
        // 차트 모달 닫기
        function closeChartModal(event) {
            if (event && event.target.id !== 'chartModal' && event.target.className !== 'chart-modal-close') {
                return;
            }
            const modal = document.getElementById('chartModal');
            modal.classList.remove('active');
            if (modalChartInstance) {
                modalChartInstance.destroy();
                modalChartInstance = null;
            }
        }
        
        // 전체 데이터 저장 (모달용)
        const fullChartData = {
            progressChart: { labels: [], datasets: [] },
            speedChart: { labels: [], datasets: [] },
            errorChart: { labels: [], datasets: [] },
            resourceChart: { labels: [], datasets: [] },
            cpuChart: { labels: [], datasets: [] },
            memoryChart: { labels: [], datasets: [] }
        };
        
        function updateChart(chart, value, timestamp, datasetIndex = 0) {
            const time = new Date(timestamp).toLocaleTimeString();
            const chartName = chart.canvas.id.replace('Chart', 'Chart');
            
            // 전체 데이터에 추가
            if (!fullChartData[chartName]) {
                fullChartData[chartName] = { labels: [], datasets: [] };
            }
            
            // 전체 데이터 업데이트
            if (fullChartData[chartName].labels.length === 0 || 
                fullChartData[chartName].labels[fullChartData[chartName].labels.length - 1] !== time) {
                fullChartData[chartName].labels.push(time);
                if (fullChartData[chartName].datasets.length === 0) {
                    // 첫 데이터셋 초기화
                    fullChartData[chartName].datasets = chart.data.datasets.map(ds => ({
                        ...ds,
                        data: []
                    }));
                }
                fullChartData[chartName].datasets.forEach(ds => {
                    if (ds.data.length === fullChartData[chartName].labels.length - 1) {
                        ds.data.push(null);
                    }
                });
            }
            
            if (fullChartData[chartName].datasets[datasetIndex]) {
                fullChartData[chartName].datasets[datasetIndex].data[
                    fullChartData[chartName].datasets[datasetIndex].data.length - 1
                ] = value;
            }
            
            // 최근 30개만 표시 (화면 너비를 모두 활용)
            const totalPoints = fullChartData[chartName].labels.length;
            const recentCount = 30;
            const startIdx = Math.max(0, totalPoints - recentCount);
            
            // 최근 데이터만 추출하여 표시
            chart.data.labels = fullChartData[chartName].labels.slice(startIdx);
            chart.data.datasets = fullChartData[chartName].datasets.map(ds => ({
                ...ds,
                data: ds.data.slice(startIdx)
            }));
            
            // x축 설정 (화면 너비 활용)
            if (chart.options.scales && chart.options.scales.x) {
                chart.options.scales.x.min = undefined;
                chart.options.scales.x.max = undefined;
            }
            
            // 차트 업데이트
            chart.update('none');
            
            // 모달 차트도 업데이트 (열려있는 경우)
            if (modalChartInstance && document.getElementById('chartModal').classList.contains('active')) {
                modalChartInstance.data = {
                    labels: [...fullChartData[chartName].labels],
                    datasets: fullChartData[chartName].datasets.map(ds => ({
                        ...ds,
                        data: [...ds.data]
                    }))
                };
                modalChartInstance.update({
                    duration: 750,
                    easing: 'easeOutQuart'
                });
            }
        }
        
        function addLogLine(line, className = '') {
            const div = document.createElement('div');
            div.className = `log-line ${className}`;
            div.textContent = line;
            logContainer.appendChild(div);
            logContainer.scrollTop = logContainer.scrollHeight;
            
            // 최근 1000줄만 유지
            while (logContainer.children.length > 1000) {
                logContainer.removeChild(logContainer.firstChild);
            }
        }
        
        // 시스템 리소스 주기적 업데이트
        setInterval(() => {
            fetch('/api/resources')
                .then(r => r.json())
                .then(data => {
                    if (data && data.load_1min !== undefined) {
                        const time = new Date(data.timestamp).toLocaleTimeString();
                        const SYSTEM_CORES = 96;
                        const load_ratio = (data.load_1min / SYSTEM_CORES) * 100;
                        resourceChart.data.labels.push(time);
                        resourceChart.data.datasets[0].data.push(load_ratio);
                        resourceChart.data.datasets[1].data.push(data.memory_used_mb / 1024);
                        
                        if (resourceChart.data.labels.length > 100) {
                            resourceChart.data.labels.shift();
                            resourceChart.data.datasets[0].data.shift();
                            resourceChart.data.datasets[1].data.shift();
                        }
                        
                        resourceChart.update('none');
                    }
                })
                .catch(err => console.error('리소스 조회 오류:', err));
        }, 5000);
        
        // 초기 로그 표시
        addLogLine('[시스템] 대시보드 초기화 완료. 실시간 로그를 기다리는 중...', 'progress-line');
        
        // ESC 키로 모달 닫기
        document.addEventListener('keydown', function(e) {
            if (e.key === 'Escape') {
                closeChartModal();
                document.getElementById('project-modal').style.display = 'none';
            }
        });
        
        // ========================================================================
        // 프로젝트 관리 JavaScript
        // ========================================================================
        let currentProject = null;
        let fileTreeData = null;
        let selectedRScript = null;
        let selectedShellScript = null;
        let currentBrowseType = null; // 'r_script', 'shell_script', 'db_completed_file', 'db_completed_folder', 'db_system_failed_file', 'db_system_failed_folder', 'structured_log', 'pipe_log'
        
        // 프로젝트 목록 로드
        async function loadProjects() {
            try {
                const response = await fetch('/api/projects');
                const data = await response.json();
                const dropdown = document.getElementById('project-dropdown');
                
                dropdown.innerHTML = '<option value="">프로젝트 선택...</option>';
                
                for (const [name, config] of Object.entries(data.projects || {})) {
                    const option = document.createElement('option');
                    option.value = name;
                    option.textContent = name;
                    if (currentProject && currentProject.name === name) {
                        option.selected = true;
                    }
                    dropdown.appendChild(option);
                }
            } catch (error) {
                console.error('프로젝트 목록 로드 실패:', error);
            }
        }
        
        // 프로젝트 정보 표시 함수
        async function displayProjectInfo(projectName) {
            if (!projectName) {
                document.getElementById('project-info-panel').style.display = 'none';
                return;
            }
            
            try {
                const response = await fetch(`/api/projects/${projectName}`);
                if (response.ok) {
                    const projectConfig = await response.json();
                    document.getElementById('project-info-name').textContent = projectConfig.name || projectName;
                    
                    // 상세 정보 구성
                    const sessionInfo = [];
                    if (projectConfig.tmux_session) {
                        sessionInfo.push(`TMUX: ${projectConfig.tmux_session}`);
                    }
                    if (projectConfig.process_pattern) {
                        sessionInfo.push(`Process: ${projectConfig.process_pattern}`);
                    }
                    document.getElementById('project-info-session').textContent = sessionInfo.join(' | ') || 'N/A';
                    
                    document.getElementById('project-info-panel').style.display = 'block';
                } else {
                    document.getElementById('project-info-panel').style.display = 'none';
                }
            } catch (error) {
                console.error('프로젝트 정보 로드 실패:', error);
                document.getElementById('project-info-panel').style.display = 'none';
            }
        }
        
        // 프로젝트 선택 (setupEventListeners 함수로 이동됨)
        
        // 파일 트리 로드
        async function loadFileTree(basePath = '/home/hashjamm/codes/disease_network') {
            try {
                const response = await fetch(`/api/files/tree?path=${encodeURIComponent(basePath)}`);
                const data = await response.json();
                fileTreeData = data.tree;
                renderFileTree(data.tree);
            } catch (error) {
                console.error('파일 트리 로드 실패:', error);
                alert('파일 트리를 로드할 수 없습니다: ' + error.message);
            }
        }
        
        // 파일 트리 렌더링
        function renderFileTree(tree, container = document.getElementById('file-tree'), level = 0) {
            container.innerHTML = '';
            
            if (!tree || tree.length === 0) {
                container.innerHTML = '<div style="color: var(--text-muted); padding: 8px;">파일이 없습니다</div>';
                return;
            }
            
            tree.forEach(item => {
                const div = document.createElement('div');
                div.className = 'file-tree-item';
                div.style.paddingLeft = `${level * 20 + 8}px`;
                
                if (item.type === 'directory') {
                    div.className += ' directory';
                    div.innerHTML = `📁 ${item.name}`;
                    div.addEventListener('click', function(e) {
                        e.stopPropagation();
                        const childrenDiv = div.nextElementSibling;
                        if (childrenDiv && childrenDiv.classList.contains('file-tree-children')) {
                            childrenDiv.remove();
                        } else {
                            const childrenContainer = document.createElement('div');
                            childrenContainer.className = 'file-tree-children';
                            renderFileTree(item.children, childrenContainer, level + 1);
                            div.after(childrenContainer);
                        }
                    });
                    // 폴더 타입 선택을 위한 더블클릭 이벤트
                    div.addEventListener('dblclick', function(e) {
                        e.stopPropagation();
                        const path = item.path;
                        let inputId = null;
                        
                        if (currentBrowseType === 'db_completed_folder') {
                            inputId = 'db-completed-folder';
                        } else if (currentBrowseType === 'db_system_failed_folder') {
                            inputId = 'db-system-failed-folder';
                        }
                        
                        if (inputId) {
                            document.getElementById(inputId).value = path;
                            document.getElementById('file-tree-container').style.display = 'none';
                        }
                    });
                } else {
                    div.className += ' file';
                    const icon = item.extension === '.R' ? '📊' : item.extension === '.sh' ? '🔧' : item.extension === '.py' ? '🐍' : item.extension === '.duckdb' ? '🗄️' : item.extension === '.log' ? '📋' : '📄';
                    div.innerHTML = `${icon} ${item.name}`;
                    div.addEventListener('click', function(e) {
                        e.stopPropagation();
                        const path = item.path;
                        let inputId = null;
                        let shouldClose = false;
                        
                        // 필드 타입에 따라 input ID 결정
                        if (currentBrowseType === 'r_script' && item.extension === '.R') {
                            inputId = 'r-script-path';
                            shouldClose = true;
                        } else if (currentBrowseType === 'shell_script' && item.extension === '.sh') {
                            inputId = 'shell-script-path';
                            shouldClose = true;
                        } else if (currentBrowseType === 'db_completed_file' && item.extension === '.duckdb') {
                            inputId = 'db-completed-file';
                            shouldClose = true;
                        } else if (currentBrowseType === 'db_system_failed_file' && item.extension === '.duckdb') {
                            inputId = 'db-system-failed-file';
                            shouldClose = true;
                        } else if (currentBrowseType === 'structured_log' && item.extension === '.log') {
                            inputId = 'structured-log';
                            shouldClose = true;
                        } else if (currentBrowseType === 'pipe_log' && item.extension === '.log') {
                            inputId = 'pipe-log';
                            shouldClose = true;
                        }
                        
                        if (inputId) {
                            document.getElementById(inputId).value = path;
                            if (shouldClose) {
                            document.getElementById('file-tree-container').style.display = 'none';
                            }
                        }
                    });
                }
                
                container.appendChild(div);
            });
        }
        
        // 이벤트 리스너 등록 함수
        function setupEventListeners() {
            // 프로젝트 선택 드롭다운
            const projectDropdown = document.getElementById('project-dropdown');
            if (projectDropdown) {
                projectDropdown.addEventListener('change', async function(e) {
                    const projectName = e.target.value;
                    if (!projectName) {
                        currentProject = null;
                        document.getElementById('project-info-panel').style.display = 'none';
                        return;
                    }
                    
                    try {
                        // 프로젝트 전환 API 호출
                        const switchResponse = await fetch('/api/switch-project', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({project_name: projectName})
                        });
                        
                        if (switchResponse.ok) {
                            const result = await switchResponse.json();
                            currentProject = result.project;
                            
                            // 프로젝트 정보 표시
                            await displayProjectInfo(projectName);
                            
                            // 세션 스토리지에 저장
                            sessionStorage.setItem('currentProject', projectName);
                            
                            // URL 업데이트
                            const url = new URL(window.location);
                            url.searchParams.set('project', projectName);
                            window.history.pushState({}, '', url);
                            
                            alert('프로젝트가 전환되었습니다. 모니터링이 새 프로젝트를 추적합니다.');
                        } else {
                            const error = await switchResponse.json();
                            alert('프로젝트 전환 실패: ' + (error.error || '알 수 없는 오류'));
                        }
                    } catch (error) {
                        console.error('프로젝트 전환 실패:', error);
                        alert('프로젝트 전환 중 오류가 발생했습니다: ' + error.message);
                    }
            });
        }
        
        // 새 프로젝트 등록 버튼
            const addProjectBtn = document.getElementById('add-project-btn');
            if (addProjectBtn) {
                addProjectBtn.addEventListener('click', function() {
            document.getElementById('project-modal').style.display = 'block';
                    // 모든 필드 초기화
            document.getElementById('project-name').value = '';
                    document.getElementById('tmux-session').value = '';
                    document.getElementById('detected-config').style.display = 'none';
                    detectedConfig = null;
                });
            }
            
            // 모든 찾아보기 버튼에 공통 이벤트 리스너 추가
            document.querySelectorAll('.browse-btn').forEach(btn => {
                btn.addEventListener('click', function() {
                    const fieldType = this.getAttribute('data-type');
            document.getElementById('file-tree-container').style.display = 'block';
                    currentBrowseType = fieldType;
            if (!fileTreeData) {
                loadFileTree();
            }
                });
            });
            
            // 자동 감지된 설정 저장 (전역 변수)
            let detectedConfig = null;
            
            // 자동 감지 버튼
            const autoDetectBtn = document.getElementById('auto-detect-btn');
            if (autoDetectBtn) {
                autoDetectBtn.addEventListener('click', async function() {
                    const tmuxSession = document.getElementById('tmux-session').value.trim();
                    if (!tmuxSession) {
                        alert('tmux 세션 이름을 먼저 입력하세요.');
                        return;
                    }
                    
                    this.disabled = true;
                    this.textContent = '감지 중...';
                    
                    try {
                        const response = await fetch('/api/detect-tmux', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({tmux_session: tmuxSession})
                        });
                        
                        const result = await response.json();
                        
                        if (response.ok && result.success) {
                            detectedConfig = result.config;
                            
                            // 감지된 설정을 목록으로 표시
                            const configList = document.getElementById('detected-config-list');
                            const configDiv = document.getElementById('detected-config');
                            
                            let html = '';
                            if (detectedConfig.r_script) {
                                html += `<div><strong>R 스크립트:</strong> ${detectedConfig.r_script}</div>`;
                            }
                            if (detectedConfig.shell_script) {
                                html += `<div><strong>쉘 스크립트:</strong> ${detectedConfig.shell_script}</div>`;
                            }
                            if (detectedConfig.process_pattern) {
                                html += `<div><strong>프로세스 패턴:</strong> ${detectedConfig.process_pattern}</div>`;
                            }
                            if (detectedConfig.db_completed_file) {
                                html += `<div><strong>완료 작업 DB 파일 (중앙):</strong> ${detectedConfig.db_completed_file}</div>`;
                            }
                            if (detectedConfig.db_completed_folder) {
                                html += `<div><strong>완료 작업 DB 폴더 (청크):</strong> ${detectedConfig.db_completed_folder}</div>`;
                            }
                            if (detectedConfig.db_system_failed_file) {
                                html += `<div><strong>시스템 실패 작업 DB 파일 (중앙):</strong> ${detectedConfig.db_system_failed_file}</div>`;
                            }
                            if (detectedConfig.db_system_failed_folder) {
                                html += `<div><strong>시스템 실패 작업 DB 폴더 (청크):</strong> ${detectedConfig.db_system_failed_folder}</div>`;
                            }
                            if (detectedConfig.db_stat_failed_file) {
                                html += `<div><strong>통계 실패 작업 DB 파일 (중앙):</strong> ${detectedConfig.db_stat_failed_file}</div>`;
                            }
                            if (detectedConfig.db_stat_failed_folder) {
                                html += `<div><strong>통계 실패 작업 DB 폴더 (청크):</strong> ${detectedConfig.db_stat_failed_folder}</div>`;
                            }
                            if (detectedConfig.db_stat_excluded_file) {
                                html += `<div><strong>통계 제외 작업 DB 파일 (중앙):</strong> ${detectedConfig.db_stat_excluded_file}</div>`;
                            }
                            if (detectedConfig.db_stat_excluded_folder) {
                                html += `<div><strong>통계 제외 작업 DB 폴더 (청크):</strong> ${detectedConfig.db_stat_excluded_folder}</div>`;
                            }
                            if (detectedConfig.structured_log) {
                                html += `<div><strong>구조화된 로그:</strong> ${detectedConfig.structured_log}</div>`;
                            }
                            if (detectedConfig.pipe_log) {
                                html += `<div><strong>Pipe-pane 로그:</strong> ${detectedConfig.pipe_log}</div>`;
                            }
                            
                            configList.innerHTML = html || '<div style="color: var(--text-muted);">감지된 설정이 없습니다.</div>';
                            configDiv.style.display = 'block';
                        } else {
                            alert('자동 감지 실패: ' + (result.error || '알 수 없는 오류'));
                            detectedConfig = null;
                            document.getElementById('detected-config').style.display = 'none';
                        }
                    } catch (error) {
                        console.error('자동 감지 실패:', error);
                        alert('자동 감지 중 오류가 발생했습니다: ' + error.message);
                        detectedConfig = null;
                        document.getElementById('detected-config').style.display = 'none';
                    } finally {
                        this.disabled = false;
                        this.textContent = '🔍 자동 감지';
                    }
                });
            }
        
        // 프로젝트 저장
            const saveProjectBtn = document.getElementById('save-project');
            if (saveProjectBtn) {
                saveProjectBtn.addEventListener('click', async function() {
            const projectName = document.getElementById('project-name').value.trim();
                    const tmuxSession = document.getElementById('tmux-session').value.trim();
            
            if (!projectName) {
                alert('프로젝트 이름을 입력하세요.');
                return;
            }
            
                    if (!tmuxSession) {
                        alert('tmux 세션 이름을 입력하세요.');
                        return;
                    }
                    
                    if (!detectedConfig) {
                        alert('먼저 자동 감지를 실행하세요.');
                return;
            }
            
            try {
                        // 자동 감지된 설정을 사용
                        const projectData = {
                            name: projectName,
                            tmux_session: tmuxSession
                        };
                        
                        // 자동 감지된 모든 설정 추가
                        if (detectedConfig.r_script) projectData.r_script = detectedConfig.r_script;
                        if (detectedConfig.shell_script) projectData.shell_script = detectedConfig.shell_script;
                        if (detectedConfig.process_pattern) projectData.process_pattern = detectedConfig.process_pattern;
                        if (detectedConfig.db_completed_file) projectData.db_completed_file = detectedConfig.db_completed_file;
                        if (detectedConfig.db_completed_folder) projectData.db_completed_folder = detectedConfig.db_completed_folder;
                        if (detectedConfig.db_system_failed_file) projectData.db_system_failed_file = detectedConfig.db_system_failed_file;
                        if (detectedConfig.db_system_failed_folder) projectData.db_system_failed_folder = detectedConfig.db_system_failed_folder;
                        if (detectedConfig.db_stat_failed_file) projectData.db_stat_failed_file = detectedConfig.db_stat_failed_file;
                        if (detectedConfig.db_stat_failed_folder) projectData.db_stat_failed_folder = detectedConfig.db_stat_failed_folder;
                        if (detectedConfig.db_stat_excluded_file) projectData.db_stat_excluded_file = detectedConfig.db_stat_excluded_file;
                        if (detectedConfig.db_stat_excluded_folder) projectData.db_stat_excluded_folder = detectedConfig.db_stat_excluded_folder;
                        if (detectedConfig.structured_log) projectData.structured_log = detectedConfig.structured_log;
                        if (detectedConfig.pipe_log) projectData.pipe_log = detectedConfig.pipe_log;
                        
                const response = await fetch('/api/projects', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify(projectData)
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    await loadProjects();
                    document.getElementById('project-modal').style.display = 'none';
                    
                    // 등록된 프로젝트 자동 선택
                    document.getElementById('project-dropdown').value = projectName;
                    await displayProjectInfo(projectName);
                    sessionStorage.setItem('currentProject', projectName);
                    
                    // URL 업데이트
                    const url = new URL(window.location);
                    url.searchParams.set('project', projectName);
                    window.history.pushState({}, '', url);
                    
                    alert('프로젝트가 등록되었습니다.');
                } else {
                    alert('오류: ' + (result.error || '프로젝트 등록 실패'));
                }
            } catch (error) {
                console.error('프로젝트 등록 실패:', error);
                alert('프로젝트 등록 중 오류가 발생했습니다: ' + error.message);
            }
        });
            }
        
        // 취소 버튼
            const cancelProjectBtn = document.getElementById('cancel-project');
            if (cancelProjectBtn) {
                cancelProjectBtn.addEventListener('click', function() {
            document.getElementById('project-modal').style.display = 'none';
        });
            }
        
        // 모달 외부 클릭 시 닫기
            const projectModal = document.getElementById('project-modal');
            if (projectModal) {
                projectModal.addEventListener('click', function(e) {
            if (e.target === this) {
                this.style.display = 'none';
            }
        });
            }
        }
        
        // 초기화: 프로젝트 목록 로드 및 현재 프로젝트 복원
        async function initializeProject() {
            await loadProjects();
            
            // URL 파라미터에서 프로젝트 이름 가져오기
            const urlParams = new URLSearchParams(window.location.search);
            let projectName = urlParams.get('project');
            
            // URL에 없으면 세션 스토리지에서 가져오기
            if (!projectName) {
                projectName = sessionStorage.getItem('currentProject');
            }
            
            // 프로젝트가 있으면 선택하고 정보 표시
            if (projectName) {
                const dropdown = document.getElementById('project-dropdown');
                dropdown.value = projectName;
                await displayProjectInfo(projectName);
            } else {
                // 기본 프로젝트 사용
                const response = await fetch('/api/projects');
                const data = await response.json();
                const defaultProject = data.default_project || 'validation';
                if (defaultProject && document.getElementById('project-dropdown').querySelector(`option[value="${defaultProject}"]`)) {
                    document.getElementById('project-dropdown').value = defaultProject;
                    await displayProjectInfo(defaultProject);
                }
            }
        }
        
        // DOM 로드 완료 후 초기화
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', function() {
                setupEventListeners();
        initializeProject();
            });
        } else {
            // DOM이 이미 로드된 경우
            setupEventListeners();
            initializeProject();
        }
    </script>
</body>
</html>
"""

# ============================================================================
# Flask 라우트 정의
# ============================================================================

@app.route('/')
def index():
    """
    메인 대시보드 페이지
    웹 브라우저에서 http://localhost:5000 접속 시 표시되는 HTML 페이지
    URL 파라미터로 프로젝트 선택 가능: ?project=프로젝트명
    """
    project_name = request.args.get('project')
    
    # 전역 변수 업데이트 (프로젝트 선택 시)
    global TMUX_SESSION, PIPE_LOG, STRUCTURED_LOG, DB_COMPLETED_FILE, DB_COMPLETED_FOLDER
    
    if project_name:
        projects_config = load_projects_config()
        if project_name in projects_config.get('projects', {}):
            project_config = projects_config['projects'][project_name]
            TMUX_SESSION = project_config.get('tmux_session', TMUX_SESSION)
            PIPE_LOG = project_config.get('pipe_log', PIPE_LOG)
            STRUCTURED_LOG = project_config.get('structured_log', STRUCTURED_LOG)
            if 'db_completed_file' in project_config:
                DB_COMPLETED_FILE = project_config['db_completed_file']
            if 'db_completed_folder' in project_config:
                DB_COMPLETED_FOLDER = project_config['db_completed_folder']
    
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/stats')
def get_stats():
    """
    통계 데이터 반환 (REST API)
    
    Returns:
        JSON: 최근 통계 히스토리, 성능 메트릭, 에러 정보
    """
    return jsonify({
        'history': list(stats_history)[-100:],  # 최근 100개만
        'performance': performance_metrics,
        'errors': list(error_history)[-10:]
    })

@app.route('/api/resources')
def get_resources():
    """
    시스템 리소스 사용량 반환 (REST API)
    
    Returns:
        JSON: 로드 평균, 메모리 사용량 등
    """
    resources = get_system_resources()
    if resources:
        return jsonify(resources)
    return jsonify({'error': '리소스 정보를 가져올 수 없습니다'}), 500

# ============================================================================
# 프로젝트 관리 함수
# ============================================================================

def load_projects_config():
    """등록된 프로젝트 목록 로드"""
    if os.path.exists(PROJECTS_CONFIG_FILE):
        try:
            with open(PROJECTS_CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"[WARNING] 프로젝트 설정 파일 읽기 실패: {e}")
    return {"projects": {}, "default_project": None}

def save_projects_config(config):
    """프로젝트 설정 저장"""
    try:
        with open(PROJECTS_CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"[ERROR] 프로젝트 설정 저장 실패: {e}")
        return False

def detect_from_tmux_session(tmux_session):
    """tmux 세션에서 실행 중인 프로세스 정보를 분석하여 설정 추출"""
    config = {
        'tmux_session': tmux_session,
        'pipe_log': f"/tmp/tmux_monitor/{tmux_session}.log"
    }
    
    try:
        # tmux 세션의 pane PID 확인
        result = subprocess.run(
            ['tmux', 'list-panes', '-t', tmux_session, '-F', '#{pane_pid}'],
            capture_output=True,
            text=True,
            timeout=2
        )
        
        if result.returncode != 0:
            return {'error': f'tmux 세션 "{tmux_session}"을 찾을 수 없습니다'}
        
        pids = [pid.strip() for pid in result.stdout.strip().split('\n') if pid.strip()]
        if not pids:
            return {'error': f'tmux 세션 "{tmux_session}"에 활성 pane이 없습니다'}
        
        # 첫 번째 pane의 PID 사용
        pane_pid = pids[0]
        
        # 프로세스 트리에서 R 스크립트 실행 프로세스 찾기
        # 먼저 pane의 자식 프로세스들 확인
        result = subprocess.run(
            ['pgrep', '-P', pane_pid],
            capture_output=True,
            text=True,
            timeout=2
        )
        
        child_pids = [pid.strip() for pid in result.stdout.strip().split('\n') if pid.strip()]
        all_pids = [pane_pid] + child_pids
        
        # 모든 관련 프로세스의 명령어 확인
        r_script_path = None
        process_pattern = None
        shell_script_path = None
        fu_value = None  # FU 환경 변수 값
        
        # 환경 변수에서 FU 값 추출
        for pid in all_pids:
            try:
                # /proc/PID/environ에서 환경 변수 읽기
                environ_file = f'/proc/{pid}/environ'
                if os.path.exists(environ_file):
                    with open(environ_file, 'rb') as f:
                        environ_data = f.read()
                        # 환경 변수는 null로 구분됨
                        environ_str = environ_data.decode('utf-8', errors='ignore')
                        for env_var in environ_str.split('\0'):
                            if env_var.startswith('FU='):
                                fu_value = env_var.split('=', 1)[1]
                                break
                    if fu_value:
                        break
            except Exception:
                continue
        
        for pid in all_pids:
            try:
                result = subprocess.run(
                    ['ps', '-p', pid, '-o', 'cmd=', '--no-headers'],
                    capture_output=True,
                    text=True,
                    timeout=1
                )
                
                if result.returncode != 0:
                    continue
                
                cmd_line = result.stdout.strip()
                
                # R 스크립트 경로 찾기
                if '.R' in cmd_line and ('Rscript' in cmd_line or '/R ' in cmd_line or ' R ' in cmd_line):
                    import re
                    # 더 정확한 패턴으로 R 스크립트 경로 찾기
                    patterns = [
                        r'Rscript\s+([/\w\.\-]+\.R)',
                        r'/R\s+([/\w\.\-]+\.R)',
                        r'([/\w\.\-]+/[\w\.\-]+\.R)',
                    ]
                    for pattern in patterns:
                        match = re.search(pattern, cmd_line)
                        if match:
                            potential_path = match.group(1)
                            if os.path.exists(potential_path):
                                r_script_path = potential_path
                                break
                    
                    if r_script_path:
                        # R 스크립트 이름에서 프로세스 패턴 추론
                        script_name = os.path.basename(r_script_path).replace('.R', '')
                        process_pattern = script_name.replace('_v5', '').replace('_v4', '').replace('_engine', '').replace('_manager', '')
                        break
                
                # 쉘 스크립트 경로 찾기
                if '.sh' in cmd_line and ('bash' in cmd_line or 'sh ' in cmd_line):
                    import re
                    match = re.search(r'([/\w\.\-]+\.sh)', cmd_line)
                    if match:
                        potential_path = match.group(1)
                        if os.path.exists(potential_path):
                            shell_script_path = potential_path
                
            except Exception:
                continue
        
        # R 스크립트를 찾았으면 분석
        if r_script_path:
            r_config = auto_detect_project_config(r_script_path, shell_script_path, fu_value)
            config.update(r_config)
            config['r_script'] = r_script_path
        
        if shell_script_path:
            config['shell_script'] = shell_script_path
            # 쉘 스크립트에서 템플릿 문자열을 실제 값으로 변환
            if fu_value:
                import re
                # LOG_FILE 템플릿 변환
                if 'structured_log' in config and '$' in config['structured_log']:
                    log_file = config['structured_log']
                    # $LOG_DIR, ${SCRIPT_BASENAME}, ${FU} 변수 치환
                    if '$LOG_DIR' in log_file:
                        log_file = log_file.replace('$LOG_DIR', '/home/hashjamm/results/disease_network')
                    if '${SCRIPT_BASENAME}' in log_file:
                        script_basename = os.path.basename(shell_script_path).replace('.sh', '')
                        log_file = log_file.replace('${SCRIPT_BASENAME}', script_basename)
                    if '${FU}' in log_file:
                        log_file = log_file.replace('${FU}', fu_value)
                    config['structured_log'] = log_file
                
                # 쉘 스크립트의 동적 경로도 변환 (${FU} 패턴)
                path_fields = [
                    'db_completed_file', 'db_completed_folder',
                    'db_system_failed_file', 'db_system_failed_folder'
                ]
                for field in path_fields:
                    if field in config and isinstance(config[field], str) and '${FU}' in config[field]:
                        config[field] = config[field].replace('${FU}', fu_value)
        
        # FU 값이 있으면 동적 경로를 실제 경로로 변환
        if fu_value:
            import re
            # 모든 경로 필드에서 fu%d, fu${FU} 패턴을 실제 값으로 변환
            path_fields = [
                'db_completed_file', 'db_completed_folder',
                'db_system_failed_file', 'db_system_failed_folder',
                'db_stat_failed_file', 'db_stat_failed_folder',
                'db_stat_excluded_file', 'db_stat_excluded_folder',
                'matched_parquet_folder', 'results_hr_folder', 'results_mapping_folder'
            ]
            
            for field in path_fields:
                if field in config and isinstance(config[field], str):
                    # fu%d 패턴을 fu{fu_value}로 변환
                    config[field] = re.sub(r'fu%d', f'fu{fu_value}', config[field])
                    # fu${FU} 패턴도 변환
                    config[field] = re.sub(r'fu\$\{FU\}', f'fu{fu_value}', config[field])
                    # v10, v9 같은 패턴도 fu{fu_value}로 변환 (필요한 경우)
                    if 'v10' in config[field] or 'v9' in config[field]:
                        # v10, v9 등을 fu{fu_value}로 변환
                        config[field] = re.sub(r'v\d+', f'fu{fu_value}', config[field])
            
            # db_path_pattern이 있으면 db_completed_file로 변환
            if 'db_path_pattern' in config:
                pattern = config['db_path_pattern']
                actual_path = re.sub(r'fu%d', f'fu{fu_value}', pattern)
                actual_path = re.sub(r'fu\$\{FU\}', f'fu{fu_value}', actual_path)
                config['db_completed_file'] = actual_path
                del config['db_path_pattern']
        
        # 프로세스 패턴이 없으면 기본값 사용
        if not process_pattern and 'process_pattern' in config:
            process_pattern = config['process_pattern']
        elif not process_pattern:
            # tmux 세션 이름 기반 추론
            process_pattern = tmux_session.replace('_validation', '').replace('_manager', '')
        
        config['process_pattern'] = process_pattern
        
        return config
        
    except subprocess.TimeoutExpired:
        return {'error': 'tmux 명령어 실행 시간 초과'}
    except Exception as e:
        return {'error': f'자동 감지 실패: {str(e)}'}

def auto_detect_project_config(r_script_path, shell_script_path=None, fu_value=None):
    """R 스크립트와 쉘 스크립트를 분석하여 자동으로 설정 생성"""
    config = {
        "r_script": r_script_path,
        "shell_script": shell_script_path,
        "tmux_session": Path(r_script_path).stem.replace("_engine", "").replace("_manager", "").replace("_v5", "").replace("_v4", ""),
    }
    
    # R 스크립트에서 정보 추출
    try:
        with open(r_script_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
            
            # 프로세스 패턴 추출
            process_match = re.search(r'process_pattern\s*=\s*["\']([^"\']+)["\']', content)
            if process_match:
                config["process_pattern"] = process_match.group(1)
            else:
                # 파일명 기반 추정
                script_name = Path(r_script_path).stem
                config["process_pattern"] = script_name.replace("_v", "").replace("_engine", "").replace("_manager", "")
            
            # 로그 경로 추출
            log_match = re.search(r'LOG_FILE\s*=\s*["\']([^"\']+)["\']', content)
            if log_match:
                config["structured_log"] = log_match.group(1)
            
            # DB 경로 추출 - 모든 필드 추출
            # db_completed_file (중앙 DB) - sprintf 동적 경로를 우선적으로 찾고, 없으면 정적 경로 사용
            # 패턴 1: sprintf 동적 경로 - db_completed_file = sprintf("/path/to/fu%d/file.duckdb", fu) (우선)
            db_match = re.search(r'db_completed_file\s*=\s*sprintf\s*\(\s*["\']([^"\']+)["\']', content)
            # 패턴 2: 정적 경로 - db_completed_file = "/path/to/file.duckdb" (대체)
            if not db_match:
                db_match = re.search(r'db_completed_file\s*=\s*["\']([^"\']+)["\']', content)
            
            if db_match:
                db_path = db_match.group(1)
                # fu 기반 동적 경로인지 확인 (%d, %s, ${FU} 등)
                if 'fu%d' in db_path or 'fu%s' in db_path or 'fu${FU}' in db_path or 'fu\\d' in db_path:
                    config["db_path_pattern"] = db_path
                    config["db_path_type"] = "dynamic_fu"
                else:
                    config["db_completed_file"] = db_path
                    config["db_path_type"] = "static"
            
            # db_completed_folder (청크 폴더) - sprintf 동적 경로를 우선적으로 찾고, 없으면 정적 경로 사용
            db_folder_match = re.search(r'db_completed_folder\s*=\s*sprintf\s*\(\s*["\']([^"\']+)["\']', content)
            if not db_folder_match:
                db_folder_match = re.search(r'db_completed_folder\s*=\s*["\']([^"\']+)["\']', content)
            if db_folder_match:
                config["db_completed_folder"] = db_folder_match.group(1)
            
            # db_system_failed_file (중앙 DB) - sprintf 동적 경로를 우선적으로 찾고, 없으면 정적 경로 사용
            db_failed_match = re.search(r'db_system_failed_file\s*=\s*sprintf\s*\(\s*["\']([^"\']+)["\']', content)
            if not db_failed_match:
                db_failed_match = re.search(r'db_system_failed_file\s*=\s*["\']([^"\']+)["\']', content)
            if db_failed_match:
                config["db_system_failed_file"] = db_failed_match.group(1)
            
            # db_system_failed_folder (청크 폴더) - sprintf 동적 경로를 우선적으로 찾고, 없으면 정적 경로 사용
            db_failed_folder_match = re.search(r'db_system_failed_folder\s*=\s*sprintf\s*\(\s*["\']([^"\']+)["\']', content)
            if not db_failed_folder_match:
                db_failed_folder_match = re.search(r'db_system_failed_folder\s*=\s*["\']([^"\']+)["\']', content)
            if db_failed_folder_match:
                config["db_system_failed_folder"] = db_failed_folder_match.group(1)
            
            # db_stat_failed_file (중앙 DB) - sprintf 동적 경로를 우선적으로 찾고, 없으면 정적 경로 사용
            db_stat_failed_match = re.search(r'db_stat_failed_file\s*=\s*sprintf\s*\(\s*["\']([^"\']+)["\']', content)
            if not db_stat_failed_match:
                db_stat_failed_match = re.search(r'db_stat_failed_file\s*=\s*["\']([^"\']+)["\']', content)
            if db_stat_failed_match:
                config["db_stat_failed_file"] = db_stat_failed_match.group(1)
            
            # db_stat_failed_folder (청크 폴더) - sprintf 동적 경로를 우선적으로 찾고, 없으면 정적 경로 사용
            db_stat_failed_folder_match = re.search(r'db_stat_failed_folder\s*=\s*sprintf\s*\(\s*["\']([^"\']+)["\']', content)
            if not db_stat_failed_folder_match:
                db_stat_failed_folder_match = re.search(r'db_stat_failed_folder\s*=\s*["\']([^"\']+)["\']', content)
            if db_stat_failed_folder_match:
                config["db_stat_failed_folder"] = db_stat_failed_folder_match.group(1)
            
            # db_stat_excluded_file (중앙 DB) - sprintf 동적 경로를 우선적으로 찾고, 없으면 정적 경로 사용
            db_stat_excluded_match = re.search(r'db_stat_excluded_file\s*=\s*sprintf\s*\(\s*["\']([^"\']+)["\']', content)
            if not db_stat_excluded_match:
                db_stat_excluded_match = re.search(r'db_stat_excluded_file\s*=\s*["\']([^"\']+)["\']', content)
            if db_stat_excluded_match:
                config["db_stat_excluded_file"] = db_stat_excluded_match.group(1)
            
            # db_stat_excluded_folder (청크 폴더) - sprintf 동적 경로를 우선적으로 찾고, 없으면 정적 경로 사용
            db_stat_excluded_folder_match = re.search(r'db_stat_excluded_folder\s*=\s*sprintf\s*\(\s*["\']([^"\']+)["\']', content)
            if not db_stat_excluded_folder_match:
                db_stat_excluded_folder_match = re.search(r'db_stat_excluded_folder\s*=\s*["\']([^"\']+)["\']', content)
            if db_stat_excluded_folder_match:
                config["db_stat_excluded_folder"] = db_stat_excluded_folder_match.group(1)
            
            # FU 환경 변수 사용 여부 확인
            if 'Sys.getenv("FU")' in content or 'Sys.getenv(\'FU\')' in content:
                config["has_fu_parameter"] = True
            
            # matched_parquet_folder 추출
            matched_parquet_match = re.search(r'matched_parquet_folder\s*=\s*["\']([^"\']+)["\']', content)
            if not matched_parquet_match:
                matched_parquet_match = re.search(r'matched_parquet_folder\s*=\s*sprintf\s*\(\s*["\']([^"\']+)["\']', content)
            if matched_parquet_match:
                config["matched_parquet_folder"] = matched_parquet_match.group(1)
            
            # results_hr_folder 추출
            results_hr_match = re.search(r'results_hr_folder\s*=\s*["\']([^"\']+)["\']', content)
            if not results_hr_match:
                results_hr_match = re.search(r'results_hr_folder\s*=\s*sprintf\s*\(\s*["\']([^"\']+)["\']', content)
            if results_hr_match:
                config["results_hr_folder"] = results_hr_match.group(1)
            
            # results_mapping_folder 추출
            results_mapping_match = re.search(r'results_mapping_folder\s*=\s*["\']([^"\']+)["\']', content)
            if not results_mapping_match:
                results_mapping_match = re.search(r'results_mapping_folder\s*=\s*sprintf\s*\(\s*["\']([^"\']+)["\']', content)
            if results_mapping_match:
                config["results_mapping_folder"] = results_mapping_match.group(1)
    except Exception as e:
        print(f"[WARNING] R 스크립트 분석 실패: {e}")
    
    # 쉘 스크립트에서 정보 추출
    if shell_script_path and os.path.exists(shell_script_path):
        try:
            with open(shell_script_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
                # FU 파라미터 확인
                if 'FU=' in content or 'export FU' in content:
                    config["has_fu_parameter"] = True
                
                # 로그 경로 추출
                log_match = re.search(r'LOG_FILE\s*=\s*["\']([^"\']+)["\']', content)
                if log_match and "structured_log" not in config:
                    config["structured_log"] = log_match.group(1)
                
                # DB 경로 추출 - 모든 필드 추출
                # DB_COMPLETED_FILE (중앙 DB) - 동적 경로(fu${FU})를 우선적으로 찾음
                db_match = re.search(r'DB_COMPLETED_FILE\s*=\s*["\']([^"\']+)["\']', content)
                if db_match and "db_completed_file" not in config:
                    db_path = db_match.group(1)
                    # fu 기반 동적 경로인지 확인 (쉘 스크립트는 ${FU} 형식 사용)
                    if 'fu${FU}' in db_path or 'fu\\d' in db_path or 'fu%d' in db_path:
                        config["db_path_pattern"] = db_path
                        config["db_path_type"] = "dynamic_fu"
                    else:
                        config["db_completed_file"] = db_path
                        config["db_path_type"] = "static"
                
                # DB_COMPLETED_FOLDER (청크 폴더)
                db_folder_match = re.search(r'DB_COMPLETED_FOLDER\s*=\s*["\']([^"\']+)["\']', content)
                if db_folder_match and "db_completed_folder" not in config:
                    config["db_completed_folder"] = db_folder_match.group(1)
                
                # DB_SYSTEM_FAILED_FILE (중앙 DB) - 쉘 스크립트에 있는 경우
                db_failed_match = re.search(r'DB_SYSTEM_FAILED_FILE\s*=\s*["\']([^"\']+)["\']', content)
                if db_failed_match and "db_system_failed_file" not in config:
                    config["db_system_failed_file"] = db_failed_match.group(1)
                
                # DB_SYSTEM_FAILED_FOLDER (청크 폴더) - 쉘 스크립트에 있는 경우
                db_failed_folder_match = re.search(r'DB_SYSTEM_FAILED_FOLDER\s*=\s*["\']([^"\']+)["\']', content)
                if db_failed_folder_match and "db_system_failed_folder" not in config:
                    config["db_system_failed_folder"] = db_failed_folder_match.group(1)
        except Exception as e:
            print(f"[WARNING] 쉘 스크립트 분석 실패: {e}")
    
    # 기본값 설정
    script_name = Path(r_script_path).stem
    
    if "structured_log" not in config:
        config["structured_log"] = f"/home/hashjamm/results/disease_network/{script_name}_history.log"
    
    if "db_completed_file" not in config and "db_path_pattern" not in config:
        config["db_completed_file"] = f"/home/hashjamm/results/disease_network/{script_name}_job_queue_db/completed_jobs.duckdb"
        config["db_path_type"] = "static"
    
    # db_completed_folder 기본값 설정 (청크 폴더)
    if "db_completed_folder" not in config:
        if "db_completed_file" in config:
            # db_completed_file의 디렉토리 + completed_jobs/ (청크 폴더)
            db_dir = os.path.dirname(config["db_completed_file"])
            config["db_completed_folder"] = os.path.join(db_dir, "completed_jobs")
        else:
            config["db_completed_folder"] = f"/home/hashjamm/results/disease_network/{script_name}_job_queue_db/completed_jobs"
    
    # db_system_failed_file 기본값 설정 (중앙 DB)
    if "db_system_failed_file" not in config:
        if "db_completed_file" in config:
            # db_completed_file과 같은 디렉토리에 system_failed_jobs.duckdb
            db_dir = os.path.dirname(config["db_completed_file"])
            config["db_system_failed_file"] = os.path.join(db_dir, "system_failed_jobs.duckdb")
        else:
            config["db_system_failed_file"] = f"/home/hashjamm/results/disease_network/{script_name}_job_queue_db/system_failed_jobs.duckdb"
    
    # db_system_failed_folder 기본값 설정 (청크 폴더)
    if "db_system_failed_folder" not in config:
        if "db_system_failed_file" in config:
            # db_system_failed_file의 디렉토리 + system_failed_jobs/ (청크 폴더)
            db_dir = os.path.dirname(config["db_system_failed_file"])
            config["db_system_failed_folder"] = os.path.join(db_dir, "system_failed_jobs")
        elif "db_completed_file" in config:
            # db_completed_file의 디렉토리 + system_failed_jobs/ (청크 폴더)
            db_dir = os.path.dirname(config["db_completed_file"])
            config["db_system_failed_folder"] = os.path.join(db_dir, "system_failed_jobs")
        else:
            config["db_system_failed_folder"] = f"/home/hashjamm/results/disease_network/{script_name}_job_queue_db/system_failed_jobs"
    
    # db_stat_failed_file 기본값 설정 (중앙 DB)
    if "db_stat_failed_file" not in config:
        if "db_completed_file" in config:
            db_dir = os.path.dirname(config["db_completed_file"])
            config["db_stat_failed_file"] = os.path.join(db_dir, "stat_failed_jobs.duckdb")
        else:
            config["db_stat_failed_file"] = f"/home/hashjamm/results/disease_network/{script_name}_job_queue_db/stat_failed_jobs.duckdb"
    
    # db_stat_failed_folder 기본값 설정 (청크 폴더)
    if "db_stat_failed_folder" not in config:
        if "db_stat_failed_file" in config:
            db_dir = os.path.dirname(config["db_stat_failed_file"])
            config["db_stat_failed_folder"] = os.path.join(db_dir, "stat_failed_jobs")
        elif "db_completed_file" in config:
            db_dir = os.path.dirname(config["db_completed_file"])
            config["db_stat_failed_folder"] = os.path.join(db_dir, "stat_failed_jobs")
        else:
            config["db_stat_failed_folder"] = f"/home/hashjamm/results/disease_network/{script_name}_job_queue_db/stat_failed_jobs"
    
    # db_stat_excluded_file 기본값 설정 (중앙 DB)
    if "db_stat_excluded_file" not in config:
        if "db_completed_file" in config:
            db_dir = os.path.dirname(config["db_completed_file"])
            config["db_stat_excluded_file"] = os.path.join(db_dir, "stat_excluded_jobs.duckdb")
        else:
            config["db_stat_excluded_file"] = f"/home/hashjamm/results/disease_network/{script_name}_job_queue_db/stat_excluded_jobs.duckdb"
    
    # db_stat_excluded_folder 기본값 설정 (청크 폴더)
    if "db_stat_excluded_folder" not in config:
        if "db_stat_excluded_file" in config:
            db_dir = os.path.dirname(config["db_stat_excluded_file"])
            config["db_stat_excluded_folder"] = os.path.join(db_dir, "stat_excluded_jobs")
        elif "db_completed_file" in config:
            db_dir = os.path.dirname(config["db_completed_file"])
            config["db_stat_excluded_folder"] = os.path.join(db_dir, "stat_excluded_jobs")
        else:
            config["db_stat_excluded_folder"] = f"/home/hashjamm/results/disease_network/{script_name}_job_queue_db/stat_excluded_jobs"
    
    if "pipe_log" not in config:
        config["pipe_log"] = f"/tmp/tmux_monitor/{config['tmux_session']}.log"
    
    return config

# ============================================================================
# API 엔드포인트
# ============================================================================

@app.route('/api/files/tree')
def get_file_tree():
    """파일 트리 구조 반환"""
    base_path = request.args.get('path', '/home/hashjamm/codes/disease_network')
    base_path = os.path.expanduser(base_path)
    
    if not os.path.exists(base_path):
        return jsonify({'error': '경로를 찾을 수 없습니다'}), 404
    
    def build_tree(path, max_depth=4, current_depth=0):
        """재귀적으로 파일 트리 구성"""
        if current_depth >= max_depth:
            return None
        
        try:
            items = []
            for item in sorted(os.listdir(path)):
                item_path = os.path.join(path, item)
                if os.path.isdir(item_path):
                    # 숨김 디렉토리 제외
                    if item.startswith('.'):
                        continue
                    subtree = build_tree(item_path, max_depth, current_depth + 1)
                    items.append({
                        'name': item,
                        'type': 'directory',
                        'path': item_path,
                        'children': subtree if subtree else []
                    })
                elif item.endswith(('.R', '.sh', '.py')):
                    items.append({
                        'name': item,
                        'type': 'file',
                        'path': item_path,
                        'extension': os.path.splitext(item)[1]
                    })
            return items
        except PermissionError:
            return []
    
    tree = build_tree(base_path)
    return jsonify({'tree': tree, 'base_path': base_path})

@app.route('/api/projects', methods=['GET'])
def get_projects():
    """등록된 프로젝트 목록 반환"""
    config = load_projects_config()
    return jsonify(config)

@app.route('/api/detect-tmux', methods=['POST'])
def detect_tmux_session():
    """tmux 세션에서 자동으로 설정 감지"""
    data = request.json
    tmux_session = data.get('tmux_session')
    
    if not tmux_session:
        return jsonify({'error': 'tmux 세션 이름이 필요합니다'}), 400
    
    config = detect_from_tmux_session(tmux_session)
    
    if 'error' in config:
        return jsonify(config), 404
    
    return jsonify({'success': True, 'config': config})

@app.route('/api/projects', methods=['POST'])
def register_project():
    """새 프로젝트 등록"""
    data = request.json
    
    project_name = data.get('name')
    r_script = data.get('r_script')
    shell_script = data.get('shell_script')
    
    if not project_name or not r_script:
        return jsonify({'error': '프로젝트 이름과 R 스크립트 경로는 필수입니다'}), 400
    
    if not os.path.exists(r_script):
        return jsonify({'error': 'R 스크립트 파일을 찾을 수 없습니다'}), 404
    
    if shell_script and not os.path.exists(shell_script):
        return jsonify({'error': '쉘 스크립트 파일을 찾을 수 없습니다'}), 404
    
    # 자동 설정 생성 (기본값으로 사용)
    config = auto_detect_project_config(r_script, shell_script)
    config['name'] = project_name
    config['created_at'] = datetime.now().isoformat()
    
    # 사용자가 입력한 경로들을 우선 적용 (빈 값이 아닌 경우만)
    if 'db_completed_file' in data and data['db_completed_file']:
        config['db_completed_file'] = data['db_completed_file']
        if 'db_path_type' not in config:
            config['db_path_type'] = 'static'
    if 'db_completed_folder' in data and data['db_completed_folder']:
        config['db_completed_folder'] = data['db_completed_folder']
    if 'db_system_failed_file' in data and data['db_system_failed_file']:
        config['db_system_failed_file'] = data['db_system_failed_file']
    if 'db_system_failed_folder' in data and data['db_system_failed_folder']:
        config['db_system_failed_folder'] = data['db_system_failed_folder']
    if 'db_stat_failed_file' in data and data['db_stat_failed_file']:
        config['db_stat_failed_file'] = data['db_stat_failed_file']
    if 'db_stat_failed_folder' in data and data['db_stat_failed_folder']:
        config['db_stat_failed_folder'] = data['db_stat_failed_folder']
    if 'db_stat_excluded_file' in data and data['db_stat_excluded_file']:
        config['db_stat_excluded_file'] = data['db_stat_excluded_file']
    if 'db_stat_excluded_folder' in data and data['db_stat_excluded_folder']:
        config['db_stat_excluded_folder'] = data['db_stat_excluded_folder']
    if 'structured_log' in data and data['structured_log']:
        config['structured_log'] = data['structured_log']
    if 'pipe_log' in data and data['pipe_log']:
        config['pipe_log'] = data['pipe_log']
    if 'tmux_session' in data and data['tmux_session']:
        config['tmux_session'] = data['tmux_session']
    if 'process_pattern' in data and data['process_pattern']:
        config['process_pattern'] = data['process_pattern']
    
    # 프로젝트 저장
    projects_config = load_projects_config()
    projects_config['projects'][project_name] = config
    save_projects_config(projects_config)
    
    return jsonify({'success': True, 'project': config})

@app.route('/api/projects/<project_name>', methods=['GET'])
def get_project(project_name):
    """개별 프로젝트 설정 조회"""
    projects_config = load_projects_config()
    if project_name in projects_config.get('projects', {}):
        return jsonify(projects_config['projects'][project_name])
    return jsonify({'error': '프로젝트를 찾을 수 없습니다'}), 404

@app.route('/api/projects/<project_name>', methods=['DELETE'])
def delete_project(project_name):
    """프로젝트 삭제"""
    projects_config = load_projects_config()
    if project_name in projects_config['projects']:
        del projects_config['projects'][project_name]
        save_projects_config(projects_config)
        return jsonify({'success': True})
    return jsonify({'error': '프로젝트를 찾을 수 없습니다'}), 404

@app.route('/api/projects/<project_name>', methods=['PUT'])
def update_project(project_name):
    """프로젝트 설정 업데이트"""
    projects_config = load_projects_config()
    if project_name not in projects_config['projects']:
        return jsonify({'error': '프로젝트를 찾을 수 없습니다'}), 404
    
    data = request.json
    projects_config['projects'][project_name].update(data)
    projects_config['projects'][project_name]['updated_at'] = datetime.now().isoformat()
    save_projects_config(projects_config)
    
    return jsonify({'success': True, 'project': projects_config['projects'][project_name]})

@app.route('/api/switch-project', methods=['POST'])
def switch_project():
    """프로젝트 전환 (동적)"""
    global _current_project_name, _current_project_config, _global_collector, _global_error_analyzer, _global_process_monitor
    
    data = request.json
    project_name = data.get('project_name')
    
    if not project_name:
        return jsonify({'error': '프로젝트 이름이 필요합니다'}), 400
    
    projects_config = load_projects_config()
    if project_name not in projects_config.get('projects', {}):
        return jsonify({'error': '프로젝트를 찾을 수 없습니다'}), 404
    
    # 현재 프로젝트 설정 업데이트
    _current_project_name = project_name
    _current_project_config = projects_config['projects'][project_name]
    
    # 메트릭 수집기 및 에러 분석기 즉시 재초기화 (새 DB 경로 사용)
    _global_collector = None
    _global_error_analyzer = None
    
    # 즉시 재초기화하여 새 프로젝트 설정 반영
    _global_collector = ComprehensiveMetricsCollector()
    _global_error_analyzer = ErrorAnalyzer()
    
    # 프로세스 모니터 업데이트
    if _global_process_monitor:
        _global_process_monitor.update_pattern(_current_project_config.get('process_pattern', 'hr_rr_mapping_validation_engine'))
    else:
        _global_process_monitor = ProcessMonitor()
    
    return jsonify({
        'success': True,
        'project': _current_project_config,
        'message': '프로젝트가 전환되었습니다. 모니터링이 새 프로젝트를 추적합니다.'
    })

@app.route('/api/current-project')
def get_current_project():
    """현재 선택된 프로젝트 설정 반환"""
    project_name = request.args.get('project')
    if not project_name:
        # URL 파라미터가 없으면 기본 프로젝트 사용
        projects_config = load_projects_config()
        project_name = projects_config.get('default_project', 'validation')
    
    projects_config = load_projects_config()
    if project_name in projects_config.get('projects', {}):
        return jsonify({
            'name': project_name,
            'config': projects_config['projects'][project_name]
        })
    return jsonify({'error': '프로젝트를 찾을 수 없습니다'}), 404

@app.route('/api/structured_log')
def get_structured_log():
    """
    구조화된 로그 데이터 반환 (REST API)
    
    기존 로그 파일에서 파싱한 통계 정보를 반환합니다.
    
    Returns:
        JSON: 파싱된 로그 통계 정보
    """
    stats = parse_structured_log()
    if stats:
        return jsonify(stats)
    return jsonify({'error': '로그 파일을 읽을 수 없습니다'}), 404

if __name__ == '__main__':
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # 백그라운드에서 pipe-pane 로그 모니터링 시작
    log_thread = threading.Thread(target=tail_pipe_log, daemon=True)
    log_thread.start()
    
    # 포괄적 메트릭 수집 시작
    metrics_thread = threading.Thread(target=collect_and_send_metrics, daemon=True)
    metrics_thread.start()
    
    print("=" * 70)
    print("고급 tmux 모니터링 시스템 시작")
    print("=" * 70)
    print(f"세션: {TMUX_SESSION}")
    print(f"pipe-pane 로그: {PIPE_LOG}")
    print(f"구조화된 로그 (읽기 전용): {STRUCTURED_LOG}")
    print(f"\n웹 브라우저에서 http://localhost:5000 접속")
    print(f"\n⚠️  기존 프로세스에 영향을 주지 않습니다:")
    print(f"   - 기존 로그 파일은 읽기 전용으로 사용")
    print(f"   - pipe-pane은 출력만 복사 (원본 프로세스 영향 없음)")
    print(f"   - 시스템 직접 추출 방식으로 메트릭 수집")
    print(f"\n📝 pipe-pane 로깅 시작 (tmux 세션에서):")
    print(f"   tmux pipe-pane -t {TMUX_SESSION} -o 'cat >> {PIPE_LOG}'")
    print("=" * 70)
    
    # localhost만 리스닝 (보안 강화)
    socketio.run(app, host='127.0.0.1', port=5000, debug=False, allow_unsafe_werkzeug=True)



