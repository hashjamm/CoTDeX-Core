# 동적 프로젝트 전환 기능 구현 작업 진행 상황

## 프로젝트 개요

**목표**: `tmux_monitor_advanced.py` 모니터링 시스템을 여러 프로젝트를 동적으로 전환하며 모니터링할 수 있도록 확장

**현재 상태**: 대부분 완료, 2개 필수 수정 남음

**작업 시작일**: 2024년 (이전 대화에서 시작)

---

## 작업 배경

기존 `tmux_monitor_advanced.py`는 `validation` 프로젝트에 하드코딩되어 있어, 다른 R 스크립트나 쉘 스크립트를 모니터링하려면 서버를 재시작해야 했습니다. 사용자가 프론트엔드에서 프로젝트를 선택하면 즉시 해당 프로젝트의 모니터링이 시작되도록 동적 전환 기능을 구현 중입니다.

---

## 핵심 아키텍처 변경사항

### 1. 전역 프로젝트 설정 관리
- `_current_project_name`: 현재 선택된 프로젝트 이름
- `_current_project_config`: 현재 프로젝트의 설정 딕셔너리
- `get_current_project_config()`: 동적으로 현재 프로젝트 설정을 반환하는 함수

### 2. 동적 설정 적용 원칙
모든 모니터링 함수/클래스가 실행 시점에 `get_current_project_config()`를 호출하여:
- `tmux_session`: tmux 세션 이름
- `pipe_log`: pipe-pane 로그 파일 경로
- `structured_log`: 구조화된 로그 파일 경로
- `db_completed_file`: 완료 작업 DB 파일 경로
- `db_completed_folder`: 완료 작업 DB 폴더 경로
- `db_system_failed_file`: 실패 작업 DB 파일 경로
- `db_system_failed_folder`: 실패 작업 DB 폴더 경로
- `process_pattern`: 프로세스 검색 패턴 (예: "hr_calculator_engine_v5")

---

## 완료된 작업 목록

### ✅ 1. 전역 변수 및 핵심 함수 추가
**파일**: `tmux_monitor_advanced.py`  
**위치**: 1084-1128줄

```python
# 전역 변수
_current_project_name = None
_current_project_config = None

# 동적 설정 가져오기 함수
def get_current_project_config():
    # Flask request context 확인
    # 캐시된 설정 사용
    # 기본 프로젝트 사용
    # 기본값 반환
```

### ✅ 2. tail_pipe_log() 함수 수정
**파일**: `tmux_monitor_advanced.py`  
**위치**: 253-261줄

- `PIPE_LOG`, `TMUX_SESSION` 하드코딩 제거
- `get_current_project_config()`로 동적 경로 가져오기

### ✅ 3. ComprehensiveMetricsCollector 클래스 수정
**파일**: `tmux_monitor_advanced.py`

#### 3.1 __init__() 메서드
**위치**: 330-340줄
- DB 경로들을 `get_current_project_config()`로 초기화

#### 3.2 get_completed_jobs_count() 메서드
**위치**: 343-374줄
- 함수 내부에서 매번 `get_current_project_config()` 호출하여 최신 설정 사용

#### 3.3 get_failed_jobs_count() 메서드
**위치**: 411-443줄
- 함수 내부에서 매번 `get_current_project_config()` 호출하여 최신 설정 사용

### ✅ 4. ProcessMonitor 클래스 수정
**파일**: `tmux_monitor_advanced.py`

#### 4.1 __init__() 메서드
**위치**: 603-610줄
- `process_pattern`을 `get_current_project_config()`로 초기화
- `update_pattern()` 메서드 추가 (610-612줄)

#### 4.2 get_process_count() 메서드
**위치**: 611-629줄
- 함수 내부에서 매번 `get_current_project_config()` 호출하여 최신 `process_pattern` 사용

### ✅ 5. collect_and_send_metrics() 함수 수정
**파일**: `tmux_monitor_advanced.py`  
**위치**: 1176-1180줄

- 프로세스 모니터의 패턴을 동적으로 업데이트하는 로직 추가

### ✅ 6. 프로젝트 전환 API 엔드포인트 추가
**파일**: `tmux_monitor_advanced.py`  
**위치**: 3734-3764줄

```python
@app.route('/api/switch-project', methods=['POST'])
def switch_project():
    # 전역 변수 업데이트
    # 메트릭 수집기 재초기화
    # 프로세스 모니터 패턴 업데이트
```

### ✅ 7. 프론트엔드 JavaScript 수정
**파일**: `tmux_monitor_advanced.py` (HTML_TEMPLATE 내부)  
**위치**: 3214-3243줄

- 프로젝트 선택 시 `/api/switch-project` API 호출
- 성공 시 URL 업데이트 및 알림 표시

---

## 남은 작업 목록

### 🔴 필수 수정 1: ProcessMonitor.get_process_status() 함수
**파일**: `tmux_monitor_advanced.py`  
**위치**: 636-660줄  
**우선순위**: 높음

**현재 문제점**:
- 648줄에서 `self.process_pattern`을 직접 사용
- 프로젝트 전환 후에도 이전 프로젝트의 패턴을 계속 사용할 수 있음

**수정 방법**:
```python
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
            if process_pattern in line and 'grep' not in line:  # 수정: self.process_pattern -> process_pattern
                # ... 나머지 코드 동일
```

**수정 위치**: 636줄 함수 시작 부분에 `project_config` 및 `process_pattern` 변수 추가, 648줄의 `self.process_pattern`을 `process_pattern`로 변경

---

### 🔴 필수 수정 2: ErrorAnalyzer.get_error_statistics() 함수
**파일**: `tmux_monitor_advanced.py`  
**위치**: 532-595줄  
**우선순위**: 높음

**현재 문제점**:
- 549줄과 579줄에서 `db_system_failed_file`, `db_system_failed_folder` 변수를 사용하지만 정의되지 않음
- `self.db_system_failed_file`을 사용해야 하지만 동적 프로젝트 설정이 적용되지 않음

**수정 방법**:
```python
def get_error_statistics(self):
    """에러 통계 조회"""
    if not DUCKDB_AVAILABLE:
        return {
            'total_errors': 0,
            'error_rate': 0.0,
            'error_types': {},
            'recent_errors': []
        }
    
    # 매번 현재 프로젝트 설정 가져오기 (추가 필요)
    project_config = get_current_project_config()
    db_system_failed_file = project_config.get('db_system_failed_file', self.db_system_failed_file)
    db_system_failed_folder = project_config.get('db_system_failed_folder', self.db_system_failed_folder)
    
    total_errors = 0
    error_types = defaultdict(int)
    recent_errors = []
    
    # 중앙 DB 확인
    if os.path.exists(db_system_failed_file):  # 수정: self.db_system_failed_file -> db_system_failed_file
        try:
            conn = duckdb.connect(db_system_failed_file, read_only=True)  # 이미 올바름
            # ... 나머지 코드
    
    # 청크 파일들 확인
    if os.path.exists(db_system_failed_folder):  # 수정: db_system_failed_folder 변수 사용 (579줄)
        for chunk_file in Path(db_system_failed_folder).glob("system_failed_chunk_*.duckdb"):
            # ... 나머지 코드
```

**수정 위치**: 
- 542줄 이후 (변수 선언 전)에 `project_config` 및 `db_system_failed_file`, `db_system_failed_folder` 변수 추가
- 547줄: `self.db_system_failed_file` → `db_system_failed_file`
- 579줄: `db_system_failed_folder` 변수가 이미 사용 중이지만, 함수 시작 부분에서 정의 필요

---

### 🟡 선택사항: ErrorAnalyzer.__init__() 메서드
**파일**: `tmux_monitor_advanced.py`  
**위치**: 528-530줄  
**우선순위**: 낮음 (일관성을 위한 권장사항)

**현재 상태**: 하드코딩된 전역 변수 사용

**수정 방법** (선택사항):
```python
def __init__(self):
    project_config = get_current_project_config()
    self.db_system_failed_file = project_config.get('db_system_failed_file', DB_SYSTEM_FAILED_FILE)
    self.db_system_failed_folder = project_config.get('db_system_failed_folder', DB_SYSTEM_FAILED_FOLDER)
```

**참고**: `get_error_statistics()`에서 이미 동적 설정을 사용하므로, 이 수정은 선택사항입니다.

---

## 관련 파일 경로

### 주요 파일
- **메인 모니터링 스크립트**: `/home/hashjamm/codes/disease_network/tmux_monitor_advanced.py`
- **프로젝트 설정 파일**: `~/.monitor_projects.json` (자동 생성됨)
- **기본 프로젝트 예시**: `validation` 프로젝트

### 참고 파일
- **R 스크립트 예시**: `/home/hashjamm/codes/disease_network/hr_calculator_engine_v5.R`
- **쉘 스크립트 예시**: `/home/hashjamm/codes/disease_network/hr_analysis_manager.sh`

---

## 테스트 방법

### 1. 기본 동작 확인
```bash
# 모니터링 서버 시작
cd /home/hashjamm/codes/disease_network
python3 tmux_monitor_advanced.py
```

### 2. 프로젝트 전환 테스트
1. 브라우저에서 `http://localhost:5000` 접속
2. 프로젝트 선택 드롭다운에서 다른 프로젝트 선택
3. `/api/switch-project` API가 호출되는지 확인 (브라우저 개발자 도구 Network 탭)
4. 프로세스 모니터링이 새 프로젝트의 `process_pattern`을 사용하는지 확인

### 3. 남은 수정사항 테스트
수정 완료 후:
- **get_process_status()**: 프로젝트 전환 후 프로세스 목록이 올바른 패턴으로 필터링되는지 확인
- **get_error_statistics()**: 프로젝트 전환 후 에러 통계가 올바른 DB 파일에서 조회되는지 확인

---

## 기술적 세부사항

### 프로젝트 설정 구조
`~/.monitor_projects.json` 파일 형식:
```json
{
  "default_project": "validation",
  "projects": {
    "validation": {
      "tmux_session": "hr_validation",
      "pipe_log": "/home/hashjamm/codes/disease_network/logs/pipe_pane.log",
      "structured_log": "/home/hashjamm/codes/disease_network/logs/structured.log",
      "db_completed_file": "/home/hashjamm/codes/disease_network/db/completed_jobs.duckdb",
      "db_completed_folder": "/home/hashjamm/codes/disease_network/db/completed_chunks",
      "db_system_failed_file": "/home/hashjamm/codes/disease_network/db/system_failed.duckdb",
      "db_system_failed_folder": "/home/hashjamm/codes/disease_network/db/system_failed_chunks",
      "process_pattern": "hr_rr_mapping_validation_engine",
      "r_script": "/home/hashjamm/codes/disease_network/hr_calculator_engine_v5.R",
      "shell_script": "/home/hashjamm/codes/disease_network/hr_analysis_manager.sh"
    }
  }
}
```

### 동적 설정 우선순위
1. Flask request context의 `?project=` 파라미터
2. 캐시된 전역 변수 (`_current_project_name`, `_current_project_config`)
3. `default_project` 설정
4. 하드코딩된 기본값

---

## 다음 단계 (수정 완료 후)

1. **남은 2개 필수 수정 완료**
   - `ProcessMonitor.get_process_status()` 수정
   - `ErrorAnalyzer.get_error_statistics()` 수정

2. **통합 테스트**
   - 여러 프로젝트 등록 및 전환 테스트
   - 각 프로젝트별 모니터링 데이터 정확성 확인

3. **문서화 업데이트**
   - `TMUX_MONITOR_README.md`에 동적 프로젝트 전환 기능 설명 추가

4. **에러 처리 강화** (선택사항)
   - 프로젝트 전환 실패 시 롤백 로직
   - 잘못된 프로젝트 설정에 대한 검증

---

## 주의사항

1. **서버 재시작 불필요**: 프로젝트 전환은 동적으로 이루어지므로 서버 재시작 없이 작동해야 합니다.

2. **백그라운드 스레드**: `tail_pipe_log()`와 `collect_and_send_metrics()`는 백그라운드 스레드에서 실행되므로, `get_current_project_config()`가 Flask request context 없이도 작동해야 합니다.

3. **변수 스코프**: `get_error_statistics()` 함수 내에서 `db_system_failed_file`과 `db_system_failed_folder` 변수를 로컬 변수로 정의해야 합니다.

4. **일관성**: 모든 모니터링 함수가 동일한 패턴(함수 시작 부분에서 `get_current_project_config()` 호출)을 따르도록 해야 합니다.

---

## 작업 완료 체크리스트

- [x] 전역 변수 및 `get_current_project_config()` 함수 추가
- [x] `tail_pipe_log()` 함수 수정
- [x] `ComprehensiveMetricsCollector` 클래스 수정 (3개 메서드)
- [x] `ProcessMonitor` 클래스 수정 (`__init__`, `get_process_count`)
- [x] `collect_and_send_metrics()` 함수 수정
- [x] `/api/switch-project` API 엔드포인트 추가
- [x] 프론트엔드 JavaScript 수정
- [ ] `ProcessMonitor.get_process_status()` 함수 수정 (필수)
- [ ] `ErrorAnalyzer.get_error_statistics()` 함수 수정 (필수)
- [ ] `ErrorAnalyzer.__init__()` 메서드 수정 (선택사항)

---

## 문의 및 참고

- 이전 대화 기록: `/home/hashjamm/.cursor/projects/home-hashjamm/agent-transcripts/6efe7ac1-292b-4312-82c2-14bbf7ca258c.txt`
- 관련 패치 파일: `/home/hashjamm/codes/disease_network/monitor_dynamic_project.patch` (참고용)
- 관련 가이드: `/home/hashjamm/codes/disease_network/DYNAMIC_PROJECT_FIX.md` (참고용)

---

**마지막 업데이트**: 작업 진행 중 (2024년)
**작업 상태**: 80% 완료 (8/10 항목 완료, 2개 필수 수정 남음)
