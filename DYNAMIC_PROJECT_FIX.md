# 동적 프로젝트 지원 수정 가이드

프로젝트 선택 시 서버 재시작 없이 모니터링이 동적으로 변경되도록 수정하는 방법입니다.

## 수정 사항 요약

### 1. 전역 변수 추가 (1054줄 다음)

```python
# 현재 프로젝트 설정 (동적 변경 가능)
_current_project_name = None
_current_project_config = None
```

### 2. 동적 프로젝트 설정 함수 추가 (1055줄 다음)

```python
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
```

### 3. tail_pipe_log() 함수 수정 (245줄)

**기존:**
```python
if not os.path.exists(PIPE_LOG):
    print(f"[INFO] pipe-pane 로그 파일이 없습니다: {PIPE_LOG}")
    ...
print(f"[INFO] pipe-pane 로그 모니터링 시작: {PIPE_LOG}")
with open(PIPE_LOG, 'r', encoding='utf-8', errors='ignore') as f:
```

**수정:**
```python
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
```

### 4. ComprehensiveMetricsCollector 클래스 수정 (330줄)

**__init__ 메서드:**
```python
def __init__(self):
    project_config = get_current_project_config()
    self.db_completed_file = project_config.get('db_completed_file', DB_COMPLETED_FILE)
    self.db_completed_folder = project_config.get('db_completed_folder', DB_COMPLETED_FOLDER)
    self.db_system_failed_file = project_config.get('db_system_failed_file', DB_SYSTEM_FAILED_FILE)
    self.db_system_failed_folder = project_config.get('db_system_failed_folder', DB_SYSTEM_FAILED_FOLDER)
    self.matched_parquet_folder = MATCHED_PARQUET_FOLDER
    self.results_mapping_folder = RESULTS_MAPPING_FOLDER
    
    self.last_completed_count = 0
    self.last_check_time = None
    self.total_jobs_cache = None
    self.start_time = None
```

**get_completed_jobs_count() 메서드 (343줄):**
```python
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
            print(f"[WARNING] 중앙 DB 조회 실패: {e}")
    
    # 청크 파일들 확인
    if os.path.exists(db_completed_folder):
        # ... 나머지 코드 동일
```

### 5. ProcessMonitor 클래스 수정 (589줄)

**__init__ 메서드:**
```python
def __init__(self, process_pattern=None):
    if process_pattern:
        self.process_pattern = process_pattern
    else:
        project_config = get_current_project_config()
        self.process_pattern = project_config.get('process_pattern', 'hr_rr_mapping_validation_engine')

def update_pattern(self, process_pattern):
    """프로세스 패턴 업데이트"""
    self.process_pattern = process_pattern
```

**get_process_count() 메서드 (592줄):**
```python
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
        # ... 나머지 코드 동일
```

### 6. collect_and_send_metrics() 함수 수정 (1104줄 근처)

```python
# 프로세스 상태
# 현재 프로젝트 설정으로 프로세스 모니터 업데이트
project_config = get_current_project_config()
if project_config.get('process_pattern'):
    process_monitor.update_pattern(project_config['process_pattern'])

process_count = process_monitor.get_process_count()
process_status = process_monitor.get_process_status()
```

### 7. 프로젝트 변경 API 추가 (3637줄 다음)

```python
@app.route('/api/switch-project', methods=['POST'])
def switch_project():
    """프로젝트 전환 (동적)"""
    global _current_project_name, _current_project_config, _global_collector, _global_process_monitor
    
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
    
    # 메트릭 수집기 재초기화 (새 DB 경로 사용)
    _global_collector = None
    
    # 프로세스 모니터 업데이트
    if _global_process_monitor:
        _global_process_monitor.update_pattern(_current_project_config.get('process_pattern', 'hr_rr_mapping_validation_engine'))
    
    return jsonify({
        'success': True,
        'project': _current_project_config,
        'message': '프로젝트가 전환되었습니다. 모니터링이 새 프로젝트를 추적합니다.'
    })
```

### 8. JavaScript 수정 (프로젝트 선택 시 API 호출)

프로젝트 선택 이벤트 핸들러에서:
```javascript
document.getElementById('project-dropdown').addEventListener('change', async function(e) {
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
```

## 적용 방법

1. 위의 수정사항을 `tmux_monitor_advanced.py`에 적용
2. 서버 재시작
3. 브라우저에서 프로젝트 선택 시 자동으로 모니터링이 전환됨

## 주의사항

- `tail_pipe_log()` 함수는 백그라운드 스레드에서 실행되므로 Flask request context가 없을 수 있습니다
- 이 경우 캐시된 `_current_project_config`를 사용하거나 기본값을 사용합니다
- 프로젝트 전환 시 `_current_project_config`를 업데이트하면 다음 루프에서 새 설정이 적용됩니다
