# tmux 모니터링 시스템 사용 가이드

## 개요
기존 실행 중인 작업에 **영향을 주지 않으면서** tmux 세션을 웹 브라우저에서 실시간으로 모니터링할 수 있는 시스템입니다.

## 안전성 보장
- ✅ 기존 로그 파일은 **읽기 전용**으로 사용 (기존 프로세스 영향 없음)
- ✅ pipe-pane은 출력만 **복사** (원본 프로세스 영향 없음)
- ✅ 웹 서버는 별도 프로세스로 실행 (기존 작업과 독립)

## 구성 요소

### 1. 기존 로그 파일 (읽기 전용)
- 경로: `/home/hashjamm/results/disease_network/hr_rr_mapping_validation_manager_v2_history.log`
- 용도: 구조화된 데이터 분석, 히스토리 추적

### 2. pipe-pane 로그 (실시간 복사)
- 경로: `/tmp/tmux_monitor/validation.log`
- 용도: 실시간 화면 출력 모니터링, 프로그래스 바 추적

### 3. 웹 서버
- 포트: `5000`
- 경로: `/home/hashjamm/codes/disease_network/tmux_monitor_advanced.py`

## 사용 방법

### 1. pipe-pane 로깅 시작 (한 번만 실행)
```bash
tmux pipe-pane -t validation -o 'cat >> /tmp/tmux_monitor/validation.log'
```

### 2. 웹 서버 실행
```bash
cd /home/hashjamm/codes/disease_network
python3 tmux_monitor_advanced.py > /tmp/tmux_monitor/server.log 2>&1 &
```

또는 백그라운드로:
```bash
nohup python3 /home/hashjamm/codes/disease_network/tmux_monitor_advanced.py > /tmp/tmux_monitor/server.log 2>&1 &
```

### 3. 웹 브라우저 접속

#### 로컬 접속 (서버에 직접 로그인된 경우)
**방법 1: 자동 스크립트 사용 (권장)**
```bash
bash /home/hashjamm/codes/disease_network/open_monitor.sh
```

**방법 2: 수동으로 브라우저 열기**
```bash
xdg-open http://localhost:5000
# 또는
firefox http://localhost:5000
```

#### 원격 접속 (다른 컴퓨터에서 - SSH 터널링 필요)
**보안상 서버는 localhost만 리스닝하므로 SSH 터널링이 필요합니다:**

1. SSH 터널 생성:
   ```bash
   ssh -L 5000:localhost:5000 사용자명@서버IP
   ```

2. 브라우저에서 접속:
   ```
   http://localhost:5000
   ```

자세한 내용은 `ssh_tunnel_guide.md` 참고

## 기능

### 실시간 대시보드
- 진행률 표시 (프로그래스 바)
- 완료 작업 수
- 처리 속도 (jobs/sec)
- 예상 완료 시간 (ETA)

### 시각화 차트
- 진행률 추이 그래프
- 처리 속도 그래프
- 에러 발생 추이
- 시스템 리소스 사용량 (CPU, 메모리)

### 실시간 로그
- 화면에 표시되는 모든 출력 실시간 표시
- 에러 메시지 자동 강조

## 서버 관리

### 서버 상태 확인
```bash
ps aux | grep tmux_monitor_advanced.py
```

### 서버 로그 확인
```bash
tail -f /tmp/tmux_monitor/server.log
```

### 서버 종료
```bash
pkill -f tmux_monitor_advanced.py
```

또는 PID 파일 사용:
```bash
kill $(cat /tmp/tmux_monitor/server.pid)
```

## pipe-pane 관리

### pipe-pane 중지
```bash
tmux pipe-pane -t validation
```

### pipe-pane 상태 확인
```bash
tmux show-options -t validation -v pipe-pane
```

## 원격 접속 (SSH 터널링)

### 보안 설정
서버는 **localhost(127.0.0.1)만 리스닝**하도록 설정되어 있어, SSH 로그인을 통해서만 접속할 수 있습니다.

### SSH 터널 생성
다른 컴퓨터에서:
```bash
ssh -L 5000:localhost:5000 사용자명@서버IP
```

백그라운드로 실행:
```bash
ssh -f -N -L 5000:localhost:5000 사용자명@서버IP
```

### 브라우저 접속
터널 생성 후 로컬 브라우저에서:
```
http://localhost:5000
```

자세한 내용은 `ssh_tunnel_guide.md` 파일 참고

## 문제 해결

### 웹 서버가 시작되지 않을 때
1. 포트 5000이 사용 중인지 확인:
   ```bash
   netstat -tlnp | grep :5000
   ```
2. 다른 포트 사용 (코드에서 포트 번호 변경)

### pipe-pane 로그가 생성되지 않을 때
1. tmux 세션이 실행 중인지 확인:
   ```bash
   tmux list-sessions
   ```
2. pipe-pane 명령 다시 실행

### 기존 프로세스에 영향이 있는지 확인
```bash
# R 프로세스 확인
ps aux | grep hr_rr_mapping_validation

# 기존 로그 파일이 계속 쓰여지는지 확인
tail -f /home/hashjamm/results/disease_network/hr_rr_mapping_validation_manager_v2_history.log
```

### 다른 컴퓨터에서 접속이 안 될 때
1. **SSH 터널 확인**
   ```bash
   # 터널이 생성되어 있는지 확인
   netstat -tlnp | grep :5000
   ss -tlnp | grep :5000
   ```

2. **SSH 연결 확인**
   ```bash
   # SSH 서버 상태 확인 (서버에서)
   sudo systemctl status ssh
   ```

3. **터널 재생성**
   ```bash
   # 기존 터널 종료
   pkill -f "ssh.*5000:localhost:5000"
   
   # 새 터널 생성
   ssh -L 5000:localhost:5000 사용자명@서버IP
   ```

## 주의사항

1. **기존 프로세스는 절대 건드리지 않습니다**
   - 로그 파일은 읽기 전용
   - pipe-pane은 출력만 복사
   - 웹 서버는 별도 프로세스

2. **디스크 공간 확인**
   - pipe-pane 로그 파일이 계속 커질 수 있음
   - 필요시 로그 로테이션 설정 권장

3. **네트워크 보안**
   - 기본적으로 모든 IP에서 접속 가능 (0.0.0.0)
   - 필요시 방화벽 설정 권장

## 파일 위치

- 웹 서버 코드: `/home/hashjamm/codes/disease_network/tmux_monitor_advanced.py`
- pipe-pane 로그: `/tmp/tmux_monitor/validation.log`
- 서버 로그: `/tmp/tmux_monitor/server.log`
- 기존 로그: `/home/hashjamm/results/disease_network/hr_rr_mapping_validation_manager_v2_history.log`

