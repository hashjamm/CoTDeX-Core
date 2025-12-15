# SSH 터널링을 통한 원격 모니터링 접속 가이드

## 개요
모니터링 서버는 **localhost(127.0.0.1)만 리스닝**하도록 설정되어 있어, SSH 로그인을 통해서만 접속할 수 있습니다.

## 보안 설정
- ✅ 서버는 localhost만 리스닝 (외부 직접 접속 불가)
- ✅ SSH 터널링을 통해서만 접속 가능
- ✅ SSH 인증이 필요하므로 보안 강화

## 원격 접속 방법 (SSH 터널링)

### 1. SSH 터널 생성
다른 컴퓨터에서 다음 명령 실행:

```bash
ssh -L 5000:localhost:5000 사용자명@서버IP
```

예시:
```bash
ssh -L 5000:localhost:5000 hashjamm@192.168.0.222
```

### 2. 백그라운드로 SSH 터널 실행 (선택사항)
SSH 세션을 유지하면서 터널만 생성:

```bash
ssh -f -N -L 5000:localhost:5000 사용자명@서버IP
```

옵션 설명:
- `-f`: 백그라운드 실행
- `-N`: 원격 명령 실행 안 함 (터널만 생성)

### 3. 브라우저에서 접속
터널이 생성된 후, 로컬 컴퓨터의 브라우저에서:

```
http://localhost:5000
```

## 로컬 접속 방법 (서버에 직접 로그인된 경우)

### 방법 1: 스크립트 사용 (권장)
```bash
bash /home/hashjamm/codes/disease_network/open_monitor.sh
```

### 방법 2: 수동으로 브라우저 열기
```bash
# 브라우저 자동 열기
xdg-open http://localhost:5000

# 또는 Firefox
firefox http://localhost:5000

# 또는 Chrome
google-chrome http://localhost:5000
```

### 방법 3: 텍스트 브라우저 (터미널에서)
```bash
# lynx 사용
lynx http://localhost:5000

# 또는 curl로 API만 확인
curl http://localhost:5000/api/stats | python3 -m json.tool
```

## SSH 터널 관리

### 터널 상태 확인
```bash
# 로컬 포트 리스닝 확인
netstat -tlnp | grep :5000
ss -tlnp | grep :5000
```

### 터널 종료
```bash
# SSH 프로세스 찾기
ps aux | grep "ssh.*5000:localhost:5000"

# 종료
pkill -f "ssh.*5000:localhost:5000"
```

## 문제 해결

### SSH 터널이 작동하지 않을 때
1. **SSH 서버 설정 확인**
   ```bash
   # 서버에서 SSH 설정 확인
   sudo systemctl status ssh
   ```

2. **포트 포워딩 확인**
   ```bash
   # SSH 설정에서 AllowTcpForwarding 확인
   grep AllowTcpForwarding /etc/ssh/sshd_config
   ```

3. **방화벽 확인**
   - SSH 포트(22)가 열려있는지 확인

### 브라우저가 열리지 않을 때
1. **DISPLAY 환경변수 확인** (X11 포워딩)
   ```bash
   echo $DISPLAY
   ```

2. **X11 포워딩으로 SSH 접속**
   ```bash
   ssh -X -L 5000:localhost:5000 사용자명@서버IP
   ```

3. **수동으로 URL 복사**
   - 브라우저 주소창에 직접 입력: `http://localhost:5000`

## 보안 이점

1. **외부 직접 접속 불가**: 서버가 localhost만 리스닝
2. **SSH 인증 필요**: SSH 키 또는 비밀번호 필요
3. **네트워크 보안**: SSH 터널을 통한 암호화된 연결
4. **방화벽 설정 불필요**: 추가 포트 개방 불필요

## 자동화 스크립트 예시

### 원격 접속용 스크립트 (로컬 컴퓨터에 저장)
```bash
#!/bin/bash
# remote_monitor.sh

SERVER_IP="192.168.0.222"
SERVER_USER="hashjamm"
LOCAL_PORT=5000
REMOTE_PORT=5000

echo "SSH 터널 생성 중..."
ssh -f -N -L ${LOCAL_PORT}:localhost:${REMOTE_PORT} ${SERVER_USER}@${SERVER_IP}

if [ $? -eq 0 ]; then
    echo "✅ 터널 생성 완료"
    echo "브라우저에서 http://localhost:${LOCAL_PORT} 접속"
    
    # 브라우저 자동 열기 (선택사항)
    if command -v xdg-open &> /dev/null; then
        xdg-open "http://localhost:${LOCAL_PORT}"
    fi
else
    echo "❌ 터널 생성 실패"
    exit 1
fi
```

