#!/bin/bash
# ============================================================================
# 로컬에서 모니터링 대시보드 열기
# 
# 사용 방법:
#   bash /home/hashjamm/codes/disease_network/open_monitor.sh
# 
# 기능:
#   - 서버 실행 상태 확인
#   - 브라우저 자동 실행 (xdg-open, firefox, chrome 등)
#   - http://localhost:5000 접속
# 
# 주의사항:
#   - 서버에 직접 로그인된 상태에서만 사용 가능
#   - 서버가 실행 중이어야 함
#   - DISPLAY 환경변수가 설정되어 있어야 함 (GUI 환경)
# ============================================================================

# 서버가 실행 중인지 확인
if ! pgrep -f "tmux_monitor_advanced.py" > /dev/null; then
    echo "⚠️  모니터링 서버가 실행 중이지 않습니다."
    echo "서버를 시작하려면:"
    echo "  cd /home/hashjamm/codes/disease_network"
    echo "  python3 tmux_monitor_advanced.py > /tmp/tmux_monitor/server.log 2>&1 &"
    exit 1
fi

# 서버가 준비될 때까지 대기
echo "서버 연결 확인 중..."
for i in {1..10}; do
    if curl -s http://localhost:5000/api/stats > /dev/null 2>&1; then
        echo "✅ 서버 연결 확인됨"
        break
    fi
    if [ $i -eq 10 ]; then
        echo "❌ 서버에 연결할 수 없습니다."
        exit 1
    fi
    sleep 1
done

# 브라우저 열기
URL="http://localhost:5000"

# Linux에서 사용 가능한 브라우저 찾기
if command -v xdg-open &> /dev/null; then
    echo "브라우저 열기: $URL"
    xdg-open "$URL" 2>/dev/null &
elif command -v gnome-open &> /dev/null; then
    gnome-open "$URL" 2>/dev/null &
elif command -v firefox &> /dev/null; then
    firefox "$URL" 2>/dev/null &
elif command -v google-chrome &> /dev/null; then
    google-chrome "$URL" 2>/dev/null &
elif command -v chromium-browser &> /dev/null; then
    chromium-browser "$URL" 2>/dev/null &
else
    echo "브라우저를 자동으로 열 수 없습니다."
    echo "수동으로 다음 URL을 브라우저에서 열어주세요:"
    echo "  $URL"
    exit 1
fi

echo "✅ 브라우저가 열렸습니다."
echo "URL: $URL"

