#!/bin/bash
# 방화벽 설정 스크립트 (포트 5000 열기)

echo "방화벽 설정 확인 중..."

# ufw 사용 시
if command -v ufw &> /dev/null; then
    echo "ufw 방화벽 감지됨"
    echo "포트 5000을 열려면 다음 명령 실행:"
    echo "  sudo ufw allow 5000/tcp"
    echo "  sudo ufw reload"
fi

# firewalld 사용 시
if command -v firewall-cmd &> /dev/null; then
    echo "firewalld 감지됨"
    echo "포트 5000을 열려면 다음 명령 실행:"
    echo "  sudo firewall-cmd --permanent --add-port=5000/tcp"
    echo "  sudo firewall-cmd --reload"
fi

# iptables 직접 사용 시
if command -v iptables &> /dev/null; then
    echo "iptables 감지됨"
    echo "포트 5000을 열려면 다음 명령 실행:"
    echo "  sudo iptables -A INPUT -p tcp --dport 5000 -j ACCEPT"
    echo "  sudo iptables-save"
fi

echo ""
echo "현재 포트 5000 리스닝 상태:"
netstat -tlnp 2>/dev/null | grep :5000 || ss -tlnp 2>/dev/null | grep :5000 || echo "포트 5000이 리스닝 중이지 않습니다"

