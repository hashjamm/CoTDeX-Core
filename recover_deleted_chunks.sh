#!/bin/bash
# 삭제된 DuckDB 청크 파일 복구 스크립트

# set -e 제거: 일부 명령어는 실패해도 계속 진행해야 함

RECOVERY_DIR="/home/hashjamm/results/disease_network/hr_mapping_results_v10_recovered"
TARGET_DIR="/home/hashjamm/results/disease_network/hr_mapping_results_v10"
DEVICE="/dev/sda2"
fu=10  # follow-up 기간

# extundelete는 파일시스템 루트 기준 상대 경로를 사용
# /dev/sda2가 /에 마운트되어 있으므로:
# 절대 경로: /home/hashjamm/results/disease_network/hr_mapping_results_v10
# 상대 경로: home/hashjamm/results/disease_network/hr_mapping_results_v10 (앞의 / 제거)
RELATIVE_PATH="home/hashjamm/results/disease_network/hr_mapping_results_v10"

echo "=== 삭제된 파일 복구 시도 ==="
echo ""

# 1. extundelete 설치 확인
echo "1. extundelete 설치 확인..."
if ! command -v extundelete &> /dev/null; then
    echo "   extundelete가 설치되어 있지 않습니다."
    echo "   설치 명령: sudo apt-get install -y extundelete"
    echo "   설치 후 다시 실행하세요."
    exit 1
fi
echo "   ✓ extundelete 설치됨"

# 2. 복구 디렉토리 생성
echo ""
echo "2. 복구 디렉토리 생성..."
mkdir -p "$RECOVERY_DIR"
echo "   복구 위치: $RECOVERY_DIR"

# 3. 삭제 시간 범위 확인
echo ""
echo "3. 삭제 시간 범위 확인..."

# 코드에서 청크 파일 삭제 시점 확인
# hr_calculator_engine.R의 918-921줄에서 청크 파일 삭제
# Parquet 파일 생성 후 즉시 삭제됨

# Parquet 파일의 최종 수정 시간 확인 (이 시간 이후에 청크 파일이 삭제되었을 가능성)
if [ -f "$TARGET_DIR/edge_pids_mapping_${fu}.parquet" ]; then
    PARQUET_TIME=$(stat -c %Y "$TARGET_DIR/edge_pids_mapping_${fu}.parquet" 2>/dev/null || echo "")
    if [ -n "$PARQUET_TIME" ]; then
        PARQUET_DATE=$(date -d "@$PARQUET_TIME" "+%Y-%m-%d %H:%M:%S" 2>/dev/null || echo "")
        echo "   Parquet 파일 최종 수정 시간: $PARQUET_DATE"
        echo "   (청크 파일은 이 시간 이후에 삭제되었을 가능성이 높습니다)"
        
        # 삭제 시간 범위 추정 (Parquet 파일 생성 후 ~ 현재)
        echo "   추정 삭제 시간 범위: $PARQUET_DATE ~ 현재"
    fi
else
    echo "   ⚠ Parquet 파일을 찾을 수 없습니다."
fi

# 현재 시간
CURRENT_DATE=$(date "+%Y-%m-%d %H:%M:%S")
echo "   현재 시간: $CURRENT_DATE"

# 디렉토리 최종 수정 시간 확인
DIR_MODIFY_TIME=$(stat -c %Y "$TARGET_DIR" 2>/dev/null || echo "")
if [ -n "$DIR_MODIFY_TIME" ]; then
    DIR_MODIFY_DATE=$(date -d "@$DIR_MODIFY_TIME" "+%Y-%m-%d %H:%M:%S" 2>/dev/null || echo "")
    echo "   디렉토리 최종 수정 시간: $DIR_MODIFY_DATE"
    echo "   (이 시간에 파일 삭제가 발생했을 가능성)"
fi

# 삭제된 파일 목록 확인
echo ""
echo "4. 삭제된 파일 확인 중..."
cd "$TARGET_DIR"
echo "   현재 디렉토리: $(pwd)"

# 5. 삭제된 파일 목록 확인 (extundelete로)
echo ""
echo "5. 삭제된 파일 목록 확인 중..."
echo "   (이 작업은 시간이 걸릴 수 있습니다)"

# 삭제된 파일 목록 확인 (시간 정보 포함)
sudo extundelete --ls --inode 2 "$DEVICE" 2>&1 | grep -i "map_chunk.*duckdb" > /tmp/deleted_files_list.txt || true

if [ -s /tmp/deleted_files_list.txt ]; then
    echo "   삭제된 map_chunk 파일 발견:"
    # 시간 정보가 포함된 경우 표시
    head -20 /tmp/deleted_files_list.txt
    echo ""
    echo "   삭제된 파일 수: $(wc -l < /tmp/deleted_files_list.txt)"
    echo "   (전체 목록: /tmp/deleted_files_list.txt)"
    
    # 삭제 시간 범위 추정 (extundelete 출력에서)
    echo ""
    echo "   삭제 시간 정보 (extundelete 출력에서 확인 가능):"
    echo "   (extundelete는 삭제 시간을 직접 표시하지 않지만, 파일 시스템 로그에서 확인 가능)"
else
    echo "   ⚠ 삭제된 파일 목록을 찾을 수 없습니다."
fi

# 6. extundelete로 복구 시도
echo ""
echo "6. extundelete로 복구 시도..."
echo "   방법: --restore-all로 모든 파일 복구 후 필터링"
echo "   (이 작업은 시간이 걸릴 수 있습니다)"
echo "   주의: sudo 권한이 필요하며, 비밀번호 입력이 필요할 수 있습니다."

# extundelete는 기본적으로 현재 디렉토리에 RECOVERED_FILES 폴더를 생성
# 복구 디렉토리로 이동하여 실행
cd "$RECOVERY_DIR"

# --restore-all로 모든 파일 복구 시도 (더 안전함)
# 이후 필요한 파일만 필터링
echo "   모든 삭제된 파일 복구 중..."
if sudo extundelete --restore-all "$DEVICE" 2>&1 | tee /tmp/extundelete_output.log; then
    echo "   ✓ extundelete 실행 완료"
else
    echo "   ⚠ extundelete 실행 중 에러 발생 (로그 확인: /tmp/extundelete_output.log)"
fi

# 원래 디렉토리로 복귀
cd - > /dev/null

# 7. 복구 결과 확인
echo ""
echo "7. 복구 결과 확인..."

# extundelete는 RECOVERED_FILES 디렉토리에 복구된 파일을 저장
RECOVERED_FILES_DIR="$RECOVERY_DIR/RECOVERED_FILES"
if [ -d "$RECOVERED_FILES_DIR" ]; then
    echo "   RECOVERED_FILES 디렉토리 발견: $RECOVERED_FILES_DIR"
    
    # 복구된 파일이 있는지 확인 (--restore-all은 전체 경로 구조로 저장)
    # home/hashjamm/results/disease_network/hr_mapping_results_v10 경로 찾기
    RECOVERED_TARGET_DIR=$(find "$RECOVERED_FILES_DIR" -type d -path "*/hr_mapping_results_v10" 2>/dev/null | head -1)
    
    if [ -z "$RECOVERED_TARGET_DIR" ]; then
        # 다른 경로에 복구되었을 수도 있음
        echo "   hr_mapping_results_v10 디렉토리를 찾는 중..."
        RECOVERED_TARGET_DIR=$(find "$RECOVERED_FILES_DIR" -type d -name "hr_mapping_results_v10" 2>/dev/null | head -1)
        if [ -z "$RECOVERED_TARGET_DIR" ]; then
            # RECOVERED_FILES 디렉토리 자체를 확인
            echo "   전체 RECOVERED_FILES 디렉토리에서 검색..."
            RECOVERED_TARGET_DIR="$RECOVERED_FILES_DIR"
        fi
    fi
    
    echo "   검색 대상 디렉토리: $RECOVERED_TARGET_DIR"
    
    # 모든 복구된 파일 확인
    all_recovered=$(find "$RECOVERED_TARGET_DIR" -type f 2>/dev/null | wc -l)
    echo "   전체 복구된 파일 수: $all_recovered"
    
    # map_chunk_*.duckdb 파일만 필터링
    recovered_files=$(find "$RECOVERED_TARGET_DIR" -name "map_chunk_*.duckdb" 2>/dev/null | wc -l)
    echo "   복구된 map_chunk_*.duckdb 파일 수: $recovered_files"
    
    if [ "$recovered_files" -gt 0 ]; then
        echo ""
        echo "   복구된 map_chunk 파일 목록 (처음 20개):"
        find "$RECOVERED_TARGET_DIR" -name "map_chunk_*.duckdb" 2>/dev/null | head -20 | while read file; do
            echo "     $(basename "$file") ($(du -h "$file" | cut -f1))"
        done
        
        echo ""
        echo "   복구된 파일 크기 합계:"
        find "$RECOVERED_TARGET_DIR" -name "map_chunk_*.duckdb" -exec du -ch {} + 2>/dev/null | tail -1
        
        echo ""
        echo "   복구 위치: $RECOVERED_TARGET_DIR"
        echo "   대상 위치: $TARGET_DIR"
        echo ""
        echo "   다음 명령어로 복구된 파일을 원래 위치로 복사할 수 있습니다:"
        echo "   cp $RECOVERED_TARGET_DIR/map_chunk_*.duckdb $TARGET_DIR/"
    else
        echo "   ⚠ map_chunk_*.duckdb 파일이 복구되지 않았습니다."
        echo ""
        echo "   복구된 다른 파일들:"
        find "$RECOVERED_TARGET_DIR" -type f 2>/dev/null | head -10
        echo "   로그 확인: /tmp/extundelete_output.log"
    fi
else
    echo "   ⚠ 복구 디렉토리를 찾을 수 없습니다."
    echo "   로그 확인: /tmp/extundelete_output.log"
fi

echo ""
echo "=== 복구 시도 완료 ==="
echo "복구된 파일 위치: $RECOVERY_DIR"

