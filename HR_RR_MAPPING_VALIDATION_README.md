# HR-RR Mapping Validation 사용 가이드

## 개요

이 도구는 HR Engine에서 제거된 `case == 1 & diff < 0` 케이스의 매핑 데이터를 수집하여 RR 엔진 결과와 비교 검증하기 위한 것입니다.

- **목적**: HR Engine의 `hr_calculator_engine_v4.R`에서 `clean_data`로부터 `diff < 0`인 경우를 제거하는데, 이렇게 제거된 케이스들에 대한 매핑 데이터를 수집하여 저장
- **검증 대상**: `case == 1 & diff < 0`인 케이스만 수집 (HR/RR 엔진의 매핑 데이터 조건과 일치)
- **아키텍처**: `hr_calculator_engine_v4.R`과 동일한 방식으로 구현 (병렬 처리, 배치 처리, 즉시 디스크 저장, 이어하기 지원)

## 파일 구조

### 소스 코드 파일
```
codes/disease_network/
├── hr_rr_mapping_validation_engine.R    # 메인 R 스크립트
│                                         # - 배치 처리 + 즉시 디스크 저장
│                                         # - 병렬 처리 및 이어하기 지원
│                                         # - hr_calculator_engine_v4.R과 동일한 방식
│
├── hr_rr_mapping_validation_manager.sh  # 자동 재시작 래퍼 스크립트
│                                         # - 자동 재시작 메커니즘
│                                         # - 메모리 제한 (100GB)
│                                         # - 진행 상황 모니터링
│
└── hr_rr_mapping_validation.ipynb       # Jupyter 노트북 (개발/테스트용)
                                          # - 함수 정의 및 테스트
                                          # - 소규모 데이터로 검증
```

### 결과 파일 구조
```
results/disease_network/hr_rr_mapping_validation_results/
│
├── validation_completed_jobs_fu<fu>.duckdb          # 완료 작업 추적 (중앙 DB)
│                                                      # - 모든 완료 작업 통합
│                                                      # - 재시작 시 이 DB를 읽어서 완료 작업 확인
│
├── validation_completed_jobs_fu<fu>/                 # 완료 작업 추적 (청크 파일)
│   └── completed_chunk_<PID>.duckdb                 # - 워커별 완료 로그
│                                                      # - 주기적으로 중앙 DB로 취합 후 삭제
│
├── validation_chunks_fu<fu>/                         # 결과 데이터 (청크 파일)
│   └── validation_chunk_<PID>.duckdb                 # - 워커별 결과 데이터
│                                                      # - 모든 작업 완료 시 Parquet으로 취합
│
└── all_diff_negative_info_fu<fu>.parquet            # 최종 취합 파일
                                                       # - 모든 작업 완료 시 생성
                                                       # - case == 1 & diff < 0 케이스만 포함
```

**참고**: `<fu>`는 추적 기간 값 (예: 8, 9, 10 등), `<PID>`는 워커 프로세스 ID입니다.

## 사용 방법

### 1. 사전 준비

#### sudoers 설정 (한 번만, 완전 자동화를 위해 필수)
```bash
echo "hashjamm ALL=(ALL) NOPASSWD: /usr/bin/systemd-run" | sudo tee /etc/sudoers.d/systemd-run
sudo chmod 0440 /etc/sudoers.d/systemd-run
```

#### 필수 패키지 확인
```bash
conda install -c conda-forge duckdb
```

### 2. 기본 실행 방법

#### 방법 1: 직접 실행 (단일 실행)
```bash
cd /home/hashjamm/codes/disease_network
export FU=8
Rscript hr_rr_mapping_validation_engine.R
```

#### 방법 2: Manager 스크립트 사용 (권장, 자동 재시작)
```bash
cd /home/hashjamm/codes/disease_network
./hr_rr_mapping_validation_manager.sh 8
```

### 3. 백그라운드 실행 (장시간 실행 시)

#### tmux 세션 사용 (권장)
```bash
# 세션 생성 및 실행
tmux new-session -d -s validation './hr_rr_mapping_validation_manager.sh 8'

# 진행 상황 확인 (다른 터미널에서)
tmux attach-session -t validation

# 세션에서 나가기 (프로세스는 계속 실행)
# Ctrl+B, D

# 세션 종료
tmux kill-session -t validation
```

#### nohup 사용 (프로그래스 바 없음)
```bash
nohup ./hr_rr_mapping_validation_manager.sh 8 > /dev/null 2>&1 &

# 로그 확인
tail -f /home/hashjamm/results/disease_network/hr_rr_mapping_validation_manager_fu8_history.log
```

## 주요 기능

### 1. 배치 처리 방식
- 전체 작업을 `batch_size` (기본값: 10,000) 단위로 분할
- 각 배치를 순차적으로 처리하여 메모리 사용량 제한
- 배치 내에서도 청크로 분할하여 병렬 처리 (부하 균형 향상)
- 청크 기반 동적 할당: 배치를 더 작은 청크로 분할하여 워커에 동적 할당

### 2. 즉시 디스크 저장
- 각 워커가 결과를 즉시 DuckDB 청크 파일로 저장
- 메모리에 모든 결과를 보관하지 않아 메모리 부하 방지
- 완료 작업도 DuckDB에 기록하여 재시작 시 중복 처리 방지
- 워커별 고유 청크 파일 사용 (PID 기반)

### 3. 이어하기 지원 (로그 취합 메커니즘)
- **사전 취합 (단계 0)**: 실행 시작 시 이전 실행의 청크 파일들을 중앙 DB로 취합
- **사이클 종료 시 취합 (단계 4)**: 배치 처리 완료 후 이번 사이클의 청크 파일들을 중앙 DB로 취합
- **중복 방지**: `ATTACH + INSERT INTO ... ON CONFLICT DO NOTHING` 사용
- **청크 파일 관리**: 취합 후 성공한 청크 파일 자동 삭제 (다음 사이클 준비)

### 4. 자동 재시작 메커니즘
- 메모리 부하나 시스템 오류로 실패 시 자동 재시작
- 완료된 작업은 DB에 저장되어 있어 중복 처리 없음
- 최대 재시작 횟수: 100회 (설정 가능)

### 5. 메모리 제한
- systemd-run으로 최대 메모리 100GB 제한
- 시스템 다운 방지

### 6. 진행 상황 추적
- 실시간 진행률 표시
- 리소스 사용량 모니터링 (CPU, 메모리, 스왑, Load average)
- ETA (예상 완료 시간) 계산

## 처리 단계

### 단계 0: 사전 로그 취합 (이어하기 준비)
- 이전 실행에서 생성된 완료 로그 청크 파일들을 중앙 DB로 취합
- 재시작 시 정확한 완료 작업 수를 파악하기 위함

### 단계 1: diff < 0 케이스 수집 (병렬 배치 처리)
- 전체 작업 목록 생성 (모든 cause-outcome 조합)
- 완료된 작업 확인 (중앙 DB만 읽음)
- 남은 작업 계산 및 배치 분할
- 병렬 배치 처리 (각 배치는 작은 청크로 분할하여 동적 할당)

### 단계 3: 결과물 청크 파일 취합 (모든 작업 완료 시)
- 모든 작업이 완료되면 결과물 청크 파일들을 하나의 Parquet 파일로 취합
- 최종 검증 데이터를 단일 파일로 제공

### 단계 4: 이번 사이클 로그 취합 (다음 사이클 준비)
- 이번 사이클에서 생성된 완료 로그 청크 파일들을 중앙 DB로 취합
- 다음 사이클 시작 시 정확한 완료 작업 수를 파악하기 위함

## 설정 변경

### R 스크립트 설정 변경 (`hr_rr_mapping_validation_engine.R`)

```r
# 병렬 처리 설정
n_cores <- 15  # 사용할 코어 수 (시스템에 맞게 조정)
batch_size <- 10000  # 배치 처리 크기 (메모리 사용량 조절)
chunks_per_core <- 3  # 코어당 청크 수 (부하 균형 향상)

# 데이터 파일 경로
matched_parquet_folder_path <- "/home/hashjamm/project_data/disease_network/matched_date_parquet/"
outcome_parquet_file_path <- "/home/hashjamm/project_data/disease_network/outcome_table.parquet"
```

### Manager 스크립트 설정 변경 (`hr_rr_mapping_validation_manager.sh`)

```bash
CONDA_ENV="disease_network_data"  # Conda 가상환경 이름
MAX_RESTARTS=100  # 최대 재시작 횟수
RESTART_DELAY=60  # 재시작 간 대기 시간 (초)
```

## 결과 파일

### 중간 파일 (청크)
- `validation_chunks_fu<fu>/validation_chunk_<PID>.duckdb`: 각 워커가 생성한 결과 청크 파일
  - 워커별 고유한 PID 기반 파일명 사용
  - `all_diff_negative_info` 테이블에 `case == 1 & diff < 0` 케이스 저장
  
- `validation_completed_jobs_fu<fu>/completed_chunk_<PID>.duckdb`: 각 워커가 기록한 완료 작업
  - 워커별 고유한 PID 기반 파일명 사용
  - `jobs` 테이블에 완료된 작업 (cause_abb, outcome_abb, fu) 저장
  - 주기적으로 중앙 DB로 취합 후 삭제됨

### 중앙 DB (완료 작업 추적)
- `validation_completed_jobs_fu<fu>.duckdb`: 모든 완료 작업을 통합한 중앙 DB
  - `jobs` 테이블: PRIMARY KEY (cause_abb, outcome_abb, fu)
  - 재시작 시 이 DB를 읽어서 완료된 작업 확인
  - 청크 파일 취합 후 자동 업데이트

### 최종 파일
- `all_diff_negative_info_fu<fu>.parquet`: 모든 청크 파일을 취합한 최종 결과
  - 모든 작업 완료 시에만 생성됨
  - 컬럼: `key`, `person_id`, `matched_id`, `case`, `status`, `diff`, `index_date`, `final_date`, `event_date`, `index_key_seq`, `key_seq`
  - `case == 1 & diff < 0`인 케이스만 포함 (HR/RR 엔진의 매핑 데이터 조건과 일치)

## 문제 해결

### 메모리 부하로 멈춤
- Manager 스크립트가 자동으로 재시작합니다
- 완료된 작업은 유지되므로 중복 처리 없음
- 메모리 제한이 100GB로 설정되어 있어 시스템 다운 방지

### 진행 상황 확인
```bash
# 로그 파일 확인
tail -f /home/hashjamm/results/disease_network/hr_rr_mapping_validation_manager_fu<fu>_history.log

# 완료 작업 수 확인 (중앙 DB)
duckdb /home/hashjamm/results/disease_network/hr_rr_mapping_validation_results/validation_completed_jobs_fu<fu>.duckdb \
  "SELECT COUNT(*) FROM jobs;"

# 완료 작업 상세 확인
duckdb /home/hashjamm/results/disease_network/hr_rr_mapping_validation_results/validation_completed_jobs_fu<fu>.duckdb \
  "SELECT * FROM jobs LIMIT 10;"

# 청크 파일 수 확인
ls -1 /home/hashjamm/results/disease_network/hr_rr_mapping_validation_results/validation_chunks_fu<fu>/ | wc -l
ls -1 /home/hashjamm/results/disease_network/hr_rr_mapping_validation_results/validation_completed_jobs_fu<fu>/ | wc -l
```

### 수동 중단
```bash
# 프로세스 찾기
ps aux | grep hr_rr_mapping_validation

# 종료 (SIGTERM으로 정상 종료 시도)
kill -TERM <PID>

# 강제 종료 (필요시)
kill -9 <PID>
```

## Jupyter 노트북 사용

`hr_rr_mapping_validation.ipynb`는 개발 및 테스트용입니다:
- 함수 정의 및 테스트
- 소규모 데이터로 검증
- 최종 실행은 R 스크립트 + Manager 사용 권장

## 참고사항

### 아키텍처 일관성
- 이 도구는 `hr_calculator_engine_v4.R`과 동일한 방식으로 구현되었습니다
- 병렬 처리: `future`/`furrr` 기반 `multisession`
- 배치 처리: 대규모 작업을 작은 배치로 분할
- 청크 기반 동적 할당: 배치를 더 작은 청크로 분할하여 워커에 동적 할당
- 즉시 디스크 저장: 메모리 부담 최소화를 위해 결과를 즉시 DuckDB에 저장
- 이어하기 지원: 완료 작업 추적을 통한 재시작 시 중복 작업 방지
- 로그 취합: 청크 파일의 완료 로그를 중앙 DB로 주기적 취합

### 데이터 필터링 조건
- **수집 조건**: `case == 1 & diff < 0`
  - `case == 1`: 원인 질병을 가진 케이스 (HR/RR 엔진의 매핑 데이터 조건)
  - `diff < 0`: `final_date < index_date` (논리적 불일치)
- HR Engine에서 제거되는 케이스만 수집하여 RR 엔진 결과와 비교 검증

### 메모리 관리
- 배치 처리 + 즉시 디스크 저장으로 메모리 부하를 최소화합니다
- 완료 작업 추적으로 재시작 시 중복 처리를 방지합니다
- 워커별 고유 청크 파일 사용으로 파일 충돌 방지

### 재시작 시 동작
- 실행 시작 시 사전 취합 (단계 0)을 통해 이전 실행의 청크 파일을 중앙 DB로 취합
- 중앙 DB에서 완료 작업을 확인하여 남은 작업만 처리
- 사이클 종료 시 이번 사이클의 청크 파일을 중앙 DB로 취합 (단계 4)
- 취합 후 청크 파일은 자동 삭제되어 다음 사이클 준비
