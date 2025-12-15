# Git 커밋 전략 및 실행 계획

## 전략 개요

**접근 방식**: 논리적 그룹화 + 시간순 세부 정렬
- 큰 틀: 기능 개발 순서 (의존성 기반)
- 세부: 같은 그룹 내에서는 버전순 또는 시간순
- 원칙: 관련 파일들을 함께 커밋

**총 커밋 수**: 24개

**작업 순서 참고 (fu 역순):**
- fu=10: hr_calculator_v5.R (단독) → aggregate 실패 → 복구 → hr_calculator_v10.R
- fu=9: hr_analysis_manager.sh + engine_v4 (stat_excluded 추가)
- fu=8: 진행 전 RR mapping 산출 → hr_analysis_manager.sh + engine_v4
- fu=7: fu=8 완료 후 validation 진행 → hr_analysis_manager.sh + engine_v5 (mapping 제외)
**커밋 메시지 스타일**: 상세한 설명 (각 파일의 변경 사항 포함)

---

## Phase 1: 기초 인프라 (3개 커밋)

### 커밋 1: 유틸리티 모듈 업데이트 및 Git 설정
**파일들:**
- `disease_network_funs.py` (수정됨 - RR 단계에서 사용)
- `.gitignore` (__pycache__ 추가 필요)

**커밋 메시지:**
```
feat: 유틸리티 모듈 업데이트 및 Git 설정 개선

- disease_network_funs.py: RR 단계 작업을 위한 함수 추가
  * 기존 파일에 새로운 함수 추가 (DBver_melting_edge_attr 등)
  * RR mapping 관련 데이터 처리 함수
  * 여러 노트북에서 공통으로 사용하는 헬퍼 함수들 확장
  
- .gitignore: Python 캐시 파일 제외 설정
  * __pycache__/ 디렉토리 제외
  * .pyc 파일 제외
```

---

### 커밋 2: 데이터 마이그레이션 및 건기식 서비스 관련
**파일들:**
- `DBver_info_merging_migration.ipynb`
- `latte_migration.ipynb`

**커밋 메시지:**
```
feat: 건기식 서비스 제공을 위한 데이터베이스 마이그레이션

- DBver_info_merging_migration.ipynb: 건기식 서비스 DB 마이그레이션
  * 건강기능식품(건기식) 관련 서비스 제공을 위한 별도 DB 마이그레이션
  * DB의 Primary Key 작업 및 최적화
  * 여러 버전의 데이터베이스 정보를 통합하는 작업
  * 데이터 일관성 검증 및 병합 로직
  
- latte_migration.ipynb: Latte 관련 데이터 마이그레이션
  * Latte 시스템 데이터 변환 및 마이그레이션
```

---

### 커밋 3: RR 단계 결과물 기반 외부 협업 노트북
**파일들:**
- `datacook_20251021.ipynb` (데이터쿡 협업 - 건기식 전처리)
- `supple_grouping_result_20250825.ipynb` (데이터쿡 협업 - 건기식 전처리)
- `datachang_data_summary.ipynb` (데이터창 협업 - 분석)
- `node_edge_info_generator_20250617.ipynb`
- `ctable_generator_20250612.ipynb`
- `info_merging_20250617.ipynb` (수정됨)
- `rr_calculator_20250611.ipynb` (수정됨)
- `network_feature_extraction_20250620.ipynb` (수정됨)

**커밋 메시지:**
```
feat: RR 단계 결과물 기반 외부 협업 및 유틸리티 노트북

- datacook_20251021.ipynb: 데이터쿡 협업 - 건기식 전처리 작업
  * RR 단계 결과물에 대해 건강기능식품(건기식) 관련 작업 진행
  * supplementary_disease_network 생성을 위한 전처리
  * 데이터쿡 회사와의 협업 과정에서 사용
  
- supple_grouping_result_20250825.ipynb: 데이터쿡 협업 - 건기식 그룹핑 결과
  * RR 단계 결과물에 대해 건기식 관련 그룹핑 작업
  * supplementary_disease_network 생성을 위한 데이터 처리
  
- datachang_data_summary.ipynb: 데이터창 협업 - 데이터 분석
  * RR 단계 결과물에 대해 데이터창에서 요청한 분석 수행
  * 데이터 관련 내용 분석 및 전달용 노트북
  
- node_edge_info_generator_20250617.ipynb: 노드 및 엣지 정보 생성기
- ctable_generator_20250612.ipynb: 교차표 생성기
- info_merging_20250617.ipynb: 정보 병합 작업 (업데이트)
- rr_calculator_20250611.ipynb: RR 계산기 (업데이트)
- network_feature_extraction_20250620.ipynb: 네트워크 특징 추출 (업데이트)
```

---

## Phase 2: HR Calculator 단독 로직 (6개 커밋)

### 커밋 4: HR Calculator 초기 버전 (v1-v3)
**파일들:**
- `hr_calculator_v1.R`
- `hr_calculator_v2.R`
- `hr_calculator_v3.R`

**커밋 메시지:**
```
feat: HR Calculator 초기 버전 구현 (v1-v3)

- hr_calculator_v1.R: 기본 HR 분석 로직 구현
  * Cox 회귀 분석
  * 경쟁위험 분석 (Fine-Gray 모델)
  * 기본 결과 출력 및 저장
  
- hr_calculator_v2.R: 데이터 전처리 개선
  * 날짜 변환 로직 개선
  * NA 처리 강화
  * 데이터 검증 로직 추가
  
- hr_calculator_v3.R: 에러 핸들링 및 로깅 개선
  * tryCatch 블록 추가
  * 상세한 로깅 기능
  * 에러 복구 메커니즘
```

---

### 커밋 5: HR Calculator v4 (기본 최적화)
**파일들:**
- `hr_calculator_v4.R`

**커밋 메시지:**
```
feat: HR Calculator v4 (OS 디스크 캐시 예열 + 메모리 제로 전략)

- hr_calculator_v4.R: 성능 최적화 버전
  * OS 디스크 캐시 예열 방식 도입
  * 메모리 제로 전략 (Push-down 병렬 처리)
  * 메모리 사용량 개선
  * 처리 속도 향상
  * 안정적인 multisession 방식 사용
```

---

### 커밋 6: HR Calculator v5 (fu=10 첫 시도, RAM 병목 문제 발생)
**파일들:**
- `hr_calculator_v5.R`

**커밋 메시지:**
```
feat: HR Calculator v5 (fu=10 작업 첫 시도, RAM 병목 문제 발생)

배경:
- fu=10 작업 시 자동화 엔진 대신 단독 로직으로 시도
- v4.0 아키텍처 기반 (OS 디스크 캐시 예열 + 메모리 제로 전략)

- hr_calculator_v5.R: fu=10 작업에 사용된 단독 로직 버전
  * 배치 처리 및 병렬 처리 기능
  * 결과물 생성 후 aggregate 과정에서 로직 문제로 병합 실패 발생
  * 청크 파일 소실 문제 발생
  * RAM 병목 문제로 인한 성능 저하 (8일 ETA)
  * v5의 결과물은 별도로 보관하여 추후 복구 작업에 활용
```

---

### 커밋 7: HR Calculator v6-v7 (RAM 병목 문제 해결 시도)
**파일들:**
- `hr_calculator_v6.1.R`
- `hr_calculator_v6.2.R`
- `hr_calculator_v6.3.R`
- `hr_calculator_v7.1.R`
- `hr_calculator_v7.2.R`

**커밋 메시지:**
```
feat: HR Calculator v6-v7 (RAM 병목 문제 해결 시도)

배경:
- v5에서 발생한 RAM 병목 문제(8일 ETA) 해결을 위한 개선 시도
- v6: COW + RAM 조인 방식 시도
- v7: DuckDB I/O로 전환하여 RAM 병목 해결

- hr_calculator_v6.1.R, v6.2.R, v6.3.R: COW + RAM 조인 방식 시도
  * v6.1: v5.0 아키텍처 - COW + RAM 조인 + Job Queue 전략
  * v4.0의 I/O 병목을 제거하고 공유 메모리(COW) 활용 시도
  * v6.2: 버그 수정 및 안정화
  * v6.3: 최종 안정화
  
- hr_calculator_v7.1.R, v7.2.R: DuckDB I/O로 전환
  * v7.1: v8.0 아키텍처 - DuckDB I/O + Job Queue 전략
  * 워커별 로그 Queue 분리 (v6에서는 성공은 중앙, 실패는 분리)
  * 쓰기 일괄 처리(Write Batching) 추가
  * v4.0의 빠른 DuckDB I/O 속도(3일 ETA)와 v7.0.1의 견고성 결합
  * v7.2: 추가 개선 및 안정화
```

---

### 커밋 8: HR Calculator v8-v9 (트랜잭션 통합 및 3-Tier Logging)
**파일들:**
- `hr_calculator_v8.1.R`
- `hr_calculator_v8.2.R`
- `hr_calculator_v9.1.R`
- `hr_calculator_v9.2.R`
- `hr_calculator_v9.3.R`

**커밋 메시지:**
```
feat: HR Calculator v8-v9 (트랜잭션 통합 및 3-Tier Logging)

- hr_calculator_v8.1.R, v8.2.R: 트랜잭션 통합 추가
  * v8.1: v10.0 아키텍처 - 트랜잭션 통합(Transactional Integration) 추가
  * 모든 쓰기 작업을 트랜잭션으로 묶어서 부분 작업 실패 시 전체 롤백 가능
  * 디스크 쓰기 횟수 최소화로 파일 시스템 병목 감소
  * v8.2: 추가 개선 및 안정화
  
- hr_calculator_v9.1.R, v9.2.R, v9.3.R: 3-Tier Logging 추가
  * v9.1: v10.1 아키텍처 - 3-Tier Logging 도입
  * 성공 로그, 시스템 실패 로그, 통계 실패 로그 3단계 분리
  * 이어하기(Resumability) 기능 보장 강화
  * v9.2: 로깅 시스템 최적화
  * v9.3: 최종 안정화 및 성능 튜닝
```

---

### 커밋 9: HR Calculator v10 (최종 안정화, fu=10 복구 작업용)
**파일들:**
- `hr_calculator_v10.R`

**커밋 메시지:**
```
feat: HR Calculator v10 (최종 안정화, fu=10 복구 작업용)

배경:
- v5에서 fu=10 작업 시도 → aggregate 실패로 청크 파일 소실
- v6~v9를 거쳐 지속적으로 개선
- v10에서 최종 안정화 완료

- hr_calculator_v10.R: 최종 안정화 버전
  * v10.1 아키텍처 (3-Tier Logging)
  * v5/v7의 RAM 병목(8일 ETA) 문제 해결
  * v4.0의 빠른 DuckDB I/O 속도(3일 ETA)와 v7.0.1의 견고성 결합
  * 트랜잭션 통합 및 자동 체크포인트 루프 구현
  * aggregate 실패로 인한 청크 파일 소실 후 복구 작업에 사용
  * v5의 결과물을 기반으로 복구 가능
  * 완전한 문서화 및 주석 추가
```

---

## Phase 3: HR 자동화 시스템 (5개 커밋)

### 커밋 10: HR Calculator Engine 초기 버전 및 Manager (v1)
**파일들:**
- `hr_calculator_engine_v1.R`
- `hr_analysis_manager.sh`

**커밋 메시지:**
```
feat: HR 분석 자동화 엔진 초기 버전 및 실행 관리자 (v1)

- hr_calculator_engine_v1.R: 자동화 엔진 기본 구현
  * 배치 처리 기능
  * 병렬 처리 지원 (future 패키지)
  * DuckDB 기반 데이터 저장
  * 청크 파일 시스템 (hr_chunk_*.duckdb)
  * 진행 상황 추적 및 로깅
  * 기본 에러 핸들링
  * fu=10으로 하드코딩 (초기 버전)
  
- hr_analysis_manager.sh: 실행 관리 스크립트
  * R 스크립트 이름과 fu 인자만 변경하여 사용하는 범용 래퍼
  * engine_v1부터 존재하여 각 engine 버전과 함께 사용
  * 자동 재시작 기능
  * 진행 상황 모니터링 (프로그래스 바)
  * 메모리 제한 설정 (100G)
  * 로그 관리 및 기록
  * 시스템 리소스 모니터링
  * fu 파라미터를 환경 변수로 전달하여 R 스크립트에 전달
```

---

### 커밋 11: HR Calculator Engine 메모리 개선 (v2)
**파일들:**
- `hr_calculator_engine_v2.R`

**커밋 메시지:**
```
feat: HR Calculator Engine 메모리 개선 (v2)

- hr_calculator_engine_v2.R: 메모리 사용량 최적화
  * DuckDB COPY 방식 시도 (실패 후 롤백)
  * 메모리 효율적인 데이터 처리
  * 청크 파일 관리 개선
  * 에러 복구 메커니즘 강화
  * 주의: aggregate 실패 시 데이터 소실 위험 발견
```

---

### 커밋 12: HR Calculator Engine 안정화 및 stat_excluded 추가 (v3-v4)
**파일들:**
- `hr_calculator_engine_v3.R`
- `hr_calculator_engine_v4.R`

**커밋 메시지:**
```
feat: HR Calculator Engine 안정화 및 stat_excluded 기능 추가 (v3-v4)

- hr_calculator_engine_v3.R: 데이터 소실 방지 개선
  * aggregate 로직을 별도 스크립트로 분리
  * 청크 파일 삭제 로직 제거 (데이터 보존)
  * 메모리 부족 문제 해결 방안 구현
  * 안정성 향상
  
- hr_calculator_engine_v4.R: stat_excluded 기능 추가 및 개선
  * stat_excluded 케이스 처리 추가 (fu=9 작업에 사용)
  * case=1 & status=1이 없는 경우 HR 분석 및 매핑 생성 스킵
  * stat_excluded 로그에 기록하고 completed 로그에도 기록
  * 통계 실패 로그 관리 개선
  * 시스템 실패 로그 관리
  * 배치 처리 최적화
  * fu=9 작업에 사용 (hr_analysis_manager.sh + engine_v4 조합)
```

---

### 커밋 13: HR Calculator Engine 최종 버전 (v5, mapping 제외)
**파일들:**
- `hr_calculator_engine_v5.R`

**커밋 메시지:**
```
feat: HR Calculator Engine 최종 버전 (v5, mapping 제외)

- hr_calculator_engine_v5.R: 매핑 데이터 제외 버전
  * 모든 이전 버전의 개선사항 통합
  * 매핑 데이터는 별도 엔진으로 분리 (hr_rr_mapping_validation_engine)
  * 통계 결과 데이터 수집에만 집중
  * fu=7 작업부터 사용 (hr_analysis_manager.sh + engine_v5 조합)
  * RR mapping validation에서 확보한 pids 기반으로 매핑 데이터는 추후 산출
  * 완전한 문서화
  * FU 환경 변수로 fu 값을 받도록 변경 (manager.sh와 연동)
```

---

### 커밋 14: 결과물 취합 Standalone 스크립트
**파일들:**
- `aggregate_results.R`
- `compare_old_new_files.R`

**커밋 메시지:**
```
feat: 결과물 취합 및 검증 스크립트

- aggregate_results.R: 청크 파일 취합 Standalone 스크립트
  * hr_calculator_engine에서 생성된 청크 파일들을 취합
  * HR 결과 취합: hr_chunk_*.duckdb → total_hr_results_{fu}.parquet
  * Node 매핑 데이터 생성: matched_*.parquet → node_*_mapping_{fu}.parquet
  * Edge 매핑 데이터 취합: map_chunk_*.duckdb → edge_*_mapping_{fu}.parquet
  * 메인 프로세스와 독립적으로 실행 가능
  * 메모리 문제로 인해 메인 프로세스에서 분리됨
  
- compare_old_new_files.R: 데이터 생성 방식 검증 스크립트
  * DuckDB 직접 저장 방식과 메모리 기반 방식의 결과물 비교
  * 기존 파일과 새로 생성된 파일의 동일성 검증
  * edge_pids_mapping 파일 비교
```

---

## Phase 4: 모니터링 시스템 (3개 커밋)

### 커밋 15: 모니터링 시스템 기본 구현
**파일들:**
- `tmux_monitor_advanced.py`
- `open_monitor.sh`
- `setup_firewall.sh`

**커밋 메시지:**
```
feat: 실시간 모니터링 시스템 구현

- tmux_monitor_advanced.py: Flask 기반 웹 모니터링 대시보드
  * 실시간 로그 스트리밍 (tail -f)
  * 진행 상황 모니터링 (완료 작업 수, 진행률)
  * 프로세스 상태 모니터링
  * 에러 통계 및 분석
  * DuckDB 기반 작업 완료/실패 추적
  * 웹 기반 대시보드 (HTML/CSS/JavaScript)
  
- open_monitor.sh: 모니터링 대시보드 실행 스크립트
  * 서버 실행 상태 확인
  * 브라우저 자동 실행 (http://localhost:5000)
  
- setup_firewall.sh: 방화벽 설정 스크립트
  * 포트 5000 열기 (ufw, firewalld, iptables 지원)
```

---

### 커밋 16: 모니터링 시스템 동적 프로젝트 지원
**파일들:**
- `monitor_dynamic_project.patch`
- `DYNAMIC_PROJECT_WORK_IN_PROGRESS.md`
- `DYNAMIC_PROJECT_FIX.md`

**커밋 메시지:**
```
feat: 모니터링 시스템 동적 프로젝트 전환 기능

- monitor_dynamic_project.patch: 동적 프로젝트 지원 패치
  * 서버 재시작 없이 프로젝트 전환 가능
  * 전역 프로젝트 설정 관리
  * 동적 설정 적용 원칙 구현
  
- DYNAMIC_PROJECT_WORK_IN_PROGRESS.md: 작업 진행 상황 문서
  * 아키텍처 변경사항 문서화
  * 완료된 작업 목록
  * 남은 작업 목록
  
- DYNAMIC_PROJECT_FIX.md: 수정 가이드 문서
  * 필수 수정 사항 가이드
  * 구현 세부사항
```

---

### 커밋 17: 모니터링 시스템 문서화
**파일들:**
- `TMUX_MONITOR_README.md`
- `ssh_tunnel_guide.md`

**커밋 메시지:**
```
docs: 모니터링 시스템 사용 가이드

- TMUX_MONITOR_README.md: 모니터링 시스템 사용 설명서
  * 설치 및 설정 방법
  * 사용 방법
  * 기능 설명
  
- ssh_tunnel_guide.md: SSH 터널링 가이드
  * 원격 접속을 위한 SSH 터널 설정 방법
  * 포트 포워딩 설정
```

---

## Phase 5: RR Mapping 산출 및 검증 (fu=8 완료 후, fu=7 이전) (4개 커밋)

**작업 시점**: fu=8 작업 완료 후, fu=7 작업 시작 전
**목적**: HR 쪽 mapping 데이터를 RR mapping validation 결과물로부터 추후 확보하기 위한 준비

### 커밋 18: RR Mapping Validation 초기 디버깅 도구
**파일들:**
- `hr_rr_mapping_validation_engine_debug.R`
- `hr_rr_mapping_validation_engine_error_locator.R`
- `hr_rr_mapping_validation.ipynb`

**커밋 메시지:**
```
feat: RR Mapping Validation 디버깅 도구

- hr_rr_mapping_validation_engine_debug.R: 초기 디버깅 스크립트
  * 에러 발생 원인 파악을 위한 상세 로깅
  * process_batch 함수 디버깅
  * 데이터 구조 문제 해결
  * 로그 파일: debug_log.txt (결과 탐색 후 삭제 함)
  
- hr_rr_mapping_validation_engine_error_locator.R: 에러 위치 파악 스크립트
  * 정확한 에러 발생 지점 파악
  * 24개 인디케이터를 통한 단계별 추적
  * 로그 파일에서 에러 발생한 질병 쌍 추출
  * 로그 파일: error_locator_log.txt (결과 탐색 후 삭제 함)
  
- hr_rr_mapping_validation.ipynb: 검증 작업 노트북
  * RR mapping 데이터 검증
  * HR 결과와의 비교 분석
```

---

### 커밋 19: RR Mapping Validation Engine v1-v2
**파일들:**
- `hr_rr_mapping_validation_engine_v1.R`
- `hr_rr_mapping_validation_engine_v2.R`

**커밋 메시지:**
```
feat: RR Mapping Validation Engine 초기 버전 (v1-v2)

- hr_rr_mapping_validation_engine_v1.R: 초기 구현
  * HR Engine에서 diff < 0인 경우 제거된 매핑 데이터 수집
  * hr_calculator_engine_v4.R과 동일한 방식으로 구현
  * negative_edge_pids 매핑 데이터 저장
  * 에러 핸들링 및 로깅
  * is.data.frame() 체크 추가로 NULL > 0 비교 에러 해결
  
- hr_rr_mapping_validation_engine_v2.R: 개선 버전
  * 성능 최적화
  * 메모리 사용량 개선
  * 에러 처리 강화
```

---

### 커밋 20: RR Mapping Validation Engine v3 및 Manager (fu=7 작업 전)
**파일들:**
- `hr_rr_mapping_validation_engine_v3.R`
- `hr_rr_mapping_validation_manager_v1.sh`
- `hr_rr_mapping_validation_manager_v2.sh`
- `HR_RR_MAPPING_VALIDATION_README.md`

**커밋 메시지:**
```
feat: RR Mapping Validation Engine 최종 버전 및 관리 시스템 (fu=7 작업 전)

배경:
- fu=8 작업 완료 후 진행
- HR 쪽 mapping 데이터를 RR mapping validation 결과물로부터 추후 확보하기 위한 준비
- validation에서 얻은 pids 기반으로 mapping 데이터를 추후 산출하기로 계획
- fu=7 작업부터는 engine_v5를 사용하여 mapping 데이터 제외하고 HR 결과만 수집

- hr_rr_mapping_validation_engine_v3.R: 최종 안정화 버전
  * 모든 이전 버전의 개선사항 통합
  * 청크 파일 관리 개선
  * 로그 청크 파일 병합 후 삭제
  * 결과물 청크 파일 보존
  * HR Engine에서 diff < 0인 경우 제거된 매핑 데이터 수집
  
- hr_rr_mapping_validation_manager_v1.sh: 실행 관리 스크립트 v1
  * 자동 재시작 기능
  * 진행 상황 모니터링
  * 로그 관리
  
- hr_rr_mapping_validation_manager_v2.sh: 실행 관리 스크립트 v2
  * v1의 개선사항 반영
  * 추가 기능 및 안정성 향상
  
- HR_RR_MAPPING_VALIDATION_README.md: 사용 설명서
  * 엔진 사용 방법
  * 설정 및 실행 가이드
  * 주요 기능 설명
```

---

### 커밋 21: RR Mapping 관련 노트북 (fu=8 작업 전 데이터 탐색)
**파일들:**
- `rr_pids_keyseq_add_after_hr_engine.ipynb`

**커밋 메시지:**
```
feat: RR Mapping 데이터 추가 노트북 (fu=8 작업 전 데이터 탐색)

- rr_pids_keyseq_add_after_hr_engine.ipynb: HR 엔진 이후 RR 매핑 데이터 추가
  * fu=8 작업 진행 전 RR mapping 산출 과정에서 사용
  * HR 분석 결과에 RR 매핑 정보 추가
  * person_id 및 key_seq 매핑 데이터 처리
  * 데이터 통합 및 검증
  * 데이터 탐색 과정 포함
```

---

## Phase 6: fu=10 데이터 복구 시스템 (fu=10 작업 이후) (2개 커밋)

### 커밋 22: fu=10 매핑 데이터 복구 시스템 (aggregate 실패 후)
**파일들:**
- `v10_verify_to_build_recover_20251110.R`
- `recover_deleted_chunks.sh`
- `hr_fu10_recover.R`
- `hr_fu10_recover_manager.sh`

**커밋 메시지:**
```
feat: fu=10 매핑 데이터 복구 시스템 (aggregate 실패 후)

배경:
- fu=10 작업 시 hr_calculator_v5.R 사용하여 작업 진행
- aggregate 과정에서 로직 문제로 병합 실패 발생
- 청크 파일들이 모두 소실됨
- 복구 작업을 위해 생성된 스크립트들

- v10_verify_to_build_recover_20251110.R: 데이터 무결성 검증 스크립트
  * HR 결과, Stat Failed, System Failed, Completed Jobs 쌍 비교 분석
  * 네 집합 간 겹침 분석
  * 데이터 일관성 검증
  * 복구 작업 전 상태 확인
  
- recover_deleted_chunks.sh: 파일시스템 레벨 복구 스크립트
  * extundelete를 사용한 삭제된 DuckDB 청크 파일 복구 시도
  * map_chunk_*.duckdb 파일 복구 시도
  * /home/hashjamm/results/disease_network/hr_mapping_results_v10 대상
  * 파일시스템 레벨 복구가 불가능하여 v5 청크에서 복구 방식으로 전환
  
- hr_fu10_recover.R: v5 청크에서 v10 데이터 복구
  * hr_calculator_v5.R의 결과물(청크 파일)에서 v10에 필요한 조합쌍만 추출
  * total_hr_results_10.parquet에 있는 조합쌍에 대해서만 매핑 데이터 복구
  * HR 분석 없이 매핑 데이터만 생성
  * map_chunk_*.duckdb만 생성
  
- hr_fu10_recover_manager.sh: 복구 작업 자동 관리 스크립트
  * hr_analysis_manager.sh와 유사한 구조
  * 자동 재시작 기능
  * 진행 상황 모니터링
  * 로그 관리
```

---

### 커밋 23: 복구 검증 및 유틸리티
**파일들:**
- `hr_fu10_recovered_mapping_check.R`

**커밋 메시지:**
```
feat: 복구된 매핑 데이터 검증 스크립트

- hr_fu10_recovered_mapping_check.R: 복구 데이터 검증
  * 복구된 매핑 데이터의 무결성 검증
  * v10 HR 결과와 복구된 매핑 데이터의 조합쌍 일치 여부 확인
  * 유니크한 조합 개수 비교
  * 데이터 일관성 확인
```

---

## Phase 7: 파일 정리 및 기타 (1개 커밋)

### 커밋 24: 파일 이름 변경 및 기타 정리
**파일들:**
- `RR_sas_code.txt` (새 파일)
- `sas_code.txt` (삭제됨 - 이미 커밋에 있음)
- `CURRENT_WORK_STATUS.txt` (선택사항)

**커밋 메시지:**
```
chore: 파일 이름 변경 및 기타 정리

- RR_sas_code.txt: sas_code.txt 파일 이름 변경
  * 파일 이름을 더 명확하게 변경
  * RR 관련 SAS 코드 포함
  
- sas_code.txt: 기존 파일 삭제 (이름 변경으로 인해)
  
- CURRENT_WORK_STATUS.txt: 현재 작업 상태 문서 (선택사항)
```

---

## 실행 순서

1. `.gitignore` 업데이트 (__pycache__ 추가)
2. Phase 1부터 Phase 7까지 순차적으로 커밋
3. 각 커밋 후 `git log` 확인
4. 모든 커밋 완료 후 `git push` 실행

## 주의사항

- `__pycache__/` 디렉토리는 커밋하지 않음 (.gitignore에 추가)
- 각 커밋은 독립적으로 의미가 있어야 함
- 커밋 메시지는 상세하게 작성
- 파일 수정 시간을 참고하여 논리적 순서 확인
- `disease_network_funs.py`는 기존에도 있었을 수 있으므로 수정사항만 확인

