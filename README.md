# 질병 네트워크 (CoTDeX) 프로젝트

* 한 줄 소개: 100만 명 코호트 데이터를 활용, 시간의 흐름에 따른 질병 관계 변화를 분석하고 탐색하는 동적 네트워크 시각화 플랫폼을 구축한 End-to-End 데이터 과학 프로젝트
* 진행 및 참여기간: 2025/07/18 → 2025/07/18
* 담당 부분: End-to-End 데이터 과학, 네트워크 분석, 대규모 데이터 처리, 시각화 플랫폼 개발
* 분야: 네트워크 분석, 데이터 시각화, 웹 개발, 의료 빅데이터
* 성과 요약: CoTDeX 플랫폼 개발, 동적 질병 네트워크 DB 구축, 학회 구연 발표
* 적용 skill 및 tools: Cytoscape.js, Django, Maria DB, SAS, SQL, multiprocessing, networkx, python

## 📖 내용 요약

1. **프로젝트 진행 흐름**

![image.png](image.png)

![image.png](image%201.png)

1. **네트워크 분석 시각화**

![image.png](image%202.png)

![image.png](image%203.png)

![image.png](image%204.png)

![image.png](image%205.png)

1. **웹 플랫폼 프로토타입**

![image.png](image%206.png)

---

## 🧶 프로젝트 배경

기존 질병 네트워크 연구는 특정 시점의 관계만을 보여주는 정적(Static) 분석에 머무르는 한계가 있었습니다. 이에 **100만 명 규모의 국민건강보험공단 코호트 데이터**를 활용하여, 10년의 시간 흐름에 따른 질병 간 상호 관계의 **동적(Dynamic) 변화를 추적하고 시각적으로 탐색**할 수 있는 플랫폼 'CoTDeX'를 구축하는 본 연구 개발 프로젝트를 수행했습니다.

---

## 🔍 문제 정의

- **대규모 데이터 처리의 복잡성**
    
    100만 명의 10년치 상병 기록(Raw Data)을 처리하고, 선별한 1,187개 질병 각각에 대해 통계적 신뢰도를 확보하기 위한 **1:5 성별/연령 매칭 코호트를 생성**하는 과정은 상당한 컴퓨팅 자원과 정교한 데이터 엔지니어링을 요구했습니다.
    
- **천문학적 계산량과 성능 문제**
    
    약 140만 개에 달하는 질병 쌍에 대해 10개년 각각의 상대위험도(RR)를 계산하여 최종적으로 **4,800만 행(row)에 달하는 통계 데이터를 생성**하는 작업은, 단일 프로세스로는 수개월이 소요될 수 있는 대규모 연산 문제였습니다.
    
- **분석 결과의 해석 및 활용의 어려움**
    
    수백만 개의 노드와 엣지로 구성된 Raw Network 데이터는 그 자체로는 의미를 파악하기 어렵습니다. 연구자들이 직관적으로 인사이트를 발견하고 가설을 검증할 수 있도록 돕는 **효과적인 시각화 및 탐색 도구가** 필요했습니다.
    

---

## 🔧 문제 해결 방식

- **SAS/SQL 기반 대규모 데이터 전처리 파이프라인 구축**
    
     `SAS`와 `SQL`을 활용하여 100만 명 코호트의 원시 데이터를 정제하고, 각 질병에 대한 1:5 매칭 코호트를 생성하는 등 통계 분석에 적합한 형태의 데이터셋을 구축하는 **초기 ETL 파이프라인을 설계**했습니다.
    
- **병렬 처리를 통한 연산 성능 최적화**
    
    주어진 단일 서버 환경의 한계 내에서 수개월이 소요될 수 있는 140만 쌍의 상대위험도 계산 문제를 해결하기 위해, `Python`의 `multiprocessing` 라이브러리를 적용했습니다. **연산 작업을 병렬화하여 자원 활용을 극대화**함으로써 전체 분석 시간을 획기적으로 단축했습니다.
    
- **네트워크 과학 기반의 구조적 특성 분석**
    
    `networkx` 라이브러리를 활용하여 연도별 질병 네트워크를 생성하고, 연결성, 밀도, 클러스터링 계수 등 **다양한 정량적 지표를 추출하여 시간의 흐름에 따른 네트워크의 구조적 진화 과정을 분석**했습니다.
    
- **MariaDB 구축 및 Django 기반 웹 시각화 프로토타입 개발**
    
    분석된 네트워크 데이터를 `MariaDB`에 이관하여 데이터베이스를 구축하고, `Django`를 통해 API를 개발했습니다. 최종적으로 사용자가 직접 질병 네트워크를 탐색할 수 있는 **웹 기반 시각화 프로토타입 'CoTDeX'를 구현**하여 학회에서 시연했습니다.
    

---

## 📄 결과

- **4,800만 건 규모의 동적 질병 네트워크 데이터 자산 구축**
    
    100만 명 코호트 기반, 1,187개 질병의 10년간 상호 관계 변화를 상대위험도(RR)로 정량화한 **총 4,800만 행 규모의 동적 질병 네트워크 데이터베이스를 성공적으로 구축**했습니다.
    
- **네트워크 분석 기반의 새로운 의학적 인사이트 도출**
    
    네트워크 구조 분석을 통해, 시간이 지남에 따라 질병 네트워크가 특정 질병 중심의 구조에서 점차 분산형으로 진화하는 패턴을 정량적으로 규명하는 등 **새로운 의학적, 역학적 인사이트를 도출**했습니다.
    
- **대한의료정보학회(KOSMI) 구연 발표 선정 및 기술력 입증**
    
    프로젝트의 전 과정과 분석 결과, 그리고 웹 시각화 프로토타입(CoTDeX)의 우수성을 인정받아 **2025년 춘계 대한의료정보학회에서 구연 발표 대상으로 선정**되어, 연구의 학술적 가치와 기술적 완성도를 공식적으로 입증했습니다.
    

---

## 🛠️ 적용 skills 및 tools

- **Data Processing & Engineering:** `Python`, `SAS`, `SQL`, `MariaDB`, `pandas`, `multiprocessing`
- **Data Science & Analysis:** `Network Analysis (networkx)`, `Statistics (Relative Risk)`, `R`
- **Web Development & Visualization:** `Django`, `Cytoscape.js`

---

## 💡 후기

- **데이터 과학의 전 과정을 아우르는 통합적 역량 성장**
    
    대규모 원시 데이터 정제부터 통계 분석, 네트워크 모델링, DB 구축, 그리고 최종 서비스 프로토타입 개발까지, **데이터가 아이디어에서 출발하여 실제 가치를 지닌 결과물로 완성되는 전 과정을 주도적으로 경험**하며 데이터 과학자로서의 통합적인 시야와 역량을 기를 수 있었습니다.
    
- **주어진 환경 내에서 문제를 해결하는 엔지니어링 역량**
    
    분산 처리 환경이 아닌 단일 서버의 제약 속에서 대규모 연산 문제를 해결하기 위해 병렬 처리 기법을 적용하며, **문제의 본질을 파악하고 주어진 환경에 맞는 최적의 기술을 선택하는 실용적인 문제 해결 능력**을 길렀습니다. 이 경험을 통해 어떤 환경에서도 데이터 기반의 문제를 해결할 수 있다는 자신감을 얻었습니다.
    

---

## 🔗 관련 링크

- https://www.kosmi.org/bbs/view_image.php?fn=%2Fdata%2Feditor%2F2507%2F95ae1abe352f9a059d1ff858bc9888cc_1751338873_4212.jpg : 자유연제 10

# 질병 네트워크 분석 및 시각화 시스템 개발 보고서


## 1. 연구 개요

본 보고서는 국민건강보험공단(NHIS)의 표본 코호트 데이터를 기반으로 하여 질병 간 시간적 연관성과 구조적 특성을 분석하고, 이를 시각화하는 웹 기반 질병 네트워크 시스템(CoTDeX)을 구축한 전 과정을 정리한 것입니다.

본 연구는 2003년 기준으로 전체 질병에 대해 1:5 Propensity Score Matching(PSM)을 수행하여 개별 코호트를 구성하고, 각 질병쌍 간 상대위험도(Relative Risk; RR)를 기반으로 연도별 질병 네트워크를 구축하는 것을 목표로 합니다. 이후, 각 네트워크에 대해 구조적 특성 지표를 분석하고 웹 시각화 시스템에 탑재하였습니다.


## 2. 전처리 및 코호트 구성 (SAS)

- 2003년 기준 NHIS 자격 보유 인구를 초기 분석 대상으로 설정
- 2002~2013년의 상병 기록(KCD-6 기준)을 통합하여 질병 진단 이력 구축
- 2002년 진단 이력을 워시아웃(washout)하여 2003년 최초 발생 질병만 유지
- 10건 이상 발생한 1,187개 질병만 선별
- 각 질병에 대해 age_group, sex 기준으로 1:5 exact matching 수행 → matched cohort 생성
- 각 cohort에 대해 follow-up 기간(1~10년) 내 질병 발생 정보 구성


## 3. Contingency Table 생성 (`ctable_generator_20250612.ipynb`)

- 각 질병코드에 대해 matched cohort 데이터를 기반으로
- follow-up 기간 내 outcome 질병 발생 여부를 기준으로 2×2 contingency table 구성
- case/control 그룹 내 outcome 발생 여부 집계 (a, b, c, d 값)


## 4. 상대위험도 및 통계량 계산 (`rr_calculator_20250611.ipynb`)

- 생성된 contingency table을 기반으로 RR, log(RR), 95% CI, p-value, Fisher's p-value 계산
- R `epitools` 패키지를 연동하여 RR 계산 수행
- 계산된 결과를 하나의 RR 테이블(csv)로 저장


## 5. 네트워크 구성 정보 생성 (`node_edge_info_generator_20250617.ipynb`)

- RR 결과를 기반으로 edge 목록 구성 (cause → outcome, log(RR) 포함)
- 노드 정보에는 질병명, 발생 수, 코드 등 포함
- 최종적으로 node/edge csv 파일 생성 → 웹 시각화 입력 데이터


## 6. 부가 정보 병합 (`info_merging_20250617.ipynb`)

- 노드에 성별, 연령대, 지역별 분포 정보 통합
- 엣지에는 case/control 수, 질병명, RR 등 추가 속성 병합
- 시각화 및 필터링 기능을 위한 확장된 attribute 테이블 생성


## 7. 데이터베이스 마이그레이션 (`latte_migration.ipynb`)

- 생성된 node/edge DataFrame을 MariaDB에 업로드
- 테이블명: `cotdex_nodes`, `cotdex_edges`
- Django 백엔드에서 쿼리로 접근 가능하도록 설계


## 8. 네트워크 구조 특성 분석 (`network_feature_extraction_20250620.ipynb`)

- 연도별 네트워크 데이터를 기반으로 다음 지표 계산:
    - 평균 degree/strength
    - network density
    - power-law 적합성
    - modularity 및 connected component 분석
- 연도별 지표 변화를 통해 네트워크 진화 분석


## 9. 결론 및 활용

본 프로젝트는 정적 질병 네트워크의 한계를 극복하고, 시간의 흐름에 따른 질병 간 연관 구조 변화를 정량적으로 분석하고 시각화할 수 있는 시스템을 구축하였다. 주요 결과는 다음과 같습니다다:

- PSM 기반 개별 질병 코호트 구성 및 1~10년 추적 질병 데이터 구축
- 연도별 RR 기반 directed weighted disease network 생성
- 질병 간 구조적 연결성, 중심 질병군, 클러스터 분석 수행
- 사용자 조건 기반 탐색 가능한 웹 시각화 도구 구현


## 10. HR(Hazard Ratio) 분석 시스템 개발 (2025-12-15)

### 10.1 기초 인프라 구축

- **유틸리티 모듈 업데이트** (`disease_network_funs.py`)
  - RR 단계 작업을 위한 함수 추가 (DBver_melting_edge_attr 등)
  - RR mapping 관련 데이터 처리 함수
  - 여러 노트북에서 공통으로 사용하는 헬퍼 함수들 확장

- **데이터베이스 마이그레이션**
  - `DBver_info_merging_migration.ipynb`: 건기식 서비스 제공을 위한 별도 DB 마이그레이션, Primary Key 작업 및 최적화
  - `latte_migration.ipynb`: Latte 시스템 데이터 변환 및 마이그레이션

- **외부 협업 노트북**
  - `datacook_20251021.ipynb`, `supple_grouping_result_20250825.ipynb`: 데이터쿡 협업 - 건기식 전처리 및 supplementary_disease_network 생성
  - `datachang_data_summary.ipynb`: 데이터창 협업 - 데이터 분석 및 전달

### 10.2 HR Calculator 단독 로직 개발 (v1-v10)

- **초기 버전 (v1-v3)**
  - 기본 HR 분석 로직 구현 (Cox 회귀, 경쟁위험 분석)
  - 데이터 전처리 개선 (날짜 변환, NA 처리, 데이터 검증)
  - 에러 핸들링 및 로깅 개선

- **성능 최적화 (v4)**
  - OS 디스크 캐시 예열 방식 도입
  - 메모리 제로 전략 (Push-down 병렬 처리)
  - 안정적인 multisession 방식 사용

- **fu=10 작업 시도 및 문제 해결 (v5-v10)**
  - **v5**: fu=10 작업 첫 시도 - RAM 병목 문제 발생, aggregate 실패로 청크 파일 소실
  - **v6-v7**: RAM 병목 문제 해결 시도
    - v6: COW + RAM 조인 방식 시도
    - v7: DuckDB I/O로 전환하여 RAM 병목 해결
  - **v8-v9**: 트랜잭션 통합 및 3-Tier Logging 추가
    - 트랜잭션 통합으로 부분 작업 실패 시 전체 롤백 가능
    - 성공/시스템 실패/통계 실패 로그 3단계 분리
  - **v10**: 최종 안정화 버전 - v5의 결과물을 기반으로 복구 작업 수행

### 10.3 HR 자동화 시스템 개발

- **HR Calculator Engine (v1-v5)**
  - **v1**: 자동화 엔진 기본 구현 (배치 처리, 병렬 처리, DuckDB 기반 저장, 청크 파일 시스템)
  - **v2**: 메모리 사용량 최적화 (DuckDB COPY 방식 시도, aggregate 실패 시 데이터 소실 위험 발견)
  - **v3**: 데이터 소실 방지 개선 (aggregate 로직 별도 분리, 청크 파일 삭제 로직 제거)
  - **v4**: stat_excluded 기능 추가 (fu=9 작업에 사용, case=1 & status=1이 없는 경우 처리)
  - **v5**: 매핑 데이터 제외 버전 (통계 결과 데이터 수집에만 집중, fu=7 작업부터 사용)

- **실행 관리 시스템**
  - `hr_analysis_manager.sh`: 범용 실행 관리 스크립트
    - R 스크립트 이름과 fu 인자만 변경하여 사용
    - 자동 재시작 기능, 진행 상황 모니터링 (프로그래스 바)
    - 메모리 제한 설정 (100G), 시스템 리소스 모니터링

- **결과물 취합 및 검증**
  - `aggregate_results.R`: 청크 파일 취합 Standalone 스크립트 (메모리 문제로 메인 프로세스에서 분리)
  - `compare_old_new_files.R`: 데이터 생성 방식 검증 스크립트

### 10.4 모니터링 시스템 개발

- **실시간 모니터링 대시보드** (`tmux_monitor_advanced.py`)
  - Flask 기반 웹 모니터링 대시보드
  - 실시간 로그 스트리밍, 진행 상황 모니터링, 에러 통계 및 분석
  - DuckDB 기반 작업 완료/실패 추적

- **동적 프로젝트 전환 기능**
  - 서버 재시작 없이 프로젝트 전환 가능
  - 전역 프로젝트 설정 관리

- **유틸리티 스크립트**
  - `open_monitor.sh`: 모니터링 대시보드 실행 스크립트
  - `setup_firewall.sh`: 방화벽 설정 스크립트 (포트 5000)

### 10.5 RR Mapping 검증 시스템 개발 (fu=8 완료 후, fu=7 이전)

- **디버깅 도구**
  - `hr_rr_mapping_validation_engine_debug.R`: 초기 디버깅 스크립트
  - `hr_rr_mapping_validation_engine_error_locator.R`: 에러 위치 파악 스크립트 (24개 인디케이터를 통한 단계별 추적)

- **RR Mapping Validation Engine (v1-v3)**
  - HR Engine에서 diff < 0인 경우 제거된 매핑 데이터 수집
  - negative_edge_pids 매핑 데이터 저장
  - 청크 파일 관리 개선, 로그 청크 파일 병합 후 삭제

- **실행 관리 시스템**
  - `hr_rr_mapping_validation_manager_v1.sh`, `v2.sh`: 자동 재시작 기능, 진행 상황 모니터링

- **데이터 탐색 노트북**
  - `rr_pids_keyseq_add_after_hr_engine.ipynb`: fu=8 작업 진행 전 RR mapping 산출 과정에서 사용

### 10.6 fu=10 데이터 복구 시스템 (aggregate 실패 후)

- **데이터 무결성 검증** (`v10_verify_to_build_recover_20251110.R`)
  - HR 결과, Stat Failed, System Failed, Completed Jobs 쌍 비교 분석
  - 네 집합 간 겹침 분석, 데이터 일관성 검증

- **파일시스템 레벨 복구 시도** (`recover_deleted_chunks.sh`)
  - extundelete를 사용한 삭제된 DuckDB 청크 파일 복구 시도
  - 파일시스템 레벨 복구가 불가능하여 v5 청크에서 복구 방식으로 전환

- **v5 청크에서 v10 데이터 복구** (`hr_fu10_recover.R`)
  - hr_calculator_v5.R의 결과물(청크 파일)에서 v10에 필요한 조합쌍만 추출
  - HR 분석 없이 매핑 데이터만 생성 (map_chunk_*.duckdb)

- **복구 검증** (`hr_fu10_recovered_mapping_check.R`)
  - 복구된 매핑 데이터의 무결성 검증
  - v10 HR 결과와 복구된 매핑 데이터의 조합쌍 일치 여부 확인

### 10.7 작업 순서 및 진행 상황

- **fu=10**: hr_calculator_v5.R (단독) → aggregate 실패 → 복구 → hr_calculator_v10.R
- **fu=9**: hr_analysis_manager.sh + engine_v4 (stat_excluded 추가)
- **fu=8**: 진행 전 RR mapping 산출 → hr_analysis_manager.sh + engine_v4
- **fu=7**: fu=8 완료 후 validation 진행 → hr_analysis_manager.sh + engine_v5 (mapping 제외)

### 10.8 주요 성과

- **대규모 HR 분석 자동화 시스템 구축**: 140만 개 질병 쌍에 대한 HR 분석을 자동화하여 처리 성능을 크게 향상
- **견고한 데이터 처리 시스템**: 이어하기(Resumability) 기능, 3-Tier Logging, 트랜잭션 통합을 통한 데이터 무결성 보장
- **실시간 모니터링 시스템**: 웹 기반 대시보드를 통한 작업 진행 상황 실시간 추적
- **데이터 복구 시스템**: aggregate 실패로 인한 데이터 소실 문제를 해결하고 복구 시스템 구축
- **RR Mapping 검증 시스템**: HR 분석 과정에서 제거된 매핑 데이터를 수집하여 데이터 완전성 확보