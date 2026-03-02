## Agent가 Star Schema를 이해 및 생성할때 참고하기 위한 자료

### 참고 문헌
1. Kimball Group — Dimensional Modeling Techniques (사실상 표준)
   - https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/
2. Wikipedia — Star Schema 기본 정의
   - https://en.wikipedia.org/wiki/Star_schema
3. Microsoft Power BI — Star Schema 실전 가이드
   - https://learn.microsoft.com/ko-kr/power-bi/guidance/star-schema
4. Dimensional Modeling 기본 개념 (한글)
   - https://statkclee.github.io/data-science/ds-dw-star.html

---

## 1. Kimball Dimensional Modeling 핵심 기법

### 1-1. Fact Table 유형

| 유형 | 설명 | 예시 |
|------|------|------|
| **Transaction Fact** | 개별 비즈니스 이벤트를 기록, 가장 세분화된 수준 | 주문 1건, 결제 1건 |
| **Periodic Snapshot Fact** | 일정 주기(일/월)로 누적 측정값을 기록 | 월말 재고, 일별 잔액 |
| **Accumulating Snapshot Fact** | 다단계 프로세스의 진행 상태를 추적 (마일스톤 날짜) | 주문→출하→배송→정산 |
| **Factless Fact** | 측정값 없이 Dimension Key만 기록 (발생 여부/관계) | 학생 출석, 프로모션 적용 |
| **Aggregated Fact** | 성능 향상을 위한 상위 레벨 집계 | 월별 매출 요약 |

### 1-2. Dimension Table 설계 기법

| 기법 | 설명 |
|------|------|
| **Surrogate Key** | 자연키(비즈니스키)와 별도로 정수형 대리키 사용. 이력 관리 및 쿼리 성능 향상 |
| **SCD Type 0** | 원본 값 유지, 변경하지 않음 |
| **SCD Type 1** | 최신 값으로 덮어쓰기 (이력 없음) |
| **SCD Type 2** | 새 행 추가로 이력 보존 (StartDate/EndDate/IsCurrent 컬럼) |
| **SCD Type 3** | 이전 값과 현재 값을 컬럼으로 병렬 저장 (제한적 이력) |
| **Conformed Dimension** | 여러 Fact 테이블에서 공통 사용하는 표준화된 Dimension |
| **Junk Dimension** | 소수 값의 플래그/코드 속성을 하나의 Dimension으로 통합 |
| **Degenerate Dimension** | 별도 Dimension 없이 Fact에 직접 포함하는 식별자 (주문번호 등) |
| **Role-Playing Dimension** | 동일 Dimension을 여러 역할로 사용 (주문일, 발송일, 배송일 → 모두 Date Dim) |
| **Date Dimension** | 스타 스키마에서 가장 기본이 되는 차원, 반드시 포함 |

### 1-3. 핵심 설계 원칙 (Kimball 4단계)

1. **비즈니스 프로세스 선택** — 무엇을 측정/분석할 것인가? (주문, 결제, 계약 등)
2. **Grain 선언** — Fact 테이블의 최소 행 단위 정의 (가장 중요한 결정)
3. **Dimension 식별** — "누가, 언제, 어디서, 무엇을, 어떻게"에 해당하는 맥락
4. **Fact(Measure) 식별** — 집계 가능한 수치 (금액, 수량, 횟수, 비율 등)

### 1-4. Bus Architecture
- 엔터프라이즈 차원에서 Conformed Dimension을 공유하여 여러 Fact를 연결
- 동일한 dim_customer, dim_date를 여러 비즈니스 프로세스에서 재사용

---

## 2. Star Schema 설계 관점 정리 (LLM/자동화용 가이드)

### 2-1. 비즈니스 프로세스 기준 수집
- 문서에서 무엇을 측정/분석할 것인가를 먼저 파악
- 예: 주문, 결재, 계약, KPI, 수주 등
- → 팩트 테이블의 "측정값(Measure)" 정의

### 2-2. 그레인(Grain) 정의
- 팩트 테이블의 최소 단위 (예: 주문 한 건, 거래 한 건, 고객 한 건)
- 문서에서 "어떤 레벨로 집계되는지" 키워드를 찾기
- → 모델 전체의 기준이 됨

### 2-3. Dimension 후보 추출 규칙

| 비즈니스 개념 | Dimension 후보 |
|---------------|----------------|
| 사람/사용자 | dim_customer, dim_employee, dim_user |
| 날짜/시간 | dim_date, dim_time |
| 제품/서비스 | dim_product, dim_service |
| 지역/부서 | dim_region, dim_department |
| 채널/소스 | dim_channel, dim_source |
| 상태/유형 | Junk Dimension으로 통합 가능 |

### 2-4. Key 식별 및 관계 설정
- Dimension PK와 Fact FK 관계를 파악
- 문서에서 ID/번호/코드 같은 식별자 → Dimension Key로 유추
- Surrogate Key (SK_xxx) 사용 권장

### 2-5. 측정값(Measure) 추출
- "수량", "금액", "실행횟수", "비율" 등 → Fact 측정값 후보
- 반드시 집계 가능해야 함 (SUM, AVG, COUNT, MIN, MAX)

### 2-6. 네이밍 룰

| 유형 | 네이밍 패턴 | 예시 |
|------|-------------|------|
| ODS 테이블 | ODS_xxx | ODS_ORDER, ODS_CUSTOMER |
| Fact 테이블 | FACT_xxx 또는 FCT_xxx | FACT_SALES, FACT_ORDER |
| Dimension 테이블 | DIM_xxx | DIM_DATE, DIM_CUSTOMER |
| DM 테이블 | DM_xxx | DM_SALES_DAILY, DM_CUSTOMER_SUMMARY |
| Surrogate Key | SK_xxx | SK_CUSTOMER_ID, SK_ORDER_ID |
| Natural Key | 원래 비즈니스키 유지 | CUSTOMER_CD, ORDER_NO |
| Measure 컬럼 | 의미 명확한 이름 | SALES_AMT, ORDER_QTY, TOTAL_UNITS |
| ETL 컬럼 | ETL_DT, BATCH_ID | 적재 일시, 배치 ID |

---

## 3. Star Schema 설계시 주의사항

1. **Fact 테이블은 최대한 단순하게** — 정규화하지 않고, 너무 많은 Dimension 붙이지 않기
2. **Dimension 비정규화** — Dimension은 평평하게(Flat) 유지, 눈송이(Snowflake) 피하기
3. **Surrogate Key 사용** — 비즈니스 키와 별도 관리, 이력 추적 지원
4. **Date Dimension 필수** — 날짜 관련 속성은 반드시 별도 Dimension으로
5. **측정값은 집계 가능해야 함** — SUM, AVG, COUNT 등이 의미 있어야 함
6. **비즈니스 용어 우선** — 기술 용어보다 실제 업무 용어 사용
7. **점진적 확장** — 처음부터 완벽한 모델보다 핵심부터 만들고 확장
8. **Grain 문서화 필수** — 각 Fact 테이블의 Grain을 명확히 정의하고 기록
9. **Conformed Dimension** — 동일 비즈니스 개념은 하나의 표준 Dimension으로 통합
10. **데이터 품질** — Null, 중복, 이상치 처리 규칙 수립

---

## 4. 스타 스키마 구성요소

### 4-1. 팩트 테이블(Fact Table)
- 비즈니스 프로세스에서 발생하는 측정 가능한 이벤트를 기록
- 차원 테이블의 FK + 숫자 측정값(Measure)으로 구성
- 일반적으로 행 수가 가장 많음
- Grain이 전체 모델의 기준

### 4-2. 차원 테이블(Dimension Table)
- 팩트의 맥락(누가, 언제, 어디서, 무엇을)을 설명
- 비즈니스 용어로 구성된 속성(Attribute)
- 비정규화하여 단일 테이블로 유지 (Snowflake 지양)
- Surrogate Key(PK) + Natural Key + 속성 컬럼 구조

### 4-3. 관계 설계
- Fact ↔ Dimension은 항상 N:1 관계
- FK는 Fact에, PK는 Dimension에 존재
- 다대다 관계가 필요하면 Bridge(Factless Fact) 테이블 사용

---

## 5. 3-Layer DW 아키텍처 (ODS → DW → DM)

### 5-1. ODS Layer (Operational Data Store)
- 원천 데이터를 최대한 원본 그대로 적재
- ETL 메타 컬럼 추가: ETL_DT (적재일시), BATCH_ID (배치 식별자)
- 네이밍: ODS_원본테이블명

### 5-2. DW Layer (Data Warehouse — Star Schema)
- Fact 테이블: SK_ 대리키 + Dimension FK + Measure 컬럼
- Dimension 테이블: SK_ 대리키 + Natural Key + 속성 컬럼
- SCD 적용: Type 1(덮어쓰기) 또는 Type 2(이력 보존) 선택

### 5-3. DM Layer (Data Mart)
- 특정 분석 목적에 맞게 집계/가공된 테이블
- 팩트+디멘전 조인 결과를 사전 계산
- 네이밍: DM_분석주제_집계단위 (예: DM_SALES_MONTHLY)
