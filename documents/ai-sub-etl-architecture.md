# AI-Driven Sub-ETL Platform — Architecture Design Document

> **Project Codename**: AETL (Agent-driven ETL)  
> **Version**: 1.0 Draft  
> **Date**: 2025-02-25  
> **Author**: JUHYEON + Claude

---

## 1. Executive Summary

엔터프라이즈 ETL 도구(Informatica, DataStage, Talend 등)의 **고비용·복잡성·벤더 종속** 문제를 해결하기 위해,
AI Agent가 핵심 ETL 지능(Transformation Logic, Metadata Analysis, Data Validation, Monitoring)을 수행하는
**경량 Sub-ETL 플랫폼**을 설계한다.

**핵심 철학**: 데이터 이동(Data Movement)은 기존 인프라(DB Link, CDC, Airflow 등)에 위임하고,
"**지능이 필요한 영역**"에 AI Agent를 집중 투입한다.

---

## 2. Problem Statement & Scope

### 2.1 기존 엔터프라이즈 ETL의 한계

| 영역 | 문제점 |
|------|--------|
| **비용** | 라이선스 비용이 연간 수천만~수억 원 |
| **유연성** | GUI 기반 매핑 변경에도 배포 사이클 필요 |
| **Transformation** | 복잡한 비즈니스 규칙을 GUI로 표현하기 어려움 |
| **메타데이터** | 리니지 추적이 제한적이거나 별도 도구 필요 |
| **검증** | Data Quality 룰 관리가 파편화 |
| **모니터링** | 장애 원인 분석이 수동적 |

### 2.2 AETL이 담당하는 영역

```
┌─────────────────────────────────────────────────────┐
│              전체 ETL 파이프라인                       │
│                                                      │
│  [Extract]  →  [Transform]  →  [Load]  →  [Validate] │
│     │              │             │            │       │
│  기존 인프라     ★ AETL       기존 인프라    ★ AETL   │
│  (CDC, DBLink)  AI Agent     (Bulk Load)   AI Agent  │
│                                                      │
│  + ★ Metadata Intelligence (AETL)                    │
│  + ★ Monitoring & Troubleshooting (AETL)             │
└─────────────────────────────────────────────────────┘
```

---

## 3. System Architecture

### 3.1 High-Level Architecture (Enhanced)

```
                    ┌──────────────┐
                    │   User UI    │
                    │ (Web + CLI)  │
                    └──────┬───────┘
                           │ REST / WebSocket
                    ┌──────▼───────────────┐
                    │  API Gateway Layer    │
                    │  (Auth, Rate Limit)   │
                    └──────┬───────────────┘
                           │
              ┌────────────▼────────────────┐
              │   AI Orchestration Layer     │
              │   ┌──────────────────────┐  │
              │   │  Agent Core Engine   │  │
              │   │  (Task Planner +     │  │
              │   │   Tool Router +      │  │
              │   │   Context Manager)   │  │
              │   └──────────┬───────────┘  │
              └──────────────┼──────────────┘
                             │
        ┌────────────┬───────┼───────┬──────────────┐
        │            │       │       │              │
   ┌────▼────┐ ┌────▼───┐ ┌─▼──┐ ┌──▼─────┐ ┌─────▼──────┐
   │Metadata │ │  SQL   │ │Data│ │Monitor │ │ Lineage    │
   │ Engine  │ │ Engine │ │Val.│ │ Engine │ │ Engine     │
   │         │ │        │ │Eng.│ │        │ │            │
   └────┬────┘ └────┬───┘ └─┬──┘ └──┬─────┘ └─────┬──────┘
        │           │       │       │              │
   ┌────▼────┐ ┌────▼───┐ ┌─▼──┐ ┌──▼─────┐ ┌─────▼──────┐
   │Schema   │ │SQL Gen │ │Rule│ │Alert & │ │ Graph DB   │
   │Analyzer │ │& Optim.│ │Exec│ │Root    │ │ (Neo4j/    │
   │(Profile)│ │(LLM+   │ │utor│ │Cause   │ │ NetworkX)  │
   │         │ │ Rules) │ │    │ │Analyzer│ │            │
   └────┬────┘ └────┬───┘ └─┬──┘ └──┬─────┘ └────────────┘
        │           │       │       │
        └───────────┴───┬───┴───────┘
                        │
              ┌─────────▼─────────┐
              │  Connection Pool  │
              │  (Multi-DB)       │
              │  Oracle│PG│MySQL  │
              │  MSSQL│BigQuery   │
              └───────────────────┘
```

### 3.2 Component Interaction Flow

```
User: "매출 테이블에서 월별 집계하는 변환 로직 만들어줘"
  │
  ▼
Agent Core → (1) Metadata Engine: 매출 테이블 스키마 조회
           → (2) Lineage Engine: 소스-타겟 관계 확인
           → (3) SQL Engine: 집계 SQL 생성 (LLM + Template)
           → (4) Validation Engine: 생성된 SQL 검증 규칙 자동 생성
           → (5) User에게 결과 + 실행 계획 반환
```

---

## 4. Core Engines — Detailed Design

### 4.1 Metadata Engine (메타데이터 엔진)

**목적**: 모든 엔진의 기반이 되는 "데이터에 대한 데이터"를 수집·분석·서빙

#### 4.1.1 기능 구성

```
Metadata Engine
├── Schema Crawler        # DB 카탈로그에서 스키마 자동 수집
│   ├── Table/Column 정보
│   ├── PK/FK/Index 정보
│   ├── Partition/Constraint 정보
│   └── View/Procedure 정의
│
├── Data Profiler          # 데이터 통계 프로파일링
│   ├── Column Statistics (min, max, avg, null%, distinct)
│   ├── Distribution Analysis (histogram)
│   ├── Pattern Detection (date format, phone, email)
│   └── Anomaly Detection (outlier 비율)
│
├── Schema Diff Detector   # 스키마 변경 감지
│   ├── Column Add/Drop/Type Change
│   ├── Constraint Changes
│   └── Change Impact Analysis
│
└── Business Glossary      # 비즈니스 용어 ↔ 물리 컬럼 매핑
    ├── Term → Column Mapping
    ├── Domain Classification
    └── Semantic Search (LLM)
```

#### 4.1.2 메타데이터 저장 모델

```sql
-- Core metadata tables
CREATE TABLE aetl_datasource (
    source_id       SERIAL PRIMARY KEY,
    source_name     VARCHAR(100) NOT NULL,
    db_type         VARCHAR(20),  -- oracle, postgresql, mysql, mssql, bigquery
    connection_info JSONB,        -- encrypted connection details
    created_at      TIMESTAMP DEFAULT NOW(),
    last_crawled_at TIMESTAMP
);

CREATE TABLE aetl_table_meta (
    table_id        SERIAL PRIMARY KEY,
    source_id       INT REFERENCES aetl_datasource(source_id),
    schema_name     VARCHAR(100),
    table_name      VARCHAR(200),
    table_type      VARCHAR(20),   -- TABLE, VIEW, MATERIALIZED_VIEW
    row_count       BIGINT,
    size_bytes       BIGINT,
    description     TEXT,          -- AI가 추론한 테이블 설명
    tags            TEXT[],        -- 자동 분류 태그
    profile_json    JSONB,         -- 프로파일링 결과
    crawled_at      TIMESTAMP
);

CREATE TABLE aetl_column_meta (
    column_id       SERIAL PRIMARY KEY,
    table_id        INT REFERENCES aetl_table_meta(table_id),
    column_name     VARCHAR(200),
    data_type       VARCHAR(100),
    is_nullable     BOOLEAN,
    is_pk           BOOLEAN,
    is_fk           BOOLEAN,
    fk_ref_table    VARCHAR(200),
    fk_ref_column   VARCHAR(200),
    ordinal_pos     INT,
    stats_json      JSONB,        -- {min, max, null_pct, distinct_count, top_values...}
    inferred_domain VARCHAR(50),  -- AI 추론: date, phone, email, amount, code...
    description     TEXT
);

CREATE TABLE aetl_schema_history (
    history_id      SERIAL PRIMARY KEY,
    table_id        INT REFERENCES aetl_table_meta(table_id),
    change_type     VARCHAR(20),  -- ADD_COLUMN, DROP_COLUMN, TYPE_CHANGE, etc.
    change_detail   JSONB,
    detected_at     TIMESTAMP DEFAULT NOW(),
    impact_analysis TEXT          -- AI가 분석한 영향도
);
```

#### 4.1.3 AI 활용 포인트

| 기능 | AI 역할 |
|------|---------|
| **테이블 설명 자동 생성** | 컬럼명+샘플데이터 → 비즈니스 설명 추론 |
| **도메인 자동 분류** | 컬럼 패턴 분석 → 개인정보/금액/코드/날짜 자동 태깅 |
| **비즈니스 용어 매핑** | 자연어 질문 → 물리 테이블·컬럼 식별 |
| **스키마 변경 영향 분석** | 변경된 컬럼을 사용하는 쿼리·ETL Job 자동 탐색 |

---

### 4.2 SQL / Transformation Engine (변환 엔진)

**목적**: 자연어 또는 규칙 기반으로 Transformation SQL을 생성·최적화·관리

#### 4.2.1 기능 구성

```
SQL / Transformation Engine
├── Transformation Logic Generator
│   ├── NL-to-SQL Translator    # 자연어 → SQL 변환
│   ├── Template Engine          # 재사용 가능한 변환 템플릿
│   ├── Rule-Based Generator     # 규칙 기반 매핑 자동 생성
│   └── Incremental Logic        # CDC/Delta 로직 자동 생성
│
├── SQL Optimizer
│   ├── Explain Plan Analyzer    # 실행 계획 분석
│   ├── Index Advisor            # 인덱스 추천
│   ├── Anti-Pattern Detector    # SQL 안티패턴 탐지
│   └── DB-Specific Rewriter     # DB별 SQL 방언 변환
│
├── Mapping Manager
│   ├── Source-Target Mapping     # 소스→타겟 컬럼 매핑 관리
│   ├── Transformation Catalog   # 변환 규칙 버전 관리
│   └── Dependency Tracker       # 변환 간 의존관계
│
└── Code Generator
    ├── Stored Procedure Gen     # SP 코드 생성
    ├── Python/Spark Code Gen    # PySpark/Pandas 코드 생성
    └── DBT Model Generator      # DBT SQL 모델 생성
```

#### 4.2.2 Transformation Logic Generation 상세

**핵심 개념**: LLM이 SQL을 "from scratch"로 생성하는 것이 아니라,
메타데이터 + 템플릿 + LLM을 결합한 **Hybrid Approach**

```
Step 1: Context Assembly (메타데이터 수집)
  ├── 소스 테이블 스키마 + 프로파일 조회
  ├── 타겟 테이블 스키마 조회 (있는 경우)
  └── 기존 유사 변환 로직 검색

Step 2: Template Selection (템플릿 선택)
  ├── 변환 유형 분류 (집계, 조인, 피벗, SCD, 증분 등)
  └── 해당 유형의 검증된 템플릿 로드

Step 3: LLM Generation (AI 생성)
  ├── Prompt = Context + Template + User Requirement
  ├── LLM이 템플릿을 채우거나 커스텀 SQL 생성
  └── DB-specific dialect 적용

Step 4: Validation (검증)
  ├── Syntax Check (DB Parser로 문법 검증)
  ├── Semantic Check (사용된 테이블/컬럼 존재 확인)
  ├── Dry Run (EXPLAIN으로 실행 가능성 확인)
  └── Test Execution (샘플 데이터로 결과 확인)
```

#### 4.2.3 변환 템플릿 예시

```yaml
# transformation_templates/scd_type2.yaml
name: SCD Type 2 (Slowly Changing Dimension)
category: dimension_management
description: |
  소스 테이블의 변경 이력을 타겟 Dimension에 Type 2 방식으로 반영
parameters:
  source_table: required
  target_table: required
  business_key: required        # 비즈니스 키 컬럼(들)
  tracked_columns: required     # 변경 추적 대상 컬럼(들)
  effective_date_col: optional  # 기본값: eff_start_dt / eff_end_dt

template: |
  -- SCD Type 2 MERGE (Oracle dialect)
  MERGE INTO {{target_table}} tgt
  USING (
    SELECT {{business_key}}, {{tracked_columns}}, 
           CURRENT_TIMESTAMP as eff_start_dt
    FROM {{source_table}}
  ) src
  ON (tgt.{{business_key}} = src.{{business_key}} AND tgt.current_flag = 'Y')
  WHEN MATCHED AND (
    {% for col in tracked_columns %}
    tgt.{{col}} != src.{{col}} {{ "OR" if not loop.last }}
    {% endfor %}
  ) THEN UPDATE SET 
    tgt.eff_end_dt = CURRENT_TIMESTAMP,
    tgt.current_flag = 'N'
  WHEN NOT MATCHED THEN INSERT (
    {{business_key}}, {{tracked_columns}}, eff_start_dt, eff_end_dt, current_flag
  ) VALUES (
    src.{{business_key}}, {% for col in tracked_columns %}src.{{col}},{% endfor %}
    CURRENT_TIMESTAMP, TO_DATE('9999-12-31','YYYY-MM-DD'), 'Y'
  );
  
  -- Insert new version for changed records
  INSERT INTO {{target_table}} (
    {{business_key}}, {{tracked_columns}}, eff_start_dt, eff_end_dt, current_flag
  )
  SELECT src.{{business_key}}, {% for col in tracked_columns %}src.{{col}},{% endfor %}
         CURRENT_TIMESTAMP, TO_DATE('9999-12-31','YYYY-MM-DD'), 'Y'
  FROM {{source_table}} src
  JOIN {{target_table}} tgt 
    ON src.{{business_key}} = tgt.{{business_key}}
  WHERE tgt.current_flag = 'N' 
    AND tgt.eff_end_dt = CURRENT_TIMESTAMP;

validation_rules:
  - check: row_count_delta
    description: "변경 전후 행 수 차이가 소스 변경분과 일치"
  - check: no_duplicate_current
    description: "business_key당 current_flag='Y'가 정확히 1건"
```

---

### 4.3 Data Validation Engine (데이터 검증 엔진)

**목적**: ETL 전·중·후 데이터 품질을 자동으로 검증하고 보고

#### 4.3.1 기능 구성

```
Data Validation Engine
├── Rule Manager
│   ├── Built-in Rules           # 사전 정의된 범용 규칙
│   ├── Custom Rule Builder      # 사용자 정의 규칙
│   ├── AI Rule Suggester        # 데이터 프로파일 기반 규칙 자동 제안
│   └── Rule Template Library    # 업종별 검증 규칙 라이브러리
│
├── Execution Engine
│   ├── Pre-load Validation      # 적재 전 소스 데이터 검증
│   ├── Post-load Validation     # 적재 후 소스-타겟 정합성 검증
│   ├── Cross-DB Validation      # 이기종 DB 간 데이터 비교
│   └── Incremental Validation   # 증분 데이터만 검증
│
├── Result Analyzer
│   ├── Pass/Fail Summary        # 검증 결과 요약
│   ├── Anomaly Highlighting     # 이상 데이터 하이라이팅
│   ├── Trend Analysis           # 검증 결과 시계열 추이
│   └── Root Cause Suggestion    # AI 기반 원인 추정
│
└── Reporting
    ├── Data Quality Scorecard   # 데이터 품질 점수 대시보드
    ├── DQ History Dashboard     # 품질 이력 추적
    └── Alert Integration        # Slack/Email 알림 연동
```

#### 4.3.2 검증 규칙 체계

```yaml
# 3-Tier Validation Framework

Tier 1 - Technical Validation (기술 검증):
  - null_check:       "컬럼 NULL 비율이 허용 범위 내인지"
  - type_check:       "데이터 타입 일관성"
  - uniqueness_check: "PK 중복 없음"
  - referential_check: "FK 정합성"
  - format_check:     "날짜/전화번호/이메일 포맷"
  - range_check:      "값의 범위 (0 < age < 150)"

Tier 2 - Reconciliation (정합성 검증):
  - row_count_match:    "소스-타겟 건수 일치"
  - sum_match:          "숫자 컬럼 합계 일치"
  - hash_match:         "행 단위 해시 비교"
  - sample_compare:     "랜덤 샘플 상세 비교"
  - aggregation_match:  "그룹별 집계 값 비교"

Tier 3 - Business Validation (비즈니스 검증):
  - business_rule:      "매출 = 단가 × 수량"
  - cross_field:        "시작일 < 종료일"
  - historical_drift:   "전일 대비 변동률 임계치"
  - distribution_shift: "데이터 분포 변화 감지"
  - completeness:       "필수 데이터 완전성"
```

#### 4.3.3 검증 규칙 저장 모델

```sql
CREATE TABLE aetl_validation_rule (
    rule_id         SERIAL PRIMARY KEY,
    rule_name       VARCHAR(200) NOT NULL,
    rule_type       VARCHAR(30),  -- null_check, row_count_match, business_rule, etc.
    tier            INT,          -- 1: technical, 2: reconciliation, 3: business
    target_table    VARCHAR(200),
    target_column   VARCHAR(200),
    rule_expression TEXT,          -- SQL expression or DSL
    severity        VARCHAR(10),   -- CRITICAL, WARNING, INFO
    threshold       NUMERIC,       -- 허용 임계치 (e.g., null_pct < 0.01)
    is_active       BOOLEAN DEFAULT TRUE,
    auto_generated  BOOLEAN DEFAULT FALSE,  -- AI가 자동 생성한 규칙인지
    created_by      VARCHAR(50),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE aetl_validation_result (
    result_id       SERIAL PRIMARY KEY,
    rule_id         INT REFERENCES aetl_validation_rule(rule_id),
    execution_id    VARCHAR(50),    -- ETL 배치 실행 ID
    run_timestamp   TIMESTAMP,
    status          VARCHAR(10),    -- PASS, FAIL, WARN, ERROR
    actual_value    TEXT,
    expected_value  TEXT,
    detail_json     JSONB,          -- 상세 검증 결과
    ai_analysis     TEXT            -- AI 원인 분석 코멘트
);
```

---

### 4.4 Monitoring & Troubleshooting Engine (모니터링 엔진)

**목적**: ETL 파이프라인의 상태를 실시간 감시하고, 장애 시 AI가 원인을 분석

#### 4.4.1 기능 구성

```
Monitoring & Troubleshooting Engine
├── Pipeline Monitor
│   ├── Job Status Tracker       # ETL Job 실행 상태 추적
│   ├── Performance Metrics      # 처리 시간/건수/처리량 수집
│   ├── Resource Monitor         # CPU/Memory/Disk/Network 모니터링
│   └── SLA Tracker              # SLA 준수율 추적
│
├── Alert Manager
│   ├── Threshold Alerts         # 임계치 기반 알림
│   ├── Anomaly Alerts           # 이상 탐지 기반 알림
│   ├── Escalation Rules         # 에스컬레이션 규칙
│   └── Multi-Channel Notify     # Slack, Email, SMS, Teams
│
├── AI Troubleshooter
│   ├── Error Pattern Matcher    # 에러 패턴 매칭 (과거 이력 기반)
│   ├── Root Cause Analyzer      # AI 기반 근본 원인 분석
│   ├── Fix Suggester            # 해결 방안 제안
│   └── Auto-Remediation         # 자동 복구 (재시도, 대안 경로)
│
└── Reporting
    ├── Operational Dashboard    # 운영 현황 대시보드
    ├── Historical Analytics     # 이력 분석
    └── Capacity Planning        # 용량 계획 리포트
```

#### 4.4.2 AI Troubleshooting Flow

```
ETL Job 실패 감지
    │
    ▼
[Step 1] 에러 컨텍스트 수집
    ├── 에러 메시지 + 스택 트레이스
    ├── 실패 시점 데이터 스냅샷
    ├── 관련 테이블 최근 변경 이력 (Schema History)
    └── 시스템 리소스 상태
    │
    ▼
[Step 2] AI 분석
    ├── 에러 패턴 매칭 (과거 유사 에러 검색)
    ├── 근본 원인 추론
    │   ├── 데이터 문제? (스키마변경, 데이터품질, 볼륨급증)
    │   ├── 시스템 문제? (리소스부족, 네트워크, 락 경합)
    │   └── 로직 문제? (SQL오류, 매핑오류, 타입불일치)
    └── Confidence Score 산출
    │
    ▼
[Step 3] 대응
    ├── [자동 복구] Confidence > 90%: 자동 재시도/대안 실행
    ├── [가이드 제공] 50% < Confidence < 90%: 해결 가이드 + 알림
    └── [에스컬레이션] Confidence < 50%: 담당자 에스컬레이션
```

---

### 4.5 Lineage Engine (리니지 엔진 — 추가 제안)

**목적**: 데이터의 흐름과 변환 과정을 End-to-End로 추적

#### 4.5.1 기능 구성

```
Lineage Engine
├── SQL Parser
│   ├── Query Dependency Extractor   # SQL에서 테이블/컬럼 의존관계 추출
│   ├── Transformation Mapper         # SELECT 표현식 → 변환 로직 매핑
│   └── Multi-Dialect Support         # Oracle, PG, MySQL, MSSQL 파서
│
├── Graph Builder
│   ├── Table-Level Lineage          # 테이블 단위 흐름도
│   ├── Column-Level Lineage         # 컬럼 단위 세밀한 추적
│   └── Job-Level Lineage            # ETL Job 단위 의존관계
│
├── Impact Analyzer
│   ├── Forward Impact               # "이 컬럼 변경 시 영향받는 하위 객체"
│   ├── Backward Trace               # "이 리포트 값의 원본 소스 추적"
│   └── Change Simulation            # "만약 이 테이블이 없어지면?"
│
└── Visualization
    ├── Interactive DAG              # 인터랙티브 방향성 그래프
    ├── ERD Auto-Generation          # ERD 자동 생성
    └── Export (Mermaid, DOT, JSON)  # 다양한 포맷 내보내기
```

#### 4.5.2 리니지 그래프 모델

```sql
-- Node: 테이블 또는 컬럼
CREATE TABLE aetl_lineage_node (
    node_id     SERIAL PRIMARY KEY,
    node_type   VARCHAR(20),  -- TABLE, COLUMN, JOB, REPORT
    source_id   INT,          -- aetl_datasource 참조
    object_name VARCHAR(300), -- schema.table.column
    metadata    JSONB
);

-- Edge: 데이터 흐름
CREATE TABLE aetl_lineage_edge (
    edge_id         SERIAL PRIMARY KEY,
    source_node_id  INT REFERENCES aetl_lineage_node(node_id),
    target_node_id  INT REFERENCES aetl_lineage_node(node_id),
    edge_type       VARCHAR(30),  -- DIRECT_COPY, TRANSFORM, AGGREGATE, JOIN, FILTER
    transformation  TEXT,          -- 변환 표현식 (e.g., "SUM(amount)")
    job_id          VARCHAR(100),  -- 어떤 ETL Job에 의한 것인지
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Index for graph traversal
CREATE INDEX idx_lineage_source ON aetl_lineage_edge(source_node_id);
CREATE INDEX idx_lineage_target ON aetl_lineage_edge(target_node_id);
```

---

## 5. AI Orchestration Layer — The Brain

### 5.1 Agent Core Engine 설계

```
┌─────────────────────────────────────────────────┐
│              Agent Core Engine                    │
│                                                   │
│  ┌───────────┐  ┌──────────────┐  ┌───────────┐ │
│  │   Task    │  │   Context    │  │   Tool    │ │
│  │  Planner  │──│   Manager    │──│  Router   │ │
│  └───────────┘  └──────────────┘  └───────────┘ │
│       │               │                │         │
│       │          ┌────▼─────┐          │         │
│       │          │ Prompt   │          │         │
│       └─────────▶│ Builder  │◀─────────┘         │
│                  └────┬─────┘                     │
│                       │                           │
│                  ┌────▼─────┐                     │
│                  │   LLM    │                     │
│                  │ Interface │                     │
│                  │(Claude/  │                     │
│                  │ GPT/Local)│                     │
│                  └──────────┘                     │
└─────────────────────────────────────────────────┘
```

### 5.2 Agent의 Tool 정의 (Function Calling)

```python
# Agent가 사용할 수 있는 도구 목록

AGENT_TOOLS = {
    # ── Metadata 도구 ──
    "get_table_schema": {
        "desc": "테이블의 컬럼, 타입, PK/FK 정보를 반환",
        "params": {"source_id": "int", "table_name": "str"}
    },
    "get_data_profile": {
        "desc": "컬럼별 통계(null%, distinct, min/max, distribution) 반환",
        "params": {"source_id": "int", "table_name": "str", "sample_size": "int"}
    },
    "search_tables": {
        "desc": "자연어로 테이블/컬럼을 검색",
        "params": {"query": "str", "source_id": "int (optional)"}
    },
    "detect_schema_changes": {
        "desc": "마지막 크롤링 이후 스키마 변경 사항 확인",
        "params": {"source_id": "int", "since": "datetime"}
    },

    # ── SQL 도구 ──
    "generate_sql": {
        "desc": "자연어 요구사항으로부터 SQL 생성",
        "params": {"requirement": "str", "source_db": "str", "target_db": "str"}
    },
    "validate_sql": {
        "desc": "SQL의 문법/의미 검증 (EXPLAIN 포함)",
        "params": {"sql": "str", "db_type": "str"}
    },
    "execute_sql": {
        "desc": "SQL을 실행하고 결과 반환 (SELECT only, 또는 DRY RUN)",
        "params": {"sql": "str", "source_id": "int", "limit": "int"}
    },
    "optimize_sql": {
        "desc": "SQL 실행계획 분석 및 최적화 제안",
        "params": {"sql": "str", "source_id": "int"}
    },

    # ── Validation 도구 ──
    "suggest_validation_rules": {
        "desc": "데이터 프로파일 기반으로 검증 규칙 자동 제안",
        "params": {"table_name": "str", "source_id": "int"}
    },
    "run_validation": {
        "desc": "검증 규칙 실행",
        "params": {"rule_ids": "list[int]", "execution_id": "str"}
    },
    "compare_datasets": {
        "desc": "소스-타겟 데이터셋 비교",
        "params": {"source_query": "str", "target_query": "str", "key_columns": "list"}
    },

    # ── Lineage 도구 ──
    "get_lineage": {
        "desc": "테이블/컬럼의 upstream/downstream 리니지 조회",
        "params": {"object_name": "str", "direction": "upstream|downstream", "depth": "int"}
    },
    "analyze_impact": {
        "desc": "스키마 변경의 영향도 분석",
        "params": {"table_name": "str", "change_type": "str"}
    },

    # ── Monitoring 도구 ──
    "get_job_status": {
        "desc": "ETL Job 실행 상태 조회",
        "params": {"job_id": "str", "time_range": "str"}
    },
    "analyze_error": {
        "desc": "에러 로그 분석 및 원인 추론",
        "params": {"error_log": "str", "context": "dict"}
    },
    "get_performance_metrics": {
        "desc": "파이프라인 성능 지표 조회",
        "params": {"job_id": "str", "metric_type": "str"}
    }
}
```

### 5.3 Prompt Engineering Strategy

```python
SYSTEM_PROMPT_TEMPLATE = """
You are AETL Agent, an AI-powered ETL assistant.

## Your Capabilities
You can analyze database metadata, generate transformation SQL,
validate data quality, track data lineage, and troubleshoot ETL issues.

## Available Context
- Connected databases: {connected_sources}
- Current user role: {user_role}

## Rules
1. ALWAYS verify table/column existence via metadata before generating SQL
2. NEVER execute DML (INSERT/UPDATE/DELETE) without explicit user confirmation
3. Generated SQL must be DB-dialect specific (current: {db_type})
4. Include validation rules for every transformation you generate
5. When troubleshooting, collect ALL context before suggesting a fix
6. Explain your reasoning at each step

## Response Format
For SQL generation:
- Show the SQL with comments
- Explain the transformation logic
- List auto-generated validation rules
- Suggest optimization opportunities if any
"""
```

---

## 6. Technology Stack Recommendation

### 6.1 Backend (권장)

| 레이어 | 기술 | 이유 |
|--------|------|------|
| **API Server** | FastAPI (Python) | async 지원, LLM 연동 용이, 타입 안전 |
| **Agent Framework** | LangChain / LangGraph | Tool calling, Chain 구성, Memory |
| **LLM** | Claude API (Anthropic) | 긴 컨텍스트, 정확한 SQL 생성, Tool Use |
| **Task Queue** | Celery + Redis | 비동기 ETL Job 실행 |
| **Metadata Store** | PostgreSQL | JSONB 지원, 확장성 |
| **Graph Store** | NetworkX (in-memory) → Neo4j (scale) | 리니지 그래프 탐색 |
| **Cache** | Redis | 메타데이터 캐시, 세션 관리 |
| **SQL Parser** | sqlglot (Python) | Multi-dialect SQL 파싱/변환 |

### 6.2 Frontend (권장)

| 레이어 | 기술 | 이유 |
|--------|------|------|
| **UI Framework** | React + TypeScript | 컴포넌트 기반, 생태계 |
| **State** | Zustand or TanStack Query | 서버 상태 관리 |
| **Visualization** | React Flow (리니지 DAG) | 인터랙티브 그래프 |
| **Charts** | Recharts | 모니터링 대시보드 |
| **Code Editor** | Monaco Editor | SQL 편집기 |

### 6.3 인프라

| 레이어 | 기술 | 이유 |
|--------|------|------|
| **Container** | Docker + Docker Compose | 로컬 개발 + 배포 |
| **CI/CD** | GitHub Actions | 자동화 파이프라인 |
| **DB Migration** | Alembic (SQLAlchemy) | 스키마 버전 관리 |
| **Secret** | HashiCorp Vault 또는 env | DB 접속정보 암호화 |

---

## 7. Project Structure (Monorepo)

```
aetl/
├── README.md
├── docker-compose.yml
├── .env.example
│
├── backend/
│   ├── pyproject.toml
│   ├── alembic/                    # DB Migration
│   │   └── versions/
│   │
│   ├── app/
│   │   ├── main.py                 # FastAPI entry
│   │   ├── config.py               # Settings (pydantic-settings)
│   │   │
│   │   ├── api/                    # API Routes
│   │   │   ├── v1/
│   │   │   │   ├── metadata.py     # 메타데이터 API
│   │   │   │   ├── transform.py    # 변환 로직 API
│   │   │   │   ├── validation.py   # 검증 API
│   │   │   │   ├── lineage.py      # 리니지 API
│   │   │   │   ├── monitor.py      # 모니터링 API
│   │   │   │   └── chat.py         # AI Chat API (WebSocket)
│   │   │   └── deps.py             # 의존성 주입
│   │   │
│   │   ├── core/                   # Core Business Logic
│   │   │   ├── agent/
│   │   │   │   ├── orchestrator.py # Agent Core Engine
│   │   │   │   ├── tools.py        # Tool definitions
│   │   │   │   ├── prompts.py      # Prompt templates
│   │   │   │   └── memory.py       # Conversation context
│   │   │   │
│   │   │   ├── metadata/
│   │   │   │   ├── crawler.py      # Schema Crawler
│   │   │   │   ├── profiler.py     # Data Profiler
│   │   │   │   ├── diff.py         # Schema Diff Detector
│   │   │   │   └── glossary.py     # Business Glossary
│   │   │   │
│   │   │   ├── transform/
│   │   │   │   ├── generator.py    # SQL Generator
│   │   │   │   ├── optimizer.py    # SQL Optimizer
│   │   │   │   ├── templates.py    # Template Engine
│   │   │   │   ├── mapper.py       # Source-Target Mapper
│   │   │   │   └── codegen.py      # Code Generator (SP, DBT)
│   │   │   │
│   │   │   ├── validation/
│   │   │   │   ├── rules.py        # Rule Manager
│   │   │   │   ├── executor.py     # Rule Executor
│   │   │   │   ├── analyzer.py     # Result Analyzer
│   │   │   │   └── suggestor.py    # AI Rule Suggestor
│   │   │   │
│   │   │   ├── lineage/
│   │   │   │   ├── parser.py       # SQL → Lineage Parser
│   │   │   │   ├── graph.py        # Graph Builder
│   │   │   │   └── impact.py       # Impact Analyzer
│   │   │   │
│   │   │   └── monitor/
│   │   │       ├── tracker.py      # Job Status Tracker
│   │   │       ├── alerts.py       # Alert Manager
│   │   │       ├── troubleshoot.py # AI Troubleshooter
│   │   │       └── metrics.py      # Performance Metrics
│   │   │
│   │   ├── connectors/             # DB Connectors
│   │   │   ├── base.py             # Abstract connector
│   │   │   ├── oracle.py
│   │   │   ├── postgresql.py
│   │   │   ├── mysql.py
│   │   │   ├── mssql.py
│   │   │   └── bigquery.py
│   │   │
│   │   ├── models/                 # SQLAlchemy Models
│   │   │   ├── datasource.py
│   │   │   ├── metadata.py
│   │   │   ├── validation.py
│   │   │   ├── lineage.py
│   │   │   └── monitor.py
│   │   │
│   │   └── schemas/                # Pydantic Schemas
│   │       ├── metadata.py
│   │       ├── transform.py
│   │       ├── validation.py
│   │       └── common.py
│   │
│   ├── templates/                  # Transformation Templates
│   │   ├── scd_type1.yaml
│   │   ├── scd_type2.yaml
│   │   ├── incremental_load.yaml
│   │   ├── full_load.yaml
│   │   ├── pivot_unpivot.yaml
│   │   ├── dedup.yaml
│   │   └── aggregation.yaml
│   │
│   └── tests/
│       ├── test_metadata.py
│       ├── test_transform.py
│       ├── test_validation.py
│       └── test_lineage.py
│
├── frontend/
│   ├── package.json
│   ├── src/
│   │   ├── App.tsx
│   │   ├── pages/
│   │   │   ├── Dashboard.tsx       # 메인 대시보드
│   │   │   ├── MetadataExplorer.tsx # 메타데이터 탐색기
│   │   │   ├── TransformStudio.tsx  # 변환 로직 작업 공간
│   │   │   ├── ValidationCenter.tsx # 검증 센터
│   │   │   ├── LineageViewer.tsx    # 리니지 뷰어
│   │   │   ├── MonitorConsole.tsx   # 모니터링 콘솔
│   │   │   └── AIChat.tsx          # AI 어시스턴트 채팅
│   │   │
│   │   ├── components/
│   │   │   ├── SqlEditor.tsx       # Monaco 기반 SQL 편집기
│   │   │   ├── LineageGraph.tsx    # React Flow 기반 DAG
│   │   │   ├── SchemaTree.tsx      # 스키마 트리 뷰
│   │   │   ├── ValidationTable.tsx # 검증 결과 테이블
│   │   │   └── MetricChart.tsx     # 메트릭 차트
│   │   │
│   │   └── hooks/
│   │       ├── useAgent.ts         # AI Agent 통신 훅
│   │       └── useWebSocket.ts     # 실시간 통신 훅
│   │
│   └── public/
│
└── docs/
    ├── architecture.md
    ├── api-reference.md
    └── user-guide.md
```

---

## 8. Key Differentiators vs Enterprise ETL

| 영역 | 기존 ETL Tool | AETL (이 프로젝트) |
|------|---------------|-------------------|
| **Transformation 생성** | GUI 드래그&드롭, 수동 매핑 | 자연어 → SQL 자동 생성, 템플릿 기반 |
| **메타데이터** | 별도 카탈로그 도구 필요 | 내장 메타데이터 엔진 + AI 해석 |
| **Data Quality** | 별도 DQ 도구 또는 제한적 | 프로파일 기반 규칙 자동 제안 |
| **Lineage** | 제한적 또는 추가 비용 | SQL 파싱 기반 컬럼 레벨 리니지 |
| **Troubleshooting** | 로그 수동 분석 | AI 근본 원인 분석 + 자동 복구 |
| **비용** | 수억 원/년 | 오픈소스 + LLM API 비용만 |
| **유연성** | 벤더 락인 | 코드 기반, 완전 커스터마이징 |

---

## 9. Implementation Roadmap

### Phase 1: Foundation (4주)
- [ ] 프로젝트 구조 + Docker 환경 설정
- [ ] DB Connector 추상화 (Oracle, PostgreSQL 우선)
- [ ] Metadata Engine: Schema Crawler + Data Profiler
- [ ] 기본 API 서버 (FastAPI) + 인증

### Phase 2: Intelligence Core (4주)
- [ ] AI Agent 프레임워크 (LangChain/LangGraph)
- [ ] SQL Engine: NL-to-SQL 생성 + 템플릿 시스템
- [ ] Validation Engine: 3-Tier 규칙 체계 + 실행 엔진
- [ ] Lineage Engine: SQL Parser + Graph Builder

### Phase 3: User Experience (3주)
- [ ] React Frontend: 메타데이터 탐색기, SQL Studio
- [ ] AI Chat Interface (WebSocket 기반)
- [ ] 리니지 시각화 (React Flow)
- [ ] 검증 결과 대시보드

### Phase 4: Production Ready (3주)
- [ ] Monitoring Engine: 알림 + 대시보드
- [ ] AI Troubleshooter: 에러 분석 + 자동 복구
- [ ] 보안 강화 (암호화, RBAC)
- [ ] 성능 최적화 + 부하 테스트

---

## 10. Risk & Mitigation

| 리스크 | 대응 |
|--------|------|
| **LLM 환각(Hallucination)** | 메타데이터 기반 검증 필수, 존재하지 않는 테이블/컬럼 참조 차단 |
| **SQL Injection** | 파라미터 바인딩 강제, DML 실행 전 승인 프로세스 |
| **성능 병목** | 메타데이터 캐싱, 비동기 처리, 프로파일링 배치 실행 |
| **DB 접속 보안** | 접속정보 암호화, IP 화이트리스트, 최소 권한 원칙 |
| **LLM API 비용** | 프롬프트 최적화, 캐싱, 경량 모델 fallback |

---

*본 문서는 AETL 프로젝트의 초기 설계 문서이며, 구현 과정에서 지속적으로 업데이트됩니다.*
