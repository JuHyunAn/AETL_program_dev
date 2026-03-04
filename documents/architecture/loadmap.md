# AETL v2 — 현재 구현 상태 및 로드맵

> **Version**: 2.3
> **Last Updated**: 2026-03-04
> **핵심 철학**: "SQL을 생성하고, 실행하고, 결과물을 만들어낸다"
> **설계 원칙**:
> 1. Human-in-the-Loop — AI는 제안만, 실행은 사람이 결정
> 2. 규칙 기반 우선 — sqlglot/규칙 엔진 우선, LLM은 설명·초안만
> 3. 멀티 DB 지원 — Oracle / MariaDB / PostgreSQL 동적 전환

---

## 1. 프로젝트 구조

```
AETL_program_dev/
│
├── aetl_llm.py               # LLM 프로바이더 통합 초기화 (PDF 네이티브 분석 포함)
├── aetl_agent.py              # LangGraph Tool-calling ETL 에이전트 (도구 7개)
│
├── db_schema.py               # DB 스키마 조회 (Oracle/MariaDB/PostgreSQL)
├── db_config.json             # DB 연결 설정
├── .schema_cache.json         # 스키마 캐시 파일 (자동 생성)
│
├── aetl_metadata_engine.py    # 메타데이터 엔진 (SQLite 사전 수집)
├── aetl_store.py              # 검증 규칙/실행 이력 저장소 (SQLite)
├── aetl_profiler.py           # 데이터 프로파일링 엔진
│
├── aetl_executor.py           # SQL 실행 엔진 (SELECT 자동/DML 승인)
├── aetl_designer.py           # DW Star Schema 설계 엔진 (PDF 네이티브 분석)
├── aetl_lineage.py            # SQL 리니지 추적 엔진 (sqlglot + NetworkX)
├── aetl_export.py             # 산출물 자동 생성 (Excel/JSON/CSV)
├── aetl_template_profile.py   # 사용자 정의 엑셀 양식 프로파일링
│
├── etl_sql_generator.py       # 검증 SQL 자동 생성 (규칙 기반 + LLM)
├── etl_metadata_parser.py     # Excel/CSV 테이블 정의서 파서
│
├── etl_streamlit_app.py       # 메인 웹 UI (Streamlit, 7개 페이지)
├── streamlit_app.py           # v1 기본 UI (레거시 — 폐기 예정)
│
├── etl_flow_component/        # ETL Flow Map 커스텀 컴포넌트 (ETL Lineage 페이지)
│   ├── __init__.py            # Streamlit 컴포넌트 선언
│   └── frontend/              # React + @xyflow/react + Vite 프론트엔드
│       ├── src/               # TypeScript 소스코드
│       └── build/             # 프로덕션 빌드 (npm run build 필요)
│
├── erd_flow_component/        # ERD Flow Map 커스텀 컴포넌트 (DW 설계 페이지)
│   ├── __init__.py            # Streamlit 컴포넌트 선언
│   └── frontend/              # React + @xyflow/react + Vite 프론트엔드
│       ├── src/               # TypeScript 소스코드
│       └── build/             # 프로덕션 빌드 (npm run build 필요)
│
├── .claude/
│   └── agent/
│       └── evaluator_agent.py # Claude Code 평가 에이전트 (pre/post-tool hook)
│
├── .env                       # 환경변수 (API 키, DB 비밀번호)
├── .aetl_metadata.db          # 메타데이터 SQLite (자동 생성)
├── aetl_metadata.db           # 검증 규칙/이력 SQLite (자동 생성)
├── CLAUDE.md                  # 프로젝트 가이드
└── documents/                 # 설계 문서
    └── architecture/
        ├── loadmap.md             # 현재 파일
        ├── AETL_Analysis_Presentation.md  # 발표 자료 초안
        ├── schema_doc.md          # Star Schema 설계 참고 문서
        ├── db_conn_template.txt   # DB 연결 설정 템플릿
        └── loadmap.jsx            # 로드맵 시각화 React 컴포넌트
```

---

## 2. UI 구조 (etl_streamlit_app.py)

### 2.1 사이드바 구조

```
┌──────────────────────────┐
│  AETL (로고)             │
│  AI-driven Sub ETL       │
├──────────────────────────┤
│  데이터 소스             │
│  ⚪ 파일 업로드          │
│  ⚪ DB 직접 연결         │
│  [DB 종류 / 연결 상태]   │
├──────────────────────────┤
│  생성 방식               │
│  [AI 강화 생성 토글]     │
├──────────────────────────┤
│  Copilot                 │
│    AI 챗봇               │
├──────────────────────────┤
│  Automation              │
│    데이터 프로파일       │
│    검증 쿼리 생성        │
│    검증 실행             │
│    매핑 자동화           │
│    ETL Lineage           │
├──────────────────────────┤
│  Modeling                │
│    DW 설계               │
└──────────────────────────┘
```

### 2.2 페이지별 기능 요약

| 페이지 | 모드 | 핵심 기능 | 백엔드 모듈 |
|--------|------|----------|-------------|
| AI 챗봇 | 공통 | 자연어 대화로 스키마 조회, SQL 생성, 규칙 제안 | aetl_agent |
| 데이터 프로파일 | DB 전용 | 테이블 통계 + AI 검증 규칙 자동 제안 | aetl_profiler, etl_sql_generator |
| 검증 쿼리 생성 | 공통 | 소스/타겟 → 검증 SQL 6종 자동 생성 | etl_metadata_parser, etl_sql_generator |
| 검증 실행 | DB 전용 | SQL 분류 + 실행 + AI 오류 진단 | aetl_executor |
| 매핑 자동화 | 공통 | 매핑 편집 → Excel/DDL/MERGE/리포트 산출물 | aetl_export, aetl_template_profile |
| ETL Lineage | 공통 | 등록된 매핑 → React Flow 시각화 | etl_flow_component |
| DW 설계 | 공통 | Swagger/PDF/텍스트 → Star Schema 설계 + ERD 시각화 | aetl_designer, erd_flow_component |

### 2.3 데이터 소스 전환

- `파일 업로드 ↔ DB 직접 연결` 전환 시 **모든 페이지 상태 초기화**
- 초기화 대상: 메타데이터, 매핑, 쿼리, 프로파일, 산출물, 채팅 이력, Flow Map
- 유지 대상: DB 연결 설정, 현재 페이지, AI 강화 토글

---

## 3. 모듈별 상세 구현 현황

### 3.1 aetl_llm.py — LLM 프로바이더 통합 유틸리티

**상태**: 구현 완료

```
지원 프로바이더:
  - gemini  : Google Gemini 2.5 Flash       (GOOGLE_API_KEY)
  - claude  : Anthropic Claude Sonnet 4.5   (ANTHROPIC_API_KEY)
  - openai  : OpenAI GPT-4o-mini            (OPENAI_API_KEY)

LLM_PROVIDER 환경변수 동작:
  - 명시 지정 시: 해당 프로바이더 우선, 나머지 fallback
  - 미설정 시  : gemini → claude → openai 순서로 자동 시도
```

**Public API**:
| 함수 | 설명 |
|------|------|
| `get_llm(with_tools=None)` | LLM 인스턴스 반환 (with_tools 시 bind_tools + fallback) |
| `call_llm(prompt)` | 프롬프트 → 텍스트 응답 (간편 래퍼) |
| `call_llm_with_pdf(prompt, pdf_bytes)` | PDF 네이티브 분석 (Claude document / Gemini inline) |

**PDF 분석 전략**:
- Claude: `document` 블록 (가장 정확)
- Gemini: `image_url` base64 인라인
- Fallback: PyMuPDF 텍스트 추출 후 프롬프트

---

### 3.2 aetl_agent.py — LangGraph Tool-calling ETL 에이전트

**상태**: 구현 완료

```
아키텍처:
  사용자 메시지 → [Agent Node] → LLM Tool-calling → [Tool Execute Node] → 반복 → 응답
```

**등록된 도구 (7개)**:

| 도구 | 설명 | 데이터 소스 |
|------|------|-------------|
| `get_table_schema` | 테이블 스키마(컬럼/PK/FK) 조회 | 메타데이터 → fallback DB |
| `search_tables` | 키워드 기반 테이블 검색 | 메타데이터 → fallback DB |
| `profile_table_tool` | 데이터 프로파일링 (통계/분포) | 메타데이터 → fallback 라이브 DB |
| `generate_validation_queries_tool` | 검증 SQL 6종 자동 생성 | 스키마 기반 규칙 엔진 |
| `suggest_rules_tool` | 프로파일 기반 검증 규칙 자동 제안 | 프로파일 통계 |
| `compare_row_counts` | 소스/타겟 건수 직접 비교 | 항상 라이브 DB |
| `sync_metadata_tool` | 스키마/프로파일 메타데이터 동기화 | DB → SQLite |

---

### 3.3 db_schema.py — DB 스키마 조회 모듈

**상태**: 구현 완료 (Oracle / MariaDB / PostgreSQL)

```
클래스 계층:
  SchemaFetcher (ABC)
  ├── OracleSchemaFetcher      — oracledb 드라이버
  ├── MariaDBSchemaFetcher     — mariadb 드라이버
  └── PostgreSQLSchemaFetcher  — psycopg2 드라이버 (다중 스키마 + LIKE 패턴)
```

**PostgreSQL 특이사항**:
- `owner` 필드에 쉼표 구분 또는 LIKE 패턴 지정 가능
  - `"public"` → `LIKE 'public%'` (public, public_staging 등 매칭)
  - `"public,marts"` → 각각 exact match
  - `"public%staging"` → 와일드카드 LIKE
- 빈 값이면 시스템 스키마 제외한 모든 사용자 스키마 자동 감지
- `pg_catalog.format_type()` 사용한 정확한 데이터 타입 조회

**캐싱 시스템**:
- `.schema_cache.json` + TTL 기반 만료 (기본 3600초)
- `schema_options` + `db_type` MD5 핑거프린트로 설정 변경 시 자동 무효화

---

### 3.4 aetl_metadata_engine.py — 메타데이터 사전 수집 엔진

**상태**: 구현 완료

```
SQLite 테이블 (.aetl_metadata.db):
  - meta_tables   : 테이블 목록 + 행 수 + 역할(suggested_role)
  - meta_columns  : 컬럼 메타데이터 (타입, PK, FK)
  - meta_profiles : 컬럼별 통계 (null 비율, distinct 수, min/max, top 값)

테이블 역할 자동 분류:
  - SOURCE: ods_, stg_, dw_, raw_, src_, ext_, load_ 접두사
  - TARGET: dm_, fact_, dim_, f_, d_, rpt_, agg_, mart_ 접두사
  - 스키마 힌트도 활용 (ods, staging → source / dm, mart → target)

프로파일 TTL: 24시간 (이내 재수집 생략)
```

> **참고**: `confirmed_role` (사용자 수동 확정) 기능은 v2.2에서 제거됨.
> `suggested_role` 자동 분류만으로 UI 라벨 추천에 충분.

---

### 3.5 aetl_store.py — 검증 규칙/실행 이력 저장소

**상태**: 구현 완료

```
SQLite 테이블 (aetl_metadata.db):
  - datasource        : 데이터소스 연결 정보
  - table_meta        : 테이블 메타 + 프로파일 JSON
  - column_meta       : 컬럼별 통계
  - validation_rule   : 검증 규칙 (AI 자동 생성/수동)
  - validation_result : 검증 실행 결과 이력
```

---

### 3.6 aetl_profiler.py — 데이터 프로파일링 엔진

**상태**: 구현 완료 (Oracle / MariaDB / PostgreSQL)

```
수집 항목 (컬럼별):
  - total_cnt / non_null_cnt → null_pct 계산
  - distinct_count
  - min / max 값
  - top_values (빈도 상위 N개)
  - inferred_domain (패턴 기반: email, phone, date, amount, code, name, id 등)
```

---

### 3.7 aetl_executor.py — SQL 실행 엔진

**상태**: 구현 완료

```
SQL 분류 (sqlglot AST 기반):
  - SELECT  → 자동 실행 허용 (행 수 제한 적용)
  - DML     → 사용자 명시적 승인 후 실행
  - DDL     → 사용자 명시적 승인 후 실행
  - UNKNOWN → 차단

안전장치:
  - sqlglot AST 전체 탐색으로 서브쿼리 내 DML/DDL 이중 차단
  - 행 수 제한 자동 적용 (Oracle ROWNUM / PG LIMIT)
  - AI 기반 검증 실패 진단 + 수정 SQL 제안
  - 실행 이력 SQLite 로깅
```

---

### 3.8 aetl_designer.py — DW Star Schema 설계 엔진

**상태**: 구현 완료

```
입력 전략 (우선순위):
  1순위: Swagger/OpenAPI JSON/YAML → 완전 자동 파싱
  2순위: PDF 문서 → AI 네이티브 분석 (Claude document / Gemini inline)
  3순위: 자유 텍스트 → AI 초안 (사용자 검토 필수)

출력:
  - Star Schema 설계 JSON (ODS/FACT/DIM/DM 테이블)
  - Mermaid erDiagram / flowchart 코드
  - DDL Script (Oracle/MariaDB/PostgreSQL)
  - erd_flow_component를 통한 인터랙티브 ERD 시각화
```

---

### 3.9 aetl_lineage.py — SQL 리니지 추적 엔진

**상태**: 구현 완료

```
역할 분리:
  - sqlglot     : SQL → AST → 테이블/컬럼 리니지 추출 (100% 결정론적)
  - NetworkX    : DAG 구성, Forward/Backward 영향도 탐색
  - LLM         : 리니지 결과를 한국어로 설명 (파싱은 절대 안 맡김)
  - Mermaid     : Streamlit 내 시각화 코드 생성
```

> **참고**: 현재 UI(ETL Lineage 페이지)에서는 React Flow 기반 Flow Map만 사용 중.
> sqlglot 기반 SQL 리니지(컬럼 레벨)는 Agent를 통해서만 활용 가능.

---

### 3.10 aetl_export.py — 산출물 자동 생성 엔진

**상태**: 구현 완료

```
출력 형식:
  1. 매핑정의서 Excel (6개 시트: 개요/소스/타겟/매핑/적재SQL/검증SQL)
  2. DDL Script (Oracle/MariaDB/PostgreSQL 방언 지원)
  3. MERGE/UPSERT SQL (Oracle MERGE / MariaDB ON DUPLICATE KEY / PG ON CONFLICT)
  4. 검증 리포트 Excel (요약/상세 결과/실행 SQL)
  5. JSON Raw Export
  6. CSV 컬럼 매핑 Export
```

---

### 3.11 etl_sql_generator.py — 검증 SQL 자동 생성

**상태**: 구현 완료

```
생성하는 검증 쿼리 (6종):
  1. row_count_check   — 건수 비교
  2. pk_missing_check  — PK 누락 검증
  3. null_check        — 주요 컬럼 NULL 체크
  4. duplicate_check   — 타겟 PK 중복 검증
  5. checksum_check    — 체크섬 비교
  6. full_diff_check   — 전체 데이터 불일치 확인

생성 방식:
  - 규칙 기반 (no_llm): DB 방언별 SQL 직접 조립 — API 키 불필요
  - LLM 기반: 프롬프트로 검증 SQL 생성 — AI 강화 토글 ON 시
  - 규칙 제안: 프로파일 통계 기반 자동 규칙 생성
```

---

### 3.12 etl_metadata_parser.py — Excel/CSV 메타데이터 파서

**상태**: 구현 완료

```
지원 형식:
  - Excel (.xlsx, .xls): 단일/멀티 시트, 매핑정의서 형식
  - CSV (.csv)

자동 감지 기능:
  - 한글/영문 양방향 컬럼명 매핑 (table_name↔테이블명 등)
  - 소스/타겟 시트 자동 감지 (source/소스/src, target/타겟/tgt 등)
  - PK 대소문자 정규화 (PostgreSQL lowercase → 대문자 비교)
```

---

### 3.13 aetl_template_profile.py — 사용자 정의 엑셀 양식 프로파일

**상태**: 구현 완료

```
동작 방식:
  1. [등록] 빈 양식 업로드 → 헤더 자동 분석 → UI에서 확인/수정
  2. [저장] 헤더↔필드 매핑을 profile.json + 원본 템플릿으로 저장
  3. [적용] profile 로드 → 원본 양식에 데이터 write-back → 다운로드

원칙:
  - AI는 초안(헤더 후보 제안)만 담당, 사람이 최종 확정
  - 확정 후 100% 결정론적 동작 (LLM 없음)
```

---

### 3.14 etl_flow_component — ETL Flow Map 커스텀 컴포넌트

**상태**: 구현 완료 (프론트엔드 빌드 필요)

```
기술 스택: React + @xyflow/react + @dagrejs/dagre + Vite
빌드: cd etl_flow_component/frontend && npm install && npm run build

기능:
  - 매핑 자동화 페이지에서 등록된 매핑을 인터랙티브 Flow Map으로 시각화
  - 자동 레이아웃 (dagre): ODS → DW → DM 레이어 배치
  - 노드 클릭 시 컬럼 상세 표시
  - 줌/팬/드래그 지원
```

---

### 3.15 erd_flow_component — ERD Flow Map 커스텀 컴포넌트

**상태**: 구현 완료 (프론트엔드 빌드 필요)

```
기술 스택: React + @xyflow/react + Vite
빌드: cd erd_flow_component/frontend && npm install && npm run build

기능:
  - DW 설계 페이지에서 Star Schema ERD를 인터랙티브 그래프로 시각화
  - ODS / FACT / DIM / DM 레이어별 색상 구분
  - 테이블 노드 클릭 시 컬럼 상세 표시
```

> **빌드 시점**: `frontend/src/` 수정 시에만 `npm run build` 필요.
> Python 파일 변경은 Streamlit 자동 반영.

---

## 4. 아키텍처 통합도

### 4.1 엔진 간 연동 흐름

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        AETL v2 통합 아키텍처                              │
│                                                                          │
│   ┌───────────────────────────────────────────────────────────────────┐  │
│   │                     사용자 인터페이스                              │  │
│   │         etl_streamlit_app.py (7개 페이지)                         │  │
│   └────────────────────────┬──────────────────────────────────────────┘  │
│                            │                                             │
│   ┌────────────────────────▼──────────────────────────────────────────┐  │
│   │                 LLM 프로바이더 (aetl_llm.py)                       │  │
│   │     Gemini / Claude / OpenAI  (fallback 체인 + PDF 분석)           │  │
│   └────────────────────────┬──────────────────────────────────────────┘  │
│                            │                                             │
│   ┌────────────────────────▼──────────────────────────────────────────┐  │
│   │               에이전트 / 엔진 레이어                               │  │
│   │                                                                   │  │
│   │  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐  │  │
│   │  │ aetl_agent   │   │ aetl_executor│   │    aetl_designer     │  │  │
│   │  │ (Tool-call   │   │ (SQL 실행    │   │ (DW 설계 + PDF 분석  │  │  │
│   │  │  Agent 7도구) │   │  + AI 진단)  │   │  + erd_flow_comp)   │  │  │
│   │  └──────┬───────┘   └──────────────┘   └──────────────────────┘  │  │
│   │         │                                                         │  │
│   │  ┌──────▼───────┐   ┌──────────────┐   ┌──────────────────────┐  │  │
│   │  │ aetl_lineage │   │ aetl_export  │   │   aetl_template      │  │  │
│   │  │ (리니지 추적)│   │ (산출물 생성)│   │   (양식 프로파일)    │  │  │
│   │  └──────────────┘   └──────────────┘   └──────────────────────┘  │  │
│   │                                                                   │  │
│   │  ┌──────────────┐   ┌──────────────┐   ┌──────────────────────┐  │  │
│   │  │ aetl_profiler│   │etl_sql_gen   │   │  etl_metadata_parser │  │  │
│   │  │ (프로파일링) │   │(검증SQL 생성)│   │  (Excel/CSV 파서)    │  │  │
│   │  └──────────────┘   └──────────────┘   └──────────────────────┘  │  │
│   │                                                                   │  │
│   │  ┌──────────────────────────────────┐                            │  │
│   │  │ etl_flow_component (ETL Lineage) │                            │  │
│   │  │ erd_flow_component (DW 설계 ERD) │                            │  │
│   │  └──────────────────────────────────┘                            │  │
│   └────────────────────────┬──────────────────────────────────────────┘  │
│                            │                                             │
│   ┌────────────────────────▼──────────────────────────────────────────┐  │
│   │                  데이터 레이어                                     │  │
│   │                                                                   │  │
│   │  ┌──────────────┐   ┌──────────────────────┐   ┌──────────────┐  │  │
│   │  │ db_schema.py │   │ aetl_metadata_engine  │   │ aetl_store   │  │  │
│   │  │ (스키마 조회  │   │ (.aetl_metadata.db    │   │(aetl_meta-   │  │  │
│   │  │  + 캐시)     │   │  스키마+프로파일 캐시) │   │ data.db      │  │  │
│   │  │              │   │                      │   │ 규칙+이력)   │  │  │
│   │  └──────┬───────┘   └──────────────────────┘   └──────────────┘  │  │
│   └─────────┼─────────────────────────────────────────────────────────┘  │
│             │                                                            │
│   ┌─────────▼──────────────────────────────────────────────────────┐     │
│   │                    DB 드라이버 레이어                            │     │
│   │    Oracle (oracledb) │ MariaDB (mariadb) │ PostgreSQL (psycopg2) │     │
│   └─────────────────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## 5. 기술 스택

| 구분 | 기술 | 용도 |
|------|------|------|
| **LLM** | Gemini 2.5 Flash / Claude Sonnet 4.5 / GPT-4o-mini | 자연어 처리, SQL 생성, 진단, PDF 분석 |
| **에이전트** | LangChain + LangGraph | Tool-calling 에이전트 파이프라인 |
| **SQL 파서** | sqlglot | SQL 분류, 리니지 추출, 안전 검사 |
| **그래프** | NetworkX | 리니지 DAG, 영향도 분석 |
| **UI** | Streamlit | 웹 대시보드 (7개 페이지) |
| **Flow 시각화** | React + @xyflow/react + dagre | ETL Flow Map / ERD Flow Map |
| **DB 드라이버** | oracledb / mariadb / psycopg2 | 멀티 DB 연결 |
| **엑셀** | openpyxl / pandas | 매핑정의서, 검증 리포트, 템플릿 |
| **저장소** | SQLite | 메타데이터, 검증 규칙/이력 영속 저장 |
| **환경** | python-dotenv | API 키, DB 비밀번호 관리 |

---

## 6. 구현 완료 / 변경 이력

### v2.3 변경 사항 (2026-03-04)

| 항목 | 모듈 | 변경 내용 |
|------|------|----------|
| 다운로드 버튼 스타일 통일 | etl_streamlit_app.py | 매핑정의서 Excel 버튼에서 dl-primary 래퍼 제거 |
| loadmap 최신화 | documents/architecture/loadmap.md | erd_flow_component 추가, app.py 삭제 반영, 도구 수 정정 |
| CLAUDE.md 정리 | CLAUDE.md | 보안 정보 제거, 내용 최신화 |

### v2.2 변경 사항 (2026-03-02)

| 항목 | 모듈 | 변경 내용 |
|------|------|----------|
| PK 표시 수정 | etl_metadata_parser.py | PostgreSQL lowercase PK → 대소문자 정규화 |
| ETL Flow Map 빌드 | etl_flow_component | React 프론트엔드 프로덕션 빌드 |
| PDF 네이티브 분석 | aetl_llm.py, aetl_designer.py | Claude/Gemini로 PDF 직접 분석 (텍스트 추출 대신) |
| 데이터 소스 전환 초기화 | etl_streamlit_app.py | 모드 변경 시 전체 세션 상태 리셋 |
| 테이블 역할 관리 제거 | etl_streamlit_app.py, aetl_agent.py | UI expander + get_tables_by_role 도구 제거 |
| app.py 삭제 | — | v1 레거시 SQL 챗봇 엔진 완전 제거 |

### v2.1 구현 완료 항목 (2026-02-28)

| 항목 | 모듈 | 상태 |
|------|------|------|
| LIKE 패턴 스키마 매칭 | db_schema.py | 완료 |
| 테이블 역할 자동 분류 (suggested_role) | aetl_metadata_engine.py | 완료 |
| UI 추천 라벨 (source/target 추천) | etl_streamlit_app.py | 완료 |
| 컬럼 상세 조회 (data_type, PK, nullable) | db_schema.py | 완료 |
| LLM_PROVIDER 엄격 fallback | aetl_llm.py | 완료 |
| 스키마 캐시 핑거프린트 무효화 | db_schema.py | 완료 |

### 전체 구현 완료 항목

| 항목 | 모듈 |
|------|------|
| LLM 프로바이더 통합 (Gemini/Claude/OpenAI) | aetl_llm.py |
| PDF 네이티브 분석 (Claude/Gemini) | aetl_llm.py |
| LangGraph Tool-calling 에이전트 (7도구) | aetl_agent.py |
| Oracle / MariaDB / PostgreSQL 스키마 조회 | db_schema.py |
| PostgreSQL 다중 스키마 + LIKE 패턴 지원 | db_schema.py |
| 스키마 캐싱 + 옵션 변경 자동 무효화 | db_schema.py |
| 데이터 프로파일링 (3 DB) | aetl_profiler.py |
| 메타데이터 사전 수집 엔진 | aetl_metadata_engine.py |
| 테이블 역할 자동 분류 + UI 추천 라벨 | aetl_metadata_engine.py |
| 검증 규칙/이력 관리 저장소 | aetl_store.py |
| 검증 SQL 자동 생성 (6종, 규칙/LLM) | etl_sql_generator.py |
| 프로파일 기반 검증 규칙 자동 제안 | etl_sql_generator.py |
| SQL 실행 엔진 (SELECT 자동/DML 승인) | aetl_executor.py |
| sqlglot 기반 SQL 분류 + 서브쿼리 이중 차단 | aetl_executor.py |
| AI 기반 검증 실패 진단 + 수정 SQL 제안 | aetl_executor.py |
| SQL 리니지 추적 (sqlglot + NetworkX) | aetl_lineage.py |
| Mermaid 리니지 시각화 (컬럼/테이블 레벨) | aetl_lineage.py |
| ETL Flow Map (React + XY Flow) | etl_flow_component |
| ERD Flow Map (React + XY Flow, DW 설계용) | erd_flow_component |
| 매핑정의서 Excel 자동 생성 (6시트) | aetl_export.py |
| DDL 생성 (Oracle/MariaDB/PostgreSQL) | aetl_export.py |
| MERGE/UPSERT SQL 생성 (3 DB) | aetl_export.py |
| 검증 리포트 Excel 생성 | aetl_export.py |
| Swagger/OpenAPI → DW 설계 자동 추천 | aetl_designer.py |
| PDF → DW 설계 (AI 네이티브 분석) | aetl_designer.py |
| Star Schema ERD (Mermaid erDiagram) | aetl_designer.py |
| ODS→DW→DM 흐름도 (Mermaid flowchart) | aetl_designer.py |
| Excel/CSV 메타데이터 파서 (한글/영문) | etl_metadata_parser.py |
| 사용자 정의 엑셀 양식 프로파일 | aetl_template_profile.py |
| 데이터 소스 전환 시 전체 상태 초기화 | etl_streamlit_app.py |
| v1 레거시 코드 제거 (app.py 삭제) | — |

---

## 7. 설계 검토 의견 (서브 ETL 툴 관점)

### 7.1 핵심 강점

| 영역 | 강점 |
|------|------|
| 멀티 DB | 3개 DB 네이티브 지원, 방언별 SQL 자동 생성 |
| 규칙 우선 | sqlglot/패턴 매칭 우선, LLM은 보조 역할만 |
| 산출물 자동화 | 매핑 테이블 하나로 Excel/DDL/MERGE/리포트 6종 동시 생성 |
| Human-in-the-Loop | DML/DDL 실행 전 사용자 승인 필수 |

### 7.2 재고가 필요한 부분

#### (1) 레거시 코드 정리 — `streamlit_app.py`

- `app.py`는 v2.2에서 삭제 완료
- `streamlit_app.py`는 아직 존재하나 `etl_streamlit_app.py`로 완전히 대체됨
- **제안**: `streamlit_app.py` 삭제 또는 `_legacy/` 폴더로 이동

#### (2) SQLite 이중 저장소 — `aetl_metadata_engine.py` vs `aetl_store.py`

- `.aetl_metadata.db`: 스키마 + 프로파일 (metadata_engine)
- `aetl_metadata.db`: 검증 규칙 + 실행 이력 (store)
- 프로파일 데이터가 **양쪽 모두에 저장**되어 동기화 불일치 가능
- **제안**: 단일 SQLite로 통합하거나, 역할을 명확히 분리 (metadata_engine=캐시 전용, store=이력 전용)

#### (3) 검증 실행 페이지 — 워크플로우 단절

- "검증 쿼리 생성"에서 만든 SQL을 "검증 실행"에서 사용하려면 **수동 복사** 필요
- 세션 상태로 연결되어 있지 않음
- **제안**: 검증 쿼리 생성 결과를 `exec_sql_input`에 자동 전달하거나, 쿼리 옆에 "바로 실행" 버튼 추가

#### (4) ETL Lineage — aetl_lineage.py 기능 미활용

- `aetl_lineage.py`는 sqlglot 기반 **컬럼 레벨 리니지** + NetworkX 영향도 분석 + Mermaid 시각화 보유
- 하지만 ETL Lineage UI 페이지는 **React Flow Map만 표시** (매핑 등록 기반)
- SQL 리니지 기능은 Agent를 통해서만 간접 접근 가능
- **제안**: Lineage 페이지에 "SQL 리니지 분석" 탭 추가 — SQL 입력 → 컬럼 레벨 Mermaid 출력

#### (5) DW 설계 — 메인 파이프라인과 미연결

- DW 설계로 생성된 테이블이 검증 쿼리 생성의 소스/타겟으로 바로 사용 불가
- 설계 결과 → 매핑 자동화로의 연계가 없음 (독립적 기능)
- **판단**: 서브 ETL 툴의 핵심은 "검증 + 매핑 자동화"이므로, DW 설계는 **보조 기능**으로 현재 수준 유지 가능

#### (6) 데이터 프로파일 — DB 전용 제한

- "파일 업로드" 모드에서는 프로파일링 불가 (DB 직접 연결 필요)
- **판단**: 프로파일링은 실제 DB 데이터가 있어야 의미 있으므로 현재 제한 합리적

#### (7) ODS→DW→DM 파이프라인 실행 부재

- ODS/DW/DM은 설계·시각화·레이어 라벨에만 사용됨. 여러 매핑을 **순서대로 실행**하는 파이프라인/잡 개념 없음.
- **제안**: [ods_dw_dm_strategy.md](./ods_dw_dm_strategy.md) 참조 — 파이프라인 정의 → 순차 실행 → 이력 저장 단계별 도입.

---

## 8. 로드맵 (향후 계획)

> **ODS → DW → DM 상용 ETL 수준 전략**은 별도 문서 [ods_dw_dm_strategy.md](./ods_dw_dm_strategy.md) 참조.  
> 파이프라인 정의·순차 실행·이력 저장을 단계별로 도입하는 현실적 계획이 정리되어 있음.

### Phase 1: 안정화 및 정리 ✅ 완료

- [x] LLM 프로바이더 통합 + PDF 네이티브 분석
- [x] PostgreSQL 다중 스키마 + 캐시 무효화
- [x] 데이터 소스 전환 시 전체 초기화
- [x] app.py 레거시 삭제
- [ ] streamlit_app.py 레거시 삭제
- [ ] SQLite 이중 저장소 통합 검토
- [ ] 전체 모듈 통합 테스트

### Phase 2: 워크플로우 연결

- [ ] 검증 쿼리 → 검증 실행 자동 연결
- [ ] ETL Lineage 페이지에 SQL 리니지 분석 탭 추가
- [ ] 검증 결과 대시보드 (Pass/Fail 추이)
- [ ] DW 설계 → 매핑 자동화 연계 (선택)
- [ ] **ODS→DW→DM 파이프라인 정의** (스텝 = 매핑 참조, 순서 저장) — [ods_dw_dm_strategy.md](./ods_dw_dm_strategy.md) Phase A
- [ ] **파이프라인 순차 실행** (DML 승인 후 스텝별 실행 + 실행 이력) — 동일 문서 Phase B

### Phase 3: 운영 기능

- [ ] 사용자 권한 기반 접근 제어
- [ ] 실행 감사 로깅 강화
- [ ] DB 계정 권한 분리 (검증/테스트)

### Phase 4: 확장

- [ ] 별도 프론트엔드 (React) + REST API
- [ ] 스케줄러 연동 (Airflow 등)
- [ ] 추가 DB 지원 (MSSQL, BigQuery)

---

## 9. 핵심 사용 시나리오

### 시나리오 1: 에이전트 대화형 ETL 검증

```
[User]  "EMPLOYEE 테이블의 스키마를 보여줘"
[Agent] get_table_schema("EMPLOYEE") → 컬럼/타입/PK 정보 출력

[User]  "EMPLOYEE와 DEPARTMENT 간 건수를 비교해줘"
[Agent] compare_row_counts("EMPLOYEE", "DEPARTMENT") → PASS/FAIL 결과

[User]  "두 테이블의 검증 쿼리를 만들어줘"
[Agent] generate_validation_queries_tool → 6종 SQL
```

### 시나리오 2: 매핑정의서 → 산출물 자동 생성

```
[Step 1] Excel 정의서 업로드
         → parse_mapping_definition_excel() → 소스/타겟/매핑 메타데이터

[Step 2] 매핑 테이블 편집 (Single Source of Truth)
         → MERGE SQL 미리보기 즉시 반영

[Step 3] 산출물 생성 버튼 클릭
         → 매핑정의서 Excel + DDL + MERGE SQL + 검증 리포트 + JSON + CSV

[Step 4] Flow Map 등록 → ETL Lineage 페이지에서 시각화
```

### 시나리오 3: API 문서 → DW 설계

```
[Step 1] Swagger JSON 또는 PDF 업로드
         → 엔티티 구조 추출 (Swagger 자동 / PDF AI 분석)

[Step 2] AI Star Schema 설계
         → ODS/FACT/DIM/DM 테이블 설계

[Step 3] 시각화 확인
         → Mermaid ERD + 레이어 흐름도 + erd_flow_component 인터랙티브 ERD

[Step 4] DDL 생성 → 다운로드
```

---

*AETL v2.3 — 생성(Generate)하고, 실행(Execute)하고, 산출물(Deliver)까지*
