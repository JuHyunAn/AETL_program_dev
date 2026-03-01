# AETL v2 — 현재 구현 상태 및 로드맵

> **Version**: 2.1
> **Last Updated**: 2026-02-28
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
├── aetl_llm.py               # LLM 프로바이더 통합 초기화 유틸리티
├── aetl_agent.py              # LangGraph Tool-calling ETL 에이전트
├── app.py                     # v1 SQL 챗봇 엔진 (LangGraph 파이프라인)
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
├── aetl_designer.py           # DW Star Schema 설계 엔진
├── aetl_lineage.py            # SQL 리니지 추적 엔진 (sqlglot + NetworkX)
├── aetl_export.py             # 산출물 자동 생성 (Excel/JSON/CSV)
├── aetl_template_profile.py   # 사용자 정의 엑셀 양식 프로파일링
│
├── etl_sql_generator.py       # 검증 SQL 자동 생성 (규칙 기반 + LLM)
├── etl_metadata_parser.py     # Excel/CSV 테이블 정의서 파서
│
├── etl_streamlit_app.py       # 메인 웹 UI (Streamlit)
├── streamlit_app.py           # 기본 UI (Streamlit, 구버전)
│
├── .env                       # 환경변수 (API 키, DB 비밀번호)
├── .aetl_metadata.db          # 메타데이터 SQLite (자동 생성)
├── aetl_metadata.db           # 검증 규칙/이력 SQLite (자동 생성)
├── CLAUDE.md                  # 프로젝트 가이드
└── documents/                 # 설계 문서
    └── loadmap.md             # 현재 파일
```

---

## 2. 모듈별 상세 구현 현황

### 2.1 aetl_llm.py — LLM 프로바이더 통합 유틸리티

**상태**: 구현 완료

모든 모듈이 공유하는 LLM 초기화 중앙 모듈입니다.

```
지원 프로바이더:
  - gemini  : Google Gemini 2.5 Flash  (GOOGLE_API_KEY)
  - claude  : Anthropic Claude Sonnet  (ANTHROPIC_API_KEY)
  - openai  : OpenAI GPT-4o-mini       (OPENAI_API_KEY)

LLM_PROVIDER 환경변수 동작:
  - 명시 지정 시: 해당 프로바이더**만** 시도, 실패 시 에러 (fallback 없음)
  - 미설정 시  : gemini → claude → openai 순서로 자동 시도
```

**Public API**:
| 함수 | 설명 |
|------|------|
| `get_llm(with_tools=None)` | LLM 인스턴스 반환 (with_tools 시 bind_tools 적용) |
| `call_llm(prompt)` | 프롬프트 → 텍스트 응답 반환 (간편 래퍼) |

**사용 모듈**: `aetl_agent`, `aetl_designer`, `aetl_executor`, `aetl_lineage`, `etl_sql_generator`

---

### 2.2 aetl_agent.py — LangGraph Tool-calling ETL 에이전트

**상태**: 구현 완료

LangGraph 기반 Tool-calling 에이전트로, 자연어 요청을 받아 도구를 자동 선택·실행합니다.

```
아키텍처:
  사용자 메시지 → [Agent Node] → LLM Tool-calling → [Tool Execute Node] → 반복 → 응답

State:
  - messages: 대화 메시지 시퀀스
  - db_type:  oracle | mariadb | postgresql
  - config_path: db_config.json 경로
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

**Public API**:
| 함수 | 설명 |
|------|------|
| `build_graph()` | LangGraph 컴파일된 그래프 반환 |
| `run_agent(user_message, db_type, config_path, chat_history)` | 에이전트 실행, (응답, 이력) 반환 |

---

### 2.3 app.py — v1 SQL 챗봇 엔진

**상태**: 구현 완료 (레거시, aetl_agent와 병존)

자연어 → Oracle SQL 변환 전용 LangGraph 파이프라인입니다.

```
처리 흐름 (v2.0 토큰 최적화):
  1. clarify_node       — 질문 모호성 확인 (LLM 없음)
  2. generate_sql_node  — 의도 파싱 + 계획 + SQL 생성 (LLM 1회)
  3. validate_sql_node  — 보안/문법 검증 (LLM 없음)
  4. repair_node        — 검증 실패 시 재시도 (LLM 1회 추가)

보안 정책:
  - DML/DDL 금지 (INSERT, UPDATE, DELETE, DROP 차단)
  - SELECT * 금지 (명시적 컬럼 지정 필수)
  - 행 수 제한 (최대 10,000건)
  - PII 보호 (주민번호 뒷자리 직접 조회 금지)
  - 스키마 검증 (정의되지 않은 테이블/컬럼 차단)
```

> **참고**: `app.py`는 아직 하드코딩된 `ChatGoogleGenerativeAI`를 사용합니다.
> `aetl_llm.py`로의 마이그레이션은 아직 미완료입니다.

---

### 2.4 db_schema.py — DB 스키마 조회 모듈

**상태**: 구현 완료 (Oracle / MariaDB / PostgreSQL)

추상 클래스 `SchemaFetcher` 기반의 전략 패턴으로 3개 DB를 지원합니다.

```
클래스 계층:
  SchemaFetcher (ABC)
  ├── OracleSchemaFetcher      — oracledb 드라이버
  ├── MariaDBSchemaFetcher     — mariadb 드라이버
  └── PostgreSQLSchemaFetcher  — psycopg2 드라이버 (다중 스키마 지원)
```

**PostgreSQL 특이사항**:
- `owner` 필드에 쉼표 구분으로 여러 스키마 지정 가능 (`"public,public_marts"`)
- 빈 값이면 시스템 스키마를 제외한 모든 사용자 스키마 자동 감지
- 다중 스키마 시 테이블 키는 `"schema.TABLE_NAME"` 형태

**캐싱 시스템**:
- `.schema_cache.json` 파일에 스키마 캐시 저장
- TTL 기반 만료 (기본 3600초)
- `schema_options` + `db_type` MD5 핑거프린트로 설정 변경 시 자동 무효화
- `force_refresh=True`로 캐시 우회 가능

**주요 함수**:
| 함수 | 설명 |
|------|------|
| `load_config(config_path)` | 설정 파일 로드 + `${ENV_VAR}` 치환 |
| `get_schema(config_path, force_refresh)` | 스키마 반환 (캐시 우선) |
| `get_db_type(config_path)` | DB 타입 문자열 반환 |
| `get_fetcher(config)` | DB 타입별 Fetcher 인스턴스 팩토리 |

---

### 2.5 aetl_metadata_engine.py — 메타데이터 사전 수집 엔진

**상태**: 구현 완료

DB 스키마/프로파일을 SQLite(`.aetl_metadata.db`)에 사전 수집하여 Agent가 빠르게 조회합니다.

```
SQLite 테이블:
  - meta_tables   : 테이블 목록 + 행 수
  - meta_columns  : 컬럼 메타데이터 (타입, PK, FK)
  - meta_profiles : 컬럼별 통계 (null 비율, distinct 수, min/max, top 값)

프로파일 TTL: 24시간 (이내 재수집 생략)
```

**주요 함수**:
| 함수 | 설명 |
|------|------|
| `sync_schema(config_path, tables)` | db_schema → SQLite 동기화 |
| `sync_profile(config_path, tables, force)` | 프로파일링 → SQLite 동기화 |
| `get_all_tables()` | 테이블 목록 조회 |
| `get_table_schema_from_meta(table_name)` | 스키마 조회 (대소문자 무관) |
| `search_tables_from_meta(keyword)` | 키워드 검색 |
| `get_profile_from_meta(table_name)` | 프로파일 조회 |
| `is_schema_synced()` | 메타데이터 존재 여부 확인 |
| `clear_metadata()` | 전체 초기화 |

---

### 2.6 aetl_store.py — 검증 규칙/실행 이력 저장소

**상태**: 구현 완료

프로파일링 결과, 검증 규칙, 검증 실행 이력을 SQLite(`aetl_metadata.db`)에 관리합니다.

```
SQLite 테이블:
  - datasource        : 데이터소스 연결 정보
  - table_meta        : 테이블 메타 + 프로파일 JSON
  - column_meta       : 컬럼별 통계
  - validation_rule   : 검증 규칙 (AI 자동 생성/수동)
  - validation_result : 검증 실행 결과 이력
```

**주요 함수**:
| 함수 | 설명 |
|------|------|
| `get_or_create_datasource(name, db_type)` | 데이터소스 UPSERT |
| `save_profile(profile, source_id)` | 프로파일 저장 |
| `save_validation_rules(rules)` | 검증 규칙 저장 |
| `save_validation_run(results, execution_id)` | 실행 결과 저장 |
| `get_validation_history(table_name, limit)` | 이력 조회 |
| `get_execution_summary(execution_id)` | 실행 요약 통계 |

---

### 2.7 aetl_profiler.py — 데이터 프로파일링 엔진

**상태**: 구현 완료 (Oracle / MariaDB / PostgreSQL)

DB에 직접 연결하여 테이블·컬럼 단위 통계를 수집합니다.

```
수집 항목 (컬럼별):
  - total_cnt / non_null_cnt → null_pct 계산
  - distinct_count
  - min / max 값
  - top_values (빈도 상위 N개)
  - inferred_domain (패턴 기반 도메인 자동 추론)

도메인 추론 패턴 (컬럼명 + 타입):
  email, phone, date, amount, code, name, id, address, count
```

**주요 함수**:
| 함수 | 설명 |
|------|------|
| `profile_table(conn, table_name, db_type, ...)` | 커넥션 기반 프로파일링 |
| `profile_table_from_config(config_path, table_name)` | 설정 파일 기반 간편 호출 |
| `profile_summary_text(profile)` | 프로파일 → LLM 프롬프트용 텍스트 |

---

### 2.8 aetl_executor.py — SQL 실행 엔진

**상태**: 구현 완료

Human-in-the-Loop 원칙에 따라 SELECT는 자동 실행, DML/DDL은 사용자 승인 후에만 실행합니다.

```
SQL 분류 (sqlglot AST 기반):
  - SELECT  → 자동 실행 허용 (행 수 제한 적용)
  - DML     → 사용자 명시적 승인 후 실행
  - DDL     → 사용자 명시적 승인 후 실행
  - UNKNOWN → 차단

안전장치:
  - sqlglot AST 전체 탐색으로 서브쿼리 내 DML/DDL 이중 차단
  - 행 수 제한 자동 적용 (Oracle ROWNUM / PG LIMIT)
  - 실행 이력 SQLite 로깅
```

**주요 함수**:
| 함수 | 설명 |
|------|------|
| `classify_sql(sql, db_type)` | SQL 유형 분류 (SELECT/DML/DDL/UNKNOWN) |
| `is_safe_to_autorun(sql, db_type)` | 자동 실행 안전 여부 확인 |
| `execute_query(sql, config_path, row_limit)` | SELECT 안전 실행 |
| `execute_dml(sql, config_path)` | DML/DDL 실행 (승인 필수) |
| `diagnose_failure(validation_name, result, ...)` | AI 기반 검증 실패 진단 |

---

### 2.9 aetl_designer.py — DW Star Schema 설계 엔진

**상태**: 구현 완료

API 문서(Swagger/OpenAPI) 또는 텍스트를 분석하여 3-Layer DW 모델을 자동 설계합니다.

```
입력 전략 (우선순위):
  1순위: Swagger/OpenAPI JSON/YAML → 완전 자동 파싱
  2순위: Excel/CSV 테이블 정의서  → 기존 parser 재사용
  3순위: PDF 텍스트              → AI 초안만 (사용자 검토 필수)

출력:
  - Star Schema 설계 JSON (ODS/FACT/DIM/DM 테이블)
  - Mermaid erDiagram 코드
  - Mermaid flowchart (ODS → DW → DM 흐름도)
  - DDL Script (aetl_export.generate_ddl 활용)
```

**주요 함수**:
| 함수 | 설명 |
|------|------|
| `parse_swagger(content)` | Swagger/OpenAPI → 엔티티 구조 추출 |
| `parse_table_definition_text(text)` | 텍스트 → AI 기반 테이블 정의 추출 |
| `design_star_schema(entities, context)` | 엔티티 → 3-Layer DW 설계 |
| `generate_mermaid_erd(design, layer)` | 설계 → Mermaid erDiagram |
| `generate_mermaid_flow(design)` | 설계 → Mermaid flowchart |
| `design_to_ddl(design, db_type)` | 설계 → DDL Script |

---

### 2.10 aetl_lineage.py — SQL 리니지 추적 엔진

**상태**: 구현 완료

sqlglot(규칙 기반 파서)로 SQL에서 리니지를 추출하고, LLM은 결과 설명만 담당합니다.

```
역할 분리:
  - sqlglot     : SQL → AST → 테이블/컬럼 리니지 추출 (100% 결정론적)
  - NetworkX    : DAG 구성, Forward/Backward 영향도 탐색
  - LLM         : 리니지 결과를 한국어로 설명 (파싱은 절대 안 맡김)
  - Mermaid     : Streamlit 내 시각화 코드 생성
```

**주요 함수**:
| 함수 | 설명 |
|------|------|
| `parse_lineage(sql, db_type)` | SQL → 리니지 추출 (sqlglot) |
| `build_lineage_graph(lineage)` | 리니지 → NetworkX DAG |
| `get_impact(G, node, direction)` | Forward/Backward 영향도 탐색 |
| `generate_mermaid_lineage(lineage)` | 컬럼 레벨 Mermaid flowchart |
| `generate_mermaid_table_lineage(lineage)` | 테이블 레벨 Mermaid flowchart |
| `explain_lineage(lineage)` | LLM으로 한국어 설명 생성 |

---

### 2.11 aetl_export.py — 산출물 자동 생성 엔진

**상태**: 구현 완료

매핑정의서, DDL, 검증 리포트를 Excel/JSON/CSV로 자동 생성합니다.

```
출력 형식:
  1. 매핑정의서 Excel (6개 시트)
     - 개요 / 소스 테이블 / 타겟 테이블 / 컬럼 매핑 / 적재 SQL / 검증 SQL
  2. DDL Script (Oracle/MariaDB/PostgreSQL 방언 지원)
  3. MERGE/UPSERT SQL (Oracle MERGE / MariaDB ON DUPLICATE KEY / PG ON CONFLICT)
  4. 검증 리포트 Excel (요약 / 상세 결과 / 실행 SQL)
  5. JSON Raw Export
  6. CSV 컬럼 매핑 Export
```

**주요 함수**:
| 함수 | 설명 |
|------|------|
| `generate_mapping_excel(source_meta, target_meta, ...)` | 매핑정의서 Excel 생성 |
| `generate_ddl(table_meta, db_type)` | CREATE TABLE DDL 생성 |
| `generate_merge_sql(source_meta, target_meta, ...)` | MERGE/UPSERT SQL 생성 |
| `generate_validation_report(run_results, ...)` | 검증 리포트 Excel 생성 |
| `generate_mapping_json(...)` | 매핑 JSON Export |
| `generate_mapping_csv(...)` | 매핑 CSV Export |

---

### 2.12 etl_sql_generator.py — 검증 SQL 자동 생성

**상태**: 구현 완료

소스/타겟 메타데이터 기반으로 검증 쿼리를 자동 생성합니다.

```
생성하는 검증 쿼리 (6종):
  1. row_count_check   — 건수 비교
  2. pk_missing_check  — PK 누락 검증
  3. null_check        — 주요 컬럼 NULL 체크
  4. duplicate_check   — 타겟 PK 중복 검증
  5. checksum_check    — 체크섬 비교
  6. full_diff_check   — 전체 데이터 불일치 확인

생성 방식:
  - 규칙 기반 (generate_validation_queries_no_llm): DB 방언별 SQL 직접 조립
  - LLM 기반 (generate_validation_queries): 프롬프트로 검증 SQL 생성
  - 규칙 제안 (suggest_validation_rules): 프로파일 통계 기반 자동 규칙 생성

DB 방언 지원:
  - Oracle  : NVL, STANDARD_HASH
  - MariaDB : IFNULL, CRC32
  - PostgreSQL : COALESCE, MD5
```

---

### 2.13 etl_metadata_parser.py — Excel/CSV 메타데이터 파서

**상태**: 구현 완료

Excel/CSV 테이블 정의서를 파싱하여 구조화된 메타데이터를 반환합니다.

```
지원 형식:
  - Excel (.xlsx, .xls): 단일/멀티 시트
  - CSV (.csv)

자동 감지 기능:
  - 컬럼명 후보 (한글/영문 양방향): table_name↔테이블명, column_name↔컬럼명 등
  - 소스/타겟 시트 자동 감지: source/소스/src, target/타겟/tgt 등
  - 매핑 시트 자동 감지: mapping/매핑/컬럼매핑 등
```

**주요 함수**:
| 함수 | 설명 |
|------|------|
| `parse_table_file(file, file_name)` | Excel/CSV → 메타데이터 |
| `parse_source_target_file(file, file_name)` | 소스+타겟 시트 자동 파싱 |
| `parse_mapping_file(file, file_name)` | 매핑 시트 파싱 |
| `parse_mapping_definition_excel(file, file_name)` | 전체 매핑정의서 파싱 |
| `schema_to_metadata(tables_dict, table_name)` | 스키마 딕셔너리 → 메타데이터 |

---

### 2.14 aetl_template_profile.py — 사용자 정의 엑셀 양식 프로파일

**상태**: 구현 완료

사용자 정의 엑셀 양식을 한 번 학습하고, 이후 자동으로 매핑 데이터를 기입합니다.

```
동작 방식:
  1. [등록] 빈 양식 업로드 → 헤더 자동 분석 → UI에서 확인/수정
  2. [저장] 헤더↔필드 매핑을 profile.json + 원본 템플릿으로 저장
  3. [적용] profile 로드 → 원본 양식에 데이터 write-back → 다운로드

원칙:
  - AI는 초안(헤더 후보 제안)만 담당, 사람이 최종 확정
  - 확정 후 100% 결정론적 동작 (LLM 없음)
  - 원본 양식 파일을 그대로 사용
```

---

## 3. 아키텍처 통합도

### 3.1 엔진 간 연동 흐름

```
┌──────────────────────────────────────────────────────────────┐
│                     AETL v2 통합 아키텍처                       │
│                                                                │
│   ┌─────────────────────────────────────────────────────────┐ │
│   │                     사용자 인터페이스                       │ │
│   │    etl_streamlit_app.py  /  streamlit_app.py              │ │
│   └────────────────────────────┬──────────────────────────────┘ │
│                                │                                │
│   ┌────────────────────────────▼──────────────────────────────┐ │
│   │                 LLM 프로바이더 (aetl_llm.py)                │ │
│   │          Gemini / Claude / OpenAI  (LLM_PROVIDER 선택)      │ │
│   └────────────────────────────┬──────────────────────────────┘ │
│                                │                                │
│   ┌────────────────────────────▼──────────────────────────────┐ │
│   │               에이전트 / 엔진 레이어                        │ │
│   │                                                            │ │
│   │  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐  │ │
│   │  │ aetl_agent   │   │   app.py     │   │ aetl_designer│  │ │
│   │  │ (Tool-call   │   │ (SQL 챗봇    │   │ (DW 설계     │  │ │
│   │  │  Agent)      │   │  파이프라인)  │   │  자동 추천)  │  │ │
│   │  └──────┬───────┘   └──────────────┘   └──────────────┘  │ │
│   │         │                                                  │ │
│   │  ┌──────▼───────┐   ┌──────────────┐   ┌──────────────┐  │ │
│   │  │ aetl_executor│   │ aetl_lineage │   │ aetl_export  │  │ │
│   │  │ (SQL 실행    │   │ (리니지 추적)│   │ (산출물 생성)│  │ │
│   │  │  + AI 진단)  │   │              │   │              │  │ │
│   │  └──────────────┘   └──────────────┘   └──────────────┘  │ │
│   │                                                            │ │
│   │  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐  │ │
│   │  │ aetl_profiler│   │etl_sql_gen   │   │etl_meta_     │  │ │
│   │  │ (프로파일링) │   │(검증SQL 생성)│   │parser        │  │ │
│   │  └──────────────┘   └──────────────┘   └──────────────┘  │ │
│   └────────────────────────────┬──────────────────────────────┘ │
│                                │                                │
│   ┌────────────────────────────▼──────────────────────────────┐ │
│   │                  데이터 레이어                               │ │
│   │                                                            │ │
│   │  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐  │ │
│   │  │ db_schema.py │   │ aetl_meta_   │   │ aetl_store   │  │ │
│   │  │ (스키마 조회  │   │ engine.py    │   │ (검증 규칙   │  │ │
│   │  │  + 캐시)     │   │ (메타데이터  │   │  + 이력)     │  │ │
│   │  │              │   │  SQLite 캐시) │   │              │  │ │
│   │  └──────┬───────┘   └──────────────┘   └──────────────┘  │ │
│   └─────────┼──────────────────────────────────────────────────┘ │
│             │                                                    │
│   ┌─────────▼────────────────────────────────────────────────┐   │
│   │                    DB 드라이버 레이어                       │   │
│   │         Oracle (oracledb) │ MariaDB │ PostgreSQL (psycopg2) │   │
│   └──────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
```

### 3.2 데이터 흐름

```
[사용자 자연어 요청]
    │
    ▼
[aetl_agent.py]  ←─── LLM Tool-calling (aetl_llm.py)
    │
    ├── get_table_schema ──→ [aetl_metadata_engine] → [db_schema.py] → DB
    ├── search_tables ─────→ [aetl_metadata_engine] → [db_schema.py] → DB
    ├── profile_table ─────→ [aetl_metadata_engine] → [aetl_profiler.py] → DB
    ├── generate_validation → [etl_sql_generator.py] (규칙 기반)
    ├── suggest_rules ─────→ [etl_sql_generator.py] + [aetl_profiler.py]
    ├── compare_row_counts → DB 직접 조회
    └── sync_metadata ────→ [aetl_metadata_engine] → DB → SQLite
    │
    ▼
[응답: 스키마 정보 / 프로파일 / 검증 SQL / 규칙 제안 / 건수 비교 결과]
```

---

## 4. 기술 스택

| 구분 | 기술 | 용도 |
|------|------|------|
| **LLM** | Gemini 2.5 Flash / Claude Sonnet / GPT-4o-mini | 자연어 처리, SQL 생성, 진단 |
| **에이전트** | LangChain + LangGraph | Tool-calling 에이전트 파이프라인 |
| **SQL 파서** | sqlglot | SQL 분류, 리니지 추출, 안전 검사 |
| **그래프** | NetworkX | 리니지 DAG, 영향도 분석 |
| **UI** | Streamlit | 웹 대시보드 |
| **DB 드라이버** | oracledb / mariadb / psycopg2 | 멀티 DB 연결 |
| **엑셀** | openpyxl / pandas | 매핑정의서, 검증 리포트 생성 |
| **저장소** | SQLite | 메타데이터, 검증 규칙/이력 영속 저장 |
| **환경** | python-dotenv | API 키, DB 비밀번호 관리 |

---

## 5. 환경 설정

### 5.1 .env

```env
# LLM 선택 (gemini | claude | openai, 미설정 시 자동 시도)
LLM_PROVIDER=claude

# API 키
GOOGLE_API_KEY=...
ANTHROPIC_API_KEY=...
OPENAI_API_KEY=...

# DB 비밀번호
DB_PASSWORD=...
```

### 5.2 db_config.json

```json
{
    "db_type": "postgresql",
    "connection": {
        "host": "...",
        "port": 6543,
        "user": "...",
        "password": "${DB_PASSWORD}",
        "database": "postgres"
    },
    "schema_options": {
        "owner": "public",
        "include_tables": [],
        "exclude_tables": [],
        "include_views": true
    },
    "cache": {
        "enabled": true,
        "ttl_seconds": 3600
    }
}
```

---

## 6. 구현 완료 / 미완 현황

### 완료 항목

| 항목 | 모듈 | 상태 |
|------|------|------|
| LLM 프로바이더 통합 (Gemini/Claude/OpenAI) | aetl_llm.py | 완료 |
| LLM_PROVIDER 엄격 모드 (지정 시 fallback 금지) | aetl_llm.py | 완료 |
| LangGraph Tool-calling 에이전트 | aetl_agent.py | 완료 |
| 자연어 → SQL 변환 파이프라인 | app.py | 완료 |
| Oracle / MariaDB / PostgreSQL 스키마 조회 | db_schema.py | 완료 |
| PostgreSQL 다중 스키마 지원 | db_schema.py | 완료 |
| 스키마 캐싱 + 옵션 변경 자동 무효화 | db_schema.py | 완료 |
| 데이터 프로파일링 (3 DB) | aetl_profiler.py | 완료 |
| 메타데이터 사전 수집 엔진 | aetl_metadata_engine.py | 완료 |
| 검증 규칙/이력 관리 저장소 | aetl_store.py | 완료 |
| 검증 SQL 자동 생성 (6종, 규칙/LLM) | etl_sql_generator.py | 완료 |
| 프로파일 기반 검증 규칙 자동 제안 | etl_sql_generator.py | 완료 |
| SQL 실행 엔진 (SELECT 자동/DML 승인) | aetl_executor.py | 완료 |
| sqlglot 기반 SQL 분류 + 서브쿼리 이중 차단 | aetl_executor.py | 완료 |
| AI 기반 검증 실패 진단 + 수정 SQL 제안 | aetl_executor.py | 완료 |
| SQL 리니지 추적 (sqlglot + NetworkX) | aetl_lineage.py | 완료 |
| Mermaid 리니지 시각화 (컬럼/테이블 레벨) | aetl_lineage.py | 완료 |
| 매핑정의서 Excel 자동 생성 (6시트) | aetl_export.py | 완료 |
| DDL 생성 (Oracle/MariaDB/PostgreSQL) | aetl_export.py | 완료 |
| MERGE/UPSERT SQL 생성 (3 DB) | aetl_export.py | 완료 |
| 검증 리포트 Excel 생성 | aetl_export.py | 완료 |
| Swagger/OpenAPI → DW 설계 자동 추천 | aetl_designer.py | 완료 |
| Star Schema ERD (Mermaid erDiagram) | aetl_designer.py | 완료 |
| ODS→DW→DM 흐름도 (Mermaid flowchart) | aetl_designer.py | 완료 |
| Excel/CSV 메타데이터 파서 (한글/영문) | etl_metadata_parser.py | 완료 |
| 사용자 정의 엑셀 양식 프로파일 | aetl_template_profile.py | 완료 |
| 실행 이력 SQLite 로깅 | aetl_executor.py | 완료 |

### 미완 / 개선 필요 항목

| 항목 | 현재 상태 | 비고 |
|------|-----------|------|
| app.py의 aetl_llm.py 마이그레이션 | 미완 | 하드코딩된 ChatGoogleGenerativeAI 사용 중 |
| Streamlit UI 고도화 | 기본 구현 | 리니지 그래프, 검증 대시보드 고도화 필요 |
| 사용자 권한 기반 접근 제어 | 미구현 | 실행 감사 로깅만 구현됨 |
| 날짜 자연어 파싱 고도화 | 기본 수준 | "지난 달", "올해" 등 상대 날짜 미지원 |
| DB 계정 권한 분리 (검증/테스트 분리) | 미구현 | 운영 정책 레벨 |
| ERD 인터랙티브 시각화 | Mermaid만 | React Flow 등은 별도 프론트엔드 필요 |
| 모니터링 대시보드 | 미구현 | Job 상태, AI Troubleshooter |

---

## 7. 로드맵 (향후 계획)

### Phase 1: 안정화 (현재)
- [x] LLM 프로바이더 통합 및 엄격 모드
- [x] PostgreSQL 다중 스키마 + 캐시 무효화
- [ ] app.py → aetl_llm.py 마이그레이션
- [ ] 전체 모듈 통합 테스트

### Phase 2: UI 고도화
- [ ] Streamlit 리니지 그래프 인터랙티브 뷰
- [ ] 검증 결과 대시보드 (Pass/Fail 추이, 히트맵)
- [ ] DW 설계 UI (st.data_editor 기반 편집)

### Phase 3: 운영 기능
- [ ] 사용자 권한 기반 접근 제어
- [ ] 실행 감사 로깅 강화
- [ ] DB 계정 권한 분리 (검증/테스트)
- [ ] 모니터링 대시보드 (Job 상태)

### Phase 4: 확장
- [ ] 별도 프론트엔드 (React) + REST API
- [ ] 인터랙티브 ERD 시각화 (React Flow)
- [ ] 스케줄러 연동 (Airflow 등)
- [ ] 추가 DB 지원 (MSSQL, BigQuery)

---

## 8. 핵심 사용 시나리오

### 시나리오 1: 에이전트 대화형 ETL 검증

```
[User]  "EMPLOYEE 테이블의 스키마를 보여줘"
[Agent] get_table_schema("EMPLOYEE") 호출 → 컬럼/타입/PK 정보 출력

[User]  "EMPLOYEE와 DEPARTMENT 간 건수를 비교해줘"
[Agent] compare_row_counts("EMPLOYEE", "DEPARTMENT") 호출 → PASS/FAIL 결과

[User]  "두 테이블의 검증 쿼리를 만들어줘"
[Agent] generate_validation_queries_tool("EMPLOYEE", "DEPARTMENT") 호출 → 6종 SQL
```

### 시나리오 2: API 문서 → DW 설계

```
[Step 1] Swagger JSON 업로드
         → parse_swagger() → 엔티티 구조 추출

[Step 2] design_star_schema() → ODS/FACT/DIM/DM 설계 JSON

[Step 3] generate_mermaid_erd() → Star Schema ERD 시각화
         generate_mermaid_flow() → ODS→DW→DM 흐름도

[Step 4] design_to_ddl() → CREATE TABLE DDL 전체 생성
```

### 시나리오 3: 매핑정의서 + 검증 자동화

```
[Step 1] Excel 정의서 업로드
         → parse_mapping_definition_excel() → 소스/타겟/매핑 메타데이터

[Step 2] generate_mapping_excel() → 6시트 매핑정의서 Excel 다운로드

[Step 3] generate_validation_queries_no_llm() → 검증 SQL 6종 생성

[Step 4] execute_query() → SELECT 검증 실행 → 결과 분석

[Step 5] diagnose_failure() → AI 진단 + 수정 SQL 제안

[Step 6] generate_validation_report() → 검증 리포트 Excel 다운로드
```

---

*AETL v2 — 생성(Generate)하고, 실행(Execute)하고, 산출물(Deliver)까지*
