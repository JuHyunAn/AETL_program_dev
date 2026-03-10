# AETL — AI-driven Sub ETL Tool

Oracle / MariaDB / PostgreSQL 대상 데이터 정합성 검증 및 매핑 자동화 도구.
LangGraph Tool-calling 에이전트와 규칙 기반 엔진을 결합하여 ETL 검증·산출물 생성을 자동화합니다.

---

## 실행

```bash
streamlit run etl_streamlit_app.py
# → http://localhost:8501
```

React 컴포넌트 수정 시 빌드 필요:
```bash
cd etl_flow_component/frontend && npm run build   # ETL Lineage Flow Map
cd erd_flow_component/frontend && npm run build   # DW 설계 ERD
```

---

## 기술 스택

| 구분 | 기술 |
|------|------|
| LLM | Gemini 2.5 Flash / Claude Sonnet 4.5 / GPT-4o-mini |
| 에이전트 | LangChain + LangGraph (Tool-calling) |
| SQL 파서 | sqlglot (분류, 리니지, 안전 검사) |
| UI | Streamlit (7페이지) + React (Flow Map / ERD) |
| DB | Oracle (oracledb) / MariaDB (mariadb) / PostgreSQL (psycopg2) |
| 저장소 | SQLite (.aetl_metadata.db, aetl_metadata.db) |
| 산출물 | openpyxl / pandas |

---

## 프로젝트 구조

```
AETL_program_dev/
├── etl_streamlit_app.py       # 메인 웹 UI (진입점)
│
├── aetl_llm.py                # LLM 프로바이더 통합 (Gemini/Claude/OpenAI fallback)
├── aetl_agent.py              # LangGraph Tool-calling 에이전트 (도구 7개)
│
├── db_schema.py               # DB 스키마 조회 + 캐시 (.schema_cache.json)
├── db_config.json             # DB 연결 설정
├── aetl_metadata_engine.py    # 메타데이터 사전 수집 (.aetl_metadata.db)
├── aetl_store.py              # 검증 규칙/이력 저장 (aetl_metadata.db)
│
├── aetl_profiler.py           # 데이터 프로파일링
├── aetl_executor.py           # SQL 실행 엔진 (SELECT 자동 / DML 승인)
├── aetl_export.py             # 산출물 생성 (Excel / DDL / MERGE SQL)
├── aetl_designer.py           # DW Star Schema 설계
├── aetl_lineage.py            # SQL 리니지 추적 (sqlglot + NetworkX)
├── aetl_template_profile.py   # 사용자 정의 엑셀 양식 프로파일
│
├── etl_sql_generator.py       # 검증 SQL 6종 자동 생성
├── etl_metadata_parser.py     # Excel/CSV 테이블 정의서 파서
│
├── etl_flow_component/        # ETL Lineage Flow Map (React 커스텀 컴포넌트)
├── erd_flow_component/        # DW 설계 ERD (React 커스텀 컴포넌트)
│
├── streamlit_app.py           # [레거시] 폐기 예정
├── .env                       # API 키 및 DB 비밀번호
└── documents/architecture/    # 설계 문서 (loadmap.md 등)
```

---

## 폴더/파일 경로로 역할 파악

- **실제 프로젝트 루트 경로(현재 개발 환경)**: `C:\Users\안주현\Desktop\AETL_program_dev`  
  - 아래 경로는 모두 이 루트 기준 상대 경로입니다.

다른 Agent 세션이나 신규 참여자가 **폴더·파일 경로만 보고** 모듈 역할을 파악할 수 있도록 경로별 역할을 정리합니다.

| 경로 (폴더/파일) | 역할 |
|------------------|------|
| `etl_streamlit_app.py` | 메인 웹 진입점. Streamlit 7페이지(검증/프로파일/라인리지/DW설계/산출물 등) |
| `aetl_*.py` | AETL 코어: `aetl_llm` LLM연동, `aetl_agent` 에이전트, `aetl_executor` SQL실행, `aetl_store` 저장, `aetl_profiler` 프로파일링, `aetl_export` 산출물, `aetl_designer` DW설계, `aetl_lineage` 리니지, `aetl_metadata_engine` 메타수집, `aetl_template_profile` 엑셀 프로파일 |
| `etl_*.py` | ETL 보조: `etl_sql_generator` 검증 SQL 생성, `etl_metadata_parser` 테이블 정의서(Excel/CSV) 파서 |
| `db_schema.py` | DB 스키마 조회·캐시. `db_config.json` 경로 기준 동작 |
| `db_config.json` | DB 연결 설정(단일). `config_path`로 다른 모듈에서 참조 |
| `etl_flow_component/` | ETL 리니지 플로우 맵 UI. `frontend/` = React 빌드, `frontend/build` = Streamlit 임베드 경로 |
| `erd_flow_component/` | DW 설계 ERD UI. 구조는 `etl_flow_component`와 동일 |
| `documents/architecture/` | 설계 문서. `Plan_ETL.md`, `loadmap.md`, `schema_doc.md` 등 |
| `documents/sample/` | 샘플 파일 (swagger, txt 등) |
| `.template_profiles/` | `aetl_template_profile` 사용자 정의 엑셀 프로파일 저장 디렉터리 (루트 기준) |
| `*.db` (루트) | SQLite: `aetl_metadata.db`(검증/이력), `.aetl_metadata.db`(메타엔진). 경로는 각 모듈 내 상수 |
| `.schema_cache.json` | `db_schema` 스키마 캐시. `db_config.json`과 같은 디렉터리 |

- **설계/로드맵**: `documents/architecture/` + **Plan_ETL.md**(ETL 플랫폼 장기 계획) 참고.
- **config_path**: 대부분 `db_config.json` 절대/상대 경로. 실행 시 워킹 디렉터리는 프로젝트 루트 가정.

---

## 환경변수 (.env)

```env
# LLM — 하나 이상 설정 (미설정 시 gemini → claude → openai 순 자동 시도)
GOOGLE_API_KEY=...
ANTHROPIC_API_KEY=...
OPENAI_API_KEY=...

# LLM 고정 지정 (선택)
LLM_PROVIDER=gemini   # gemini | claude | openai

# DB 비밀번호 (db_config.json에서 ${DB_PASSWORD}로 참조)
DB_PASSWORD=...
```

---

## DB 연결 설정 (db_config.json)

```json
{
    "db_type": "postgresql",
    "connection": {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "${DB_PASSWORD}",
        "database": "postgres"
    },
    "schema_options": {
        "owner": null,
        "include_tables": [],
        "exclude_tables": [],
        "include_views": false
    },
    "cache": {
        "enabled": true,
        "ttl_seconds": 3600
    }
}
```

- `db_type`: `oracle` | `mariadb` | `postgresql`
- `owner`: PostgreSQL에서 스키마 지정 (쉼표 구분 또는 LIKE 패턴 지원, `null`이면 전체)
- 스키마 캐시 강제 갱신: `python db_schema.py --refresh`

---

## 설계 원칙

- **규칙 기반 우선**: sqlglot AST 기반 SQL 분류/파싱 — LLM은 설명·초안만 담당
- **Human-in-the-Loop**: DML/DDL은 사용자 명시적 승인 후 실행
- **Single Source of Truth**: 매핑 테이블 하나로 Excel/DDL/MERGE SQL/리포트 동시 생성
- **멀티 DB**: Oracle / MariaDB / PostgreSQL 방언별 SQL 자동 생성

---

## 문의

WI사업부 안주현
