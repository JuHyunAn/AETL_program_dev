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
