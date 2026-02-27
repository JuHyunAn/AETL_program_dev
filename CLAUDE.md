# AETL Agent

Oracle 또는 MariaDB, PostgresSQL등으로 변환하는 SUB ETL Tool입니다.

상용 LLM과 LangGraph를 활용하여 ETL 작업을 자동화합니다.

---

## 주요 기능

- **자연어 → SQL 변환**: 한국어 질문을 Oracle SQL 쿼리로 자동 변환
- **ETL 작업 자동화**: 사용자의 요구사항을 분석하여 ETL 작업을 자동화합니다.
- **다중 턴 대화**: 기간, 날짜 등의 정보를 대화 중에 수집하여 컨텍스트 유지
- **보안 정책 적용**: DML/DDL 차단, PII 보호, 행 수 제한 등
- **자동 검증 및 복구**: 생성된 SQL을 검증하고, 문제가 있으면 자동으로 수정 시도
- **스키마 캐싱**: DB 스키마를 캐싱하여 빠른 응답 속도
- **스키마 동적 로딩**: DB 연결 설정 파일에 따라 동적으로 스키마 로딩
- **Lineage 추적**: 테이블 간의 관계를 추적하여 데이터의 흐름을 파악

---

## 기술 스택

| 구분 | 기술 |
|------|------|
| LLM | Google Gemini 2.5 Flash, Claude 3.5 Sonnet |
| 프레임워크 | LangChain + LangGraph |
| UI | Streamlit, React |
| 언어 | Python 3.10+ |
| 대상 DB | Oracle / MariaDB / PostgreSQL (동적 스키마 로딩 지원) |

---

## 프로젝트 구조

```
AETL_program_dev/
├── app.py                  # 메인 서버 로직 (FastAPI/LangGraph)
├── streamlit_app.py        # 기본 웹 UI (Streamlit)
├── aetl_agent.py           # 핵심 에이전트 로직 (LangGraph 파이프라인)
├── aetl_designer.py        # DW/스키마 디자인 로직
├── aetl_executor.py        # SQL 실행 및 검증 로직
├── aetl_export.py          # 엑셀 산출물 자동화 로직
├── aetl_lineage.py         # 데이터 흐름(Lineage) 시각화 로직
├── aetl_profiler.py        # 데이터 프로파일링 및 통계 수집
├── aetl_store.py           # 실행 이력 및 상태 관리
├── aetl_template_profile.py # 템플릿 기반 프로파일링 관리
├── db_schema.py            # DB 스키마 조회 (Oracle/MariaDB/PostgreSQL)
├── db_config.json          # 데이터베이스 연결 설정
├── .env                    # 환경변수 (API 키, DB 접속 정보)
├── .schema_cache.json      # 스키마 캐시 파일 (자동 생성)
├── CLAUDE.md               # 프로젝트 가이드 (현재 파일)
└── documents/              # 아키텍처 및 상세 설계 문서
```

---

## 설치 방법

### 1. 필수 라이브러리 설치

```bash
pip install streamlit langchain langchain-google-genai langgraph python-dotenv
```

### 2. DB 드라이버 설치 (선택)

실제 DB에서 스키마를 동적으로 가져오려면 해당 DB 드라이버를 설치합니다:

```bash
# Oracle 사용 시
pip install oracledb

# MariaDB/MySQL 사용 시
pip install mariadb
```

> **참고**: DB 드라이버가 없어도 기본 스키마(DEFAULT_SCHEMA)로 동작합니다.

### 3. 환경변수 설정

프로젝트 폴더에 `.env` 파일을 생성하고 필요한 키를 입력합니다:

```env
GOOGLE_API_KEY=your_google_api_key_here
DB_PASSWORD=your_db_password_here
```

> **참고**: 
Google API 키는 [Google AI Studio](https://aistudio.google.com/)에서 발급받을 수 있습니다.

Claude API 키는 [Claude](https://claude.com/)에서 발급받을 수 있습니다.

---

## DB 연결 설정 (선택)

실제 DB에서 스키마를 동적으로 가져오려면 `db_config.json` 파일을 설정합니다.

### Oracle 설정 예시

```json
{
    "db_type": "oracle",
    "connection": {
        "host": "localhost",
        "port": 1521,
        "user": "your_username",
        "password": "${DB_PASSWORD}",
        "database": "your_service_name"
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

### MariaDB/MySQL 설정 예시

```json
{
    "db_type": "mariadb",
    "connection": {
        "host": "localhost",
        "port": 3306,
        "user": "your_username",
        "password": "${DB_PASSWORD}",
        "database": "your_database"
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

### 설정 항목 설명

| 항목 | 설명 |
|------|------|
| `db_type` | `"oracle"` 또는 `"mariadb"` |
| `host` | DB 서버 호스트 |
| `port` | 포트 (Oracle: 1521, MariaDB: 3306) |
| `user` | DB 접속 계정 |
| `password` | 비밀번호 (`${DB_PASSWORD}` 형태로 환경변수 참조 가능) |
| `database` | Oracle: service_name/SID, MariaDB: 데이터베이스명 |
| `owner` | `null`이면 접속 계정 소유 객체만, 지정 시 해당 스키마 조회 |
| `include_tables` | 빈 배열이면 전체, 지정 시 해당 테이블만 |
| `exclude_tables` | 제외할 테이블 패턴 (예: `["TEMP_%", "LOG_%"]`) |
| `include_views` | 뷰 포함 여부 |
| `cache.enabled` | 스키마 캐싱 활성화 |
| `cache.ttl_seconds` | 캐시 유효 시간 (초) |

### 스키마 조회 테스트

```bash
# 캐시 사용
python db_schema.py

# 캐시 무시하고 DB에서 새로 조회
python db_schema.py --refresh
```

> **참고**: `db_config.json`이 없거나 DB 연결에 실패하면 자동으로 기본 스키마(DEFAULT_SCHEMA)를 사용합니다.

---

## 실행 방법

### 웹 UI 실행 (권장)

```bash
# 방법 1
streamlit run etl_streamlit_app.py

# 방법 2
python -m streamlit run etl_streamlit_app.py
```

실행 후 브라우저에서 `http://localhost:8501` 접속

### 콘솔 테스트 실행

```bash
python app.py
```

---

## 사용 예시

### 예시 1: 부서별 인원 조회

```
사용자: 부서별 인원 수를 알려줘
챗봇: [SQL 생성]
```

### 예시 2: 기간이 필요한 질문

```
사용자: 실적이 가장 좋은 부서를 알려줘
챗봇: 추가 확인이 필요합니다:
      - 기간이 필요합니다. 예: 2025-01-01 ~ 2025-01-31

사용자: 2025-01-01 ~ 2025-01-31
챗봇: [SQL 생성]
```

### 예시 3: 직원 정보 조회

```
사용자: 연봉이 가장 높은 직원 5명 알려줘
챗봇: [SQL 생성]
```

---

## 프로세스 파이프라인

```
사용자 질문
    │
    ▼
┌─────────────────┐
│ 1. clarify_node │  ← 추가 정보 필요 여부 확인
└────────┬────────┘
         │
         ▼
┌─────────────────────┐
│ 2. parse_intent_node│  ← 의도 파악 (LIST, AGG, TOP 등)
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│ 3. build_plan_node  │  ← 쿼리 계획 수립 (JSON)
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│ 4. generate_sql_node│  ← Oracle SQL 생성
└────────┬────────────┘
         │
         ▼
┌─────────────────────┐
│ 5. validate_sql_node│  ← 보안 정책 검증
└────────┬────────────┘
         │
    검증 실패 시
         │
         ▼
┌─────────────────────┐
│ 6. repair_node      │  ← 최대 2회 재시도
└─────────────────────┘
```

---

## 보안 정책

| 정책 | 설명 |
|------|------|
| DML/DDL 금지 | INSERT, UPDATE, DELETE, DROP 등 차단 |
| SELECT * 금지 | 명시적 컬럼 지정 필수 |
| 행 수 제한 | 최대 10,000건으로 제한 |
| PII 보호 | 주민번호 뒷자리(RRN_BACK) 직접 조회 금지 |
| 스키마 검증 | 정의되지 않은 테이블/컬럼 사용 금지 |

---

## 테스트 데이터베이스 스키마

현재 MVP 버전에서는 다음 테이블을 지원합니다:

### DEPARTMENT(부서)
| 컬럼 | 설명 |
|------|------|
| DEPT_ID | 부서 ID (PK) |
| DEPT_NAME | 부서명 |
| CREATED_AT | 생성일 |

### EMPLOYEE(사원)
| 컬럼 | 설명 |
|------|------|
| EMP_ID | 사원 ID (PK) |
| EMP_NAME | 사원명 |
| DEPT_ID | 부서 ID (FK) |
| BIRTH_DATE | 생년월일 |
| GENDER_CD | 성별 코드 |
| PHONE_NO | 전화번호 |
| HIRE_DATE | 입사일 |
| SALARY_ANNUAL | 연봉 |
| RRN_FRONT | 주민번호 앞자리 |
| RRN_BACK | 주민번호 뒷자리 (조회 금지) |

### DEPT_SALES_DAILY(부서별 일일 매출)
| 컬럼 | 설명 |
|------|------|
| SALES_DATE | 매출일 (PK) |
| DEPT_ID | 부서 ID (PK, FK) |
| REVENUE_AMT | 매출액 |

---

## 파일별 역할

### app.py / aetl_agent.py (에이전트 시스템)
- LangGraph 기반의 ETL 워크플로우 상태 머신 구현
- 자연어 의도 파악, 계획 수립, SQL 생성 및 검증 파이프라인

### aetl_executor.py / aetl_store.py (실행 및 이력 관리)
- 생성된 SQL의 실제 DB 실행 (SELECT/DML)
- 실행 이력 저장 및 검증 결과(Pass/Fail) 관리

### aetl_export.py / aetl_designer.py (산출물 및 설계)
- 매핑정의서 및 검증 리포트 엑셀 자동 생성
- API 문서(Swagger 등) 기반 Star Schema 설계 및 ERD 시각화 데이터 생성

### aetl_lineage.py / aetl_profiler.py (분석 및 흐름)
- 테이블/컬럼 간 데이터 흐름(Lineage) 추적 및 시각화
- 데이터 프로파일링(통계, 분포) 수행 및 결과 제공

### db_schema.py / db_config.json (데이터베이스 레이어)
- Oracle/MariaDB/PostgreSQL 메타데이터 조회 및 캐싱
- 데이터베이스 연결 설정 및 보안 정책 관리

### etl_streamlit_app.py / streamlit_app.py (UI 레이어)
- 에이전트 채팅 인터페이스 및 결과 시각화
- 리니지 그래프 및 검증 리포트 대시보드 제공

---

## 향후 개발 계획

- [x] ~~동적 스키마 로딩 (DB 메타데이터 조회)~~ ✅ 완료
- [x] ~~Oracle / MariaDB / PostgreSQL 지원~~ ✅ 완료
- [x] ~~실제 DB 연결 후 쿼리 실행 및 검증 기능~~ ✅ 완료
- [x] ~~매핑정의서 및 산출물 자동 생성 (Excel)~~ ✅ 완료
- [/] 데이터 흐름(Lineage) 시각화 엔진 고도화
- [ ] 더 정교한 자연어 날짜 파싱 (예: "지난 달", "올해")
- [ ] 사용자 권한 기반 접근 제어 및 실행 감사 로깅

---

## 실행
streamlit run etl_streamlit_app.py

---

## 문의
WI사업부 안주현


## API KEY는 사용하는 LLM 제품의 KEY를 .env에 설정하여 활용하세요.