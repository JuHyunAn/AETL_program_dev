# [분석 및 발표 자료] AETL: BI 개발자를 위한 AI 기반 Sub ETL 솔루션

본 문서는 AETL(AI-driven Sub ETL) 프로그램의 코드와 설계 문서를 기반으로 한 분석 결과 및 발표용 PPT 초안입니다.

---

## 1. 프로그램 전체 분석 요약

AETL은 단순한 SQL 생성기를 넘어, **데이터 엔지니어링의 '마지막 1마일'(검증 및 문서화)을 자동화**하는 AI 에이전트 기반 솔루션입니다.

### 핵심 아키텍처 (3-Layer Engine)
1.  **Metadata & Profiling Engine**: 실제 DB(Oracle, MariaDB, PostgreSQL)와 연결하여 스키마와 데이터 통계를 SQLite 기반으로 캐싱하고 분석합니다.
2.  **AI Design & Generation Engine**: Swagger, PDF, Excel 등의 비정형 문서를 분석하여 DW Star Schema를 설계하고, 6종의 정합성 검증 SQL을 자동으로 생성합니다.
3.  **Safe Execution & Export Engine**: 생성된 SQL을 AST(추상 구문 트리) 기반으로 분류하여 안전하게 실행하고, 그 결과를 매핑정의서 및 검증 리포트(Excel)로 자동 산출합니다.

---

## 2. 발표 자료 (PPT) 구성 초안

> **이 내용은 발표 슬라이드 구성을 위해 작성되었습니다.**

### Slide 1: 타이틀
*   **제목**: AETL - AI-driven Sub ETL Solution
*   **부제**: BI 개발자를 위한 데이터 정합성 검증 및 매핑 자동화 어시스턴트
*   **핵심 슬로건**: "SQL을 생성하고, 실행하고, 결과물을 배달합니다."

### Slide 2: BI 개발자의 페인 포인트 (Problem)
*   **데이터 검증의 공백**: ETL 적재 후 데이터가 맞는지 확인하는 쿼리 작성이 수동으로 이루어짐 (Row Count, PK 중복 등).
*   **문서화의 지옥**: 매핑정의서, DDL, MERGE SQL 등을 수동으로 관리하며 코드와 문서 간 불일치 발생.
*   **복잡한 툴**: 상용 ETL 툴(Informatica, Talend 등)은 무겁고, 검증 쿼리 자동화 기능은 부족함.

### Slide 3: 핵심 기능 (Key Features)
*   **AI Copilot**: 자연어 대화로 스키마 조회부터 건수 비교까지 수행 (`aetl_agent`).
*   **자동 설계 (Designer)**: PDF/Swagger만 넣으면 Star Schema(Fact/Dim) 설계 및 DDL 생성.
*   **검증 SQL 자동 생성**: 소스-타겟 매핑 기반 6대 검증 쿼리(Row Count, Checksum, Full Diff 등) 즉시 생성.
*   **산출물 One-stop**: 매핑정의서(Excel), MERGE SQL, 검증 리포트를 버튼 하나로 추출.

### Slide 4: 사용 흐름 (Usage Flow)
1.  **연결/업로드**: DB를 직접 연결하거나 테이블 정의서(Excel)를 업로드합니다.
2.  **분석/프로파일**: AI가 데이터를 프로파일링하여 데이터의 특성 및 도메인을 파악합니다.
3.  **검증/설계**: 소스-타겟 매핑을 등록하고 검증 SQL을 생성하여 즉시 실행합니다.
4.  **산출**: 최종 확정된 매핑 정보를 기반으로 운영계에 적용할 DDL/DML 및 문서를 다운로드합니다.

---

## 3. BI 개발자를 위한 SUB ETL로서의 가치 분석

### 가치가 충분한 이유 (Value Proposition)
BI 개발자는 데이터를 '소비'하는 쪽에 가깝지만, 데이터의 '신뢰성'을 확보하기 위해 ETL 개발자와 끊임없이 소통해야 합니다. AETL은 이 소통의 비용을 줄여줍니다.
- **개발 생산성**: 반복적인 검증 SQL 작성 시간을 90% 이상 단축.
- **문서 신뢰성**: 코드가 곧 문서가 되는 구조(Single Source of Truth).
- **품질 확보**: 사람이 놓치기 쉬운 체크섬 비교나 전체 데이터 불일치(Full Diff)를 AI가 강제함.

---

## 4. 상용 소프트웨어(TOS, BTL 등)와의 비교

### AETL의 핵심 강점 (Strengths)
| 항목 | 상용 ETL (Talend, Informatica) | AETL (Sub ETL) |
| :--- | :--- | :--- |
| **목적** | 대량 데이터의 이동 및 변환 (Data Movement) | **데이터의 정합성 검증 및 설계 (Validation)** |
| **사용성** | 전문 교육 필요, 복잡한 GUI | **자연어 대화 및 단순 웹 UI (Low-code)** |
| **검증** | 수동 컴포넌트 조합 필요 | **6종 검증 SQL 자동 생성 및 AI 진단** |
| **산출물** | 문서 자동화 기능이 약하거나 별도 옵션 | **매핑정의서(Excel) 및 리포트 기본 제공** |
| **유연성** | 무거운 서버 환경 필수 | **가벼운 라이브러리 기반 (Python/Streamlit)** |

### 상용 소프트웨어를 여전히 활용해야 하는 이유 (Weaknesses)
- **High-Performance Data Movement**: 수억 건의 데이터를 메모리 효율적으로 관리하며 실제 이동시키는 성능은 상용 엔진(Informatica, Spark 등)이 압도적입니다.
- **Enterprise Governance**: 상세한 접근 권한 제어(RBAC), 감사 로그, 전사적 엔터프라이즈 리니지 관리는 상용 툴의 영역입니다.
- **Scheduling & Monitoring**: 복잡한 의존 관계를 가진 수천 개의 잡(Job) 스케줄링은 Airflow나 상용 스케줄러가 더 전문적입니다.

---

## 5. 최종 결론 및 substitution 분석

### 대체 가능한 기능인가? 별개의 프로그램인가?
AETL은 상용 ETL 툴의 **'단위 테스트(Unit Testing)' 및 '설계/문서화(Documentation)' 모듈을 대체**하고 있습니다.

- **실제 대체 중인 기능**:
    - Talend의 Data Quality(DQ) 모듈 일부 기능.
    - Informatica의 Metadata Manager 중 매핑 정의 부분.
    - 수동으로 작성하던 검증 SQL 스크립트 뭉치.
- **프로그램의 정체성**:
    AETL은 상용 ETL을 대체하는 것이 아니라, **상용 ETL이 채워주지 못하는 "AI 기반 설계 보조 및 정합성 검증" 영역을 담당하는 'AI Copilot'**입니다. 따라서 상용 툴과 병행하여 사용했을 때 시너지가 극대화되는 별개의 전문 솔루션으로 정의됩니다.

---

> **비고**: 이 내용은 외부 Gemini 등 생성형 AI에 입력하여 실제 PPT 파일을 생성하는 프롬프트로 활용 가능하도록 최적화되었습니다.
