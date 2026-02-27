## 현재 DB에 직접 연결하여 Agent를 활용하는 방식의 문제점

1. Context Window
LLM은 전체 DB 스키마를 볼 수 없습니다. 스키마가 너무 크면 잘못된 SQL/로직을 생성할 수 있습니다.

2. Latency
On-demand 프로파일링(COUNT DISTINCT 등)은 시간이 오래 걸립니다. UI/Agent가 타임아웃될 수 있습니다.

3. Security
직접 탐색 도구는 파괴적인 SQL을 실행하도록 속을 수 있습니다. 데이터 위험이 있습니다.

4. Cost
Agent의 '생각' 하나하나가 비싼 DB 집계를 유발할 수 있습니다. 시스템 성능에 영향을 줍니다.

## 해결방안: 메타데이터 엔진(완충재)
피드백은 업계 표준입니다. (예: DataBrew, Databricks)

크롤링 및 프로파일링
저장
쿼리
안전한 SQL
라이브 DB
메타데이터 엔진
메타데이터 저장소
AI 에이전트

## 주요 장점:
- 사전 계산: Agent는 DB를 쿼리하는 대신 저장소에서 "컬럼 X의 null 비율은 얼마인가?"를 밀리초 단위로 답합니다.
- 지형 매핑: Agent는 처음부터 모든 테이블과 관계를 "알고" 있어 계획 능력이 향상됩니다.
- 보안: 메타데이터 저장소에는 PII가 없고 통계만 있습니다. Agent는 검증된 특정 작업에 대해서만 DB와 상호작용합니다.

## 현재 AETL 상태
- 스키마: 이미 캐싱 메커니즘을 사용합니다 (.schema_cache.json). 이것은 "부분 메타데이터 엔진"입니다.
- 프로파일러: 현재 Direct 모드로 작동합니다. 캐시에 결과를 저장하도록 업그레이드해야 합니다.
- 에이전트: 캐시를 라이브 도구보다 우선하도록 재배선해야 합니다.


---
## 기능에 대한 피드백

1. Gemini 피드백
- documents 폴더 내 architecture_analysis.md.resolved 파일과 implementation_plan.md.resolved 파일을 참고.

2. OpenAI 피드백
- 2️⃣ 제안된 Metadata Engine 구조가 문제를 해결하는가?
- 결론: 구조적으로 문제를 거의 완전히 해결 가능, 단, 구현 방식에 따라 다시 실패할 수 있음
- 3️⃣ implementation_plan.md 구조 평가
- 제안 구조:
    DB → Metadata Engine → Metadata Store (.json) → Agent 이건 방향은 정확합니다.
    하지만 설계 수준에서 몇 가지 리스크가 보입니다.
- 4️⃣ 핵심 설계 리스크 분석
    ⚠️ 1. JSON 단일 파일 저장 (.aetl_metadata.json)
    이 방식의 문제:
    - DB가 커지면 파일이 수십 MB 이상 가능
    - LLM context에 통째로 못 넣음
    - 동시성 처리 불가
    - 부분 갱신 어려움

    👉 개선 제안
    - JSON 대신: SQLite metadata.db 또는 Postgres metadata schema 구조화 저장 권장.

    ⚠️ 2. "Metadata Sync" 수동 버튼 방식

    문서에서: Streamlit UI: Add a "Sync Metadata" button

    이 방식의 문제:
    - 사용자가 누르지 않으면 stale
    - Agent와 metadata 정합성 깨짐
    - 실시간 DB 변경 반영 안 됨

    👉 개선 제안
    - Last DDL timestamp 비교
    - incremental sync
    - table 단위 lazy sync

    ⚠️ 3. Agent가 Metadata Store만 보게 하는 구조

    문서에 이 부분이 있습니다: Agent → Metadata Store only 
    이건 절반만 맞습니다.
    
    Agent는:
    - planning 단계 → metadata만 사용
    - execution 단계 → safety-checked SQL → live DB
    이 이중 구조가 되어야 합니다.
    
    - Agent -- Safety-Checked SQL --> DB 
    좋은 방향입니다.