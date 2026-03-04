# AETL ETL 파이프라인 실행 엔진 — 구현 계획

> **작성일**: 2026-03-04
> **목표**: Talend/ADF 수준의 "정의 → 시각화 → 실행 → 모니터링" ODS→DW→DM 파이프라인을 AETL에 구현한다.
> **참조**: [loadmap.md](./loadmap.md), [ods_dw_dm_strategy.md](./ods_dw_dm_strategy.md)

---

## 1. 결론 선요약

**가능하다. 현실적으로 3~4주 분량의 작업이다.**

현재 AETL은 "설계 → 매핑 편집 → SQL 생성 → 단일 SQL 실행"까지 갖춰져 있다.
여기에 **"파이프라인 정의 + 순차 실행 + 실행 이력"** 레이어만 추가하면
ODS → DW → DM 전 과정이 상용 ETL 툴처럼 실제로 흐른다.

| 구현 항목 | 신규 코드 추정 | 기존 재사용 |
|-----------|--------------|------------|
| 파이프라인 CRUD 엔진 (`aetl_pipeline.py`) | ~400줄 | aetl_store.py 패턴 그대로 |
| SQLite 스키마 확장 | 테이블 4개 추가 | aetl_store.py init_db 확장 |
| 파이프라인 실행 엔진 | ~200줄 | aetl_executor.execute_dml 재사용 |
| UI 신규 페이지 (etl_streamlit_app.py) | ~500줄 | etl_flow_component 재사용 |
| React 컴포넌트 수정 (선택) | ~100줄 | etl_flow_component 확장 |

---

## 2. 상용 툴 벤치마킹

같은 문제를 상용 툴들이 어떻게 풀었는지 정리하고, AETL에 적합한 방식을 도출한다.

### 2.1 툴별 방식 비교

| 툴 | 파이프라인 정의 방식 | 실행 UI | AETL 적합성 |
|----|---------------------|---------|------------|
| **Talend** | 시각적 Job Designer — 컴포넌트를 캔버스에 드래그하여 연결 | 실행 버튼 → 콘솔 로그 실시간 출력 | 캔버스 방식은 과도함. React 기반이라 구현 비용 큼 |
| **Azure Data Factory** | 활동(Activity) 리스트 + 시각 캔버스 병행. 리스트에서 정의 후 캔버스는 자동 생성 | 실행 → 단계별 진행률 바 | **리스트 정의 + 시각 미리보기 방식 최적 벤치마크** |
| **dbt** | YAML 파일에 모델 의존 관계 선언. CLI로 실행 | 터미널 또는 dbt Cloud UI | 코드 중심이라 비개발자 진입장벽 높음 |
| **Airbyte** | Source/Destination 커넥터 선택 → 단순 매핑 리스트 | 실행 버튼 → 상태 배지(Running/Succeeded/Failed) | **단순 리스트 UI 방식 참고** — 배지 표시 적합 |
| **Informatica** | 매핑 + 워크플로우 Studio (GUI) | 워크플로우 실행 → 단계 하이라이트 | 규모가 너무 큼 |
| **Apache NiFi** | 프로세서 캔버스 + 연결선 드래그 | 실시간 데이터 흐름 표시 | 실시간 스트리밍 중심 — AETL 범위 초과 |
| **AWS Glue** | Job(스크립트/시각) + Trigger 설정 | Job 상태 대시보드 | 스케줄/트리거 참고용 |

### 2.2 AETL에 최적인 방식 — "ADF + Airbyte 하이브리드"

```
┌─────────────────────────────────────────────────────────┐
│  AETL 파이프라인 — 리스트 정의 + React Flow 미리보기     │
│                                                         │
│  [파이프라인 이름]  ODS_TO_DW_SALES                     │
│                                                         │
│  스텝 리스트  ← ADF Activity 리스트 방식                │
│  ┌──────────────────────────────────────────────────┐   │
│  │ 1. [ODS] ODS_ORDER  →  [DW]  FACT_SALES   ✓ 저장 │   │
│  │ 2. [ODS] ODS_CUST   →  [DIM] DIM_CUSTOMER ✓ 저장 │   │
│  │ 3. [DW]  FACT_SALES →  [DM]  DM_SALES_MNT ✓ 저장 │   │
│  └──────────────────────────────────────────────────┘   │
│                                                         │
│  Flow 미리보기  ← 기존 etl_flow_component 재사용        │
│  [ODS_ORDER] → [FACT_SALES] → [DM_SALES_MNT]           │
│  [ODS_CUST]  → [DIM_CUSTOMER]                          │
│                                                         │
│  [ 파이프라인 실행 ]  ← Airbyte 상태 배지 방식          │
│  스텝 1: ● RUNNING ...                                  │
│  스텝 2: ✅ SUCCESS (3,241건 / 0.8초)                   │
│  스텝 3: ⏸ PENDING                                     │
└─────────────────────────────────────────────────────────┘
```

**이 방식을 선택하는 이유**:
- Streamlit에서 **리스트 편집**은 `st.data_editor`로 즉시 구현 가능
- **React Flow 시각화**는 `etl_flow_component`가 이미 있어 재사용
- **진행률 표시**는 `st.status` / `st.empty()` + 루프로 Streamlit에서 구현 가능
- 복잡한 드래그 캔버스 없이도 상용 툴 수준의 경험 제공

---

## 3. 기존 코드 갭 분석

### 3.1 재사용 가능한 자산

| 모듈/기능 | 재사용 방법 |
|-----------|------------|
| `aetl_executor.execute_dml()` | 파이프라인 각 스텝의 MERGE SQL 실행에 그대로 사용 |
| `aetl_executor.execute_query()` | 스텝별 사후 검증 SQL 실행 |
| `aetl_export.generate_merge_sql()` | 스텝 저장 시 MERGE SQL 스냅샷 생성 |
| `aetl_export.generate_validation_queries_no_llm()` | 스텝별 검증 SQL 자동 생성 |
| `aetl_store.init_db()` + `_conn()` | 파이프라인 테이블도 같은 SQLite 패턴으로 추가 |
| `etl_flow_component` | 파이프라인 Flow 미리보기 시각화 재사용 |
| `flow_map_mappings` session state | 등록된 매핑 목록 → 파이프라인 스텝 후보 |

### 3.2 새로 만들어야 하는 것

| 항목 | 내용 |
|------|------|
| `aetl_pipeline.py` | 파이프라인 CRUD + 순차 실행 엔진 (신규 모듈) |
| SQLite 테이블 4개 | `pipeline`, `pipeline_step`, `pipeline_run`, `pipeline_run_step` |
| "파이프라인 실행" 페이지 | `etl_streamlit_app.py`에 신규 페이지 추가 |
| 매핑 영속화 | 현재 매핑이 세션에만 있음 → SQLite에 저장하는 기능 필요 |

### 3.3 핵심 갭 — 매핑 영속화 문제

**현재 구조의 한계**:
`flow_map_mappings`는 Streamlit 세션 상태(메모리)에만 존재한다.
앱을 새로고침하면 사라지고, 파이프라인 스텝으로 저장할 영속 레코드가 없다.

**해결 방향**:
매핑 자동화 페이지의 "Flow Map 생성" 시점에 매핑 메타데이터를
`pipeline_step` 테이블 또는 별도 `mapping_registry` 테이블에 저장한다.

---

## 4. 전체 아키텍처 설계

### 4.1 데이터 모델 (SQLite — aetl_metadata.db 확장)

```sql
-- 파이프라인 정의
CREATE TABLE pipeline (
    pipeline_id   TEXT    PRIMARY KEY,   -- UUID
    name          TEXT    NOT NULL UNIQUE,
    description   TEXT,
    db_type       TEXT    NOT NULL,       -- oracle | mariadb | postgresql
    created_at    TEXT    DEFAULT (datetime('now','localtime')),
    updated_at    TEXT    DEFAULT (datetime('now','localtime'))
);

-- 파이프라인 스텝 (매핑 1개 = 스텝 1개)
CREATE TABLE pipeline_step (
    step_id            TEXT    PRIMARY KEY,   -- UUID
    pipeline_id        TEXT    REFERENCES pipeline(pipeline_id),
    step_order         INTEGER NOT NULL,       -- 실행 순서 (1-based)
    step_name          TEXT,                  -- 사용자 지정 이름 (선택)
    layer_from         TEXT,                  -- ODS | DW | DM | SOURCE
    layer_to           TEXT,                  -- ODS | DW | DM | TARGET
    source_table       TEXT    NOT NULL,
    target_table       TEXT,
    mapping_json       TEXT,                  -- col_mappings JSON 스냅샷
    merge_sql_snapshot TEXT,                  -- 저장 시점의 MERGE SQL
    validation_sqls_json TEXT,               -- 검증 SQL 목록 JSON
    run_validation     INTEGER DEFAULT 1,     -- 적재 후 검증 실행 여부
    UNIQUE(pipeline_id, step_order)
);

-- 파이프라인 실행 이력
CREATE TABLE pipeline_run (
    run_id          TEXT    PRIMARY KEY,   -- UUID
    pipeline_id     TEXT    REFERENCES pipeline(pipeline_id),
    started_at      TEXT,
    finished_at     TEXT,
    status          TEXT,                  -- RUNNING | SUCCESS | PARTIAL | FAILED
    triggered_by    TEXT    DEFAULT 'manual'
);

-- 스텝별 실행 이력
CREATE TABLE pipeline_run_step (
    run_step_id     TEXT    PRIMARY KEY,   -- UUID
    run_id          TEXT    REFERENCES pipeline_run(run_id),
    step_id         TEXT    REFERENCES pipeline_step(step_id),
    step_order      INTEGER,
    started_at      TEXT,
    finished_at     TEXT,
    status          TEXT,                  -- PENDING | RUNNING | SUCCESS | FAILED | SKIPPED
    executed_sql    TEXT,
    affected_rows   INTEGER,
    elapsed_sec     REAL,
    error_message   TEXT,
    validation_summary TEXT              -- JSON: {passed:3, failed:1}
);
```

### 4.2 신규 모듈 — aetl_pipeline.py

```
aetl_pipeline.py
│
├── [CRUD]
│   ├── create_pipeline(name, description, db_type) → pipeline_id
│   ├── get_pipeline(pipeline_id) → dict
│   ├── list_pipelines() → list[dict]
│   ├── delete_pipeline(pipeline_id)
│   │
│   ├── add_step(pipeline_id, step_order, source_table, target_table,
│   │            mapping_json, merge_sql, validation_sqls, ...) → step_id
│   ├── reorder_steps(pipeline_id, new_order: list[step_id])
│   └── remove_step(step_id)
│
├── [실행 엔진]
│   ├── run_pipeline(pipeline_id, config_path, auto_dml=False)
│   │   → Generator[StepResult] (실시간 스텝별 결과 yield)
│   │
│   ├── _execute_step(step, config_path, auto_dml) → StepResult
│   │   ├── execute_dml(merge_sql)          ← aetl_executor 재사용
│   │   └── execute_query(validation_sql)   ← aetl_executor 재사용
│   │
│   └── get_run_history(pipeline_id, limit) → list[dict]
│
└── [헬퍼]
    ├── build_step_from_mapping(src_meta, tgt_meta, col_mappings, ...) → dict
    └── regenerate_merge_sql(step_id) → str
```

### 4.3 UI 페이지 구조

```
"파이프라인 실행" 페이지 (etl_streamlit_app.py 신규 추가)
│
├── [탭 1] 파이프라인 정의
│   ├── 파이프라인 선택 / 신규 생성
│   ├── 스텝 리스트 편집 (st.data_editor 또는 커스텀)
│   │   └── 각 스텝: 순서 | 이름 | 소스 | 타겟 | 레이어 | MERGE SQL 미리보기
│   ├── 스텝 추가 방법
│   │   ├── [현재 세션 매핑에서 가져오기] ← flow_map_mappings 재사용
│   │   └── [직접 소스/타겟 지정]
│   └── Flow 미리보기 (etl_flow_component 재사용)
│
├── [탭 2] 파이프라인 실행
│   ├── 실행 옵션
│   │   ├── DML 실행 모드: "전체 사전 승인" | "스텝별 승인"
│   │   └── 실패 시: "중단" | "다음 스텝 계속"
│   ├── [▶ 파이프라인 실행] 버튼
│   └── 실시간 진행 표시
│       ├── 전체 진행률 바
│       └── 스텝별 상태 카드 (PENDING → RUNNING → SUCCESS/FAILED)
│           └── 성공: 영향 행 수, 소요 시간
│           └── 실패: 에러 메시지 + AI 진단
│
└── [탭 3] 실행 이력
    ├── 파이프라인별 실행 이력 목록
    └── 실행 상세 (스텝별 결과 + 검증 요약)
```

---

## 5. 단계별 구현 계획

### Phase A: 파이프라인 정의 + 저장 (1주)

**목표**: 사용자가 "어떤 매핑들을 어떤 순서로 실행할지"를 정의하고 저장할 수 있다.

**작업 목록**:

1. **`aetl_store.py` 확장**
   - `_DDL`에 `pipeline`, `pipeline_step` 테이블 추가
   - 기존 `init_db()` 호출로 자동 생성됨 (하위 호환 유지)

2. **`aetl_pipeline.py` 신규 생성** (CRUD 파트만)
   ```python
   create_pipeline(), get_pipeline(), list_pipelines(), delete_pipeline()
   add_step(), remove_step(), reorder_steps()
   build_step_from_mapping()  # 현재 세션 매핑 → 스텝 변환
   ```

3. **`etl_streamlit_app.py` 신규 페이지 추가**
   - 사이드바 메뉴에 "파이프라인 실행" 추가 (Automation 그룹)
   - 탭 1 (파이프라인 정의) 구현
   - 매핑 자동화 "Flow Map 생성" 버튼 옆에 **"파이프라인에 추가"** 버튼 추가

**주요 UX 결정**:
- 스텝 순서는 `st.data_editor`로 숫자 직접 입력 (드래그 불필요)
- 레이어(ODS/DW/DM) 라벨은 selectbox로 수동 지정
- Flow 미리보기는 기존 `etl_flow_component`에 그대로 연결

**완료 기준**: 파이프라인 저장 → 새로고침 후에도 목록에서 조회 가능

---

### Phase B: 순차 실행 엔진 (1주)

**목표**: "파이프라인 실행" 버튼 클릭 → 스텝 순서대로 MERGE SQL 실행 → 이력 저장.

**작업 목록**:

1. **`aetl_pipeline.py` 실행 파트 추가**
   ```python
   run_pipeline(pipeline_id, config_path, auto_dml=False)
   # → generator: yield {"step_order": 1, "status": "SUCCESS", "affected_rows": 3241, ...}

   _execute_step(step, config_path) → StepResult
   # 내부: aetl_executor.execute_dml() 재사용
   ```

2. **`aetl_store.py` 확장** — `pipeline_run`, `pipeline_run_step` 테이블 추가
   ```python
   save_pipeline_run(), update_pipeline_run_status()
   save_pipeline_run_step(), update_pipeline_run_step()
   get_run_history()
   ```

3. **UI — 탭 2 (실행) 구현**:

   **실행 흐름 (Human-in-the-Loop 유지)**:
   ```
   [파이프라인 실행] 클릭
        ↓
   실행 모드 선택: "전체 MERGE SQL 미리보기 후 일괄 승인" (권장)
        ↓
   전체 스텝의 MERGE SQL 목록 표시 + [승인 후 실행] 버튼
        ↓
   st.status 컨텍스트 내에서 generator 순회
   → 각 스텝 완료 시 카드 업데이트 (st.empty 재렌더링)
        ↓
   전체 완료 → 요약 (성공 N건 / 실패 N건)
   ```

4. **에러 처리**:
   - 스텝 실패 시: `aetl_executor.diagnose_failure()` 자동 호출 → AI 원인 분석
   - 실패 스텝은 `FAILED` 이력에 저장, 이후 스텝은 옵션에 따라 계속 또는 중단

**완료 기준**: ODS→DW 2개 스텝 파이프라인이 한 번의 승인으로 순차 실행됨

---

### Phase C: 스텝별 검증 자동 연동 (3~4일)

**목표**: 각 스텝의 MERGE 실행 직후 사전 저장된 검증 SQL을 자동 실행하여
"적재 → 검증"이 한 흐름으로 처리된다.

**작업 목록**:

1. **파이프라인 스텝에 검증 SQL 바인딩**
   - `pipeline_step.validation_sqls_json` 컬럼에 `generate_validation_queries_no_llm()` 결과 저장
   - 스텝 정의 UI에 "적재 후 검증 실행" 토글 추가

2. **실행 엔진 확장** — `_execute_step()` 내에 검증 루프 추가
   ```python
   # MERGE 실행 후
   if step["run_validation"] and step["validation_sqls_json"]:
       for v_sql in validation_sqls:
           result = execute_query(v_sql, config_path)
           # 결과를 pipeline_run_step.validation_summary에 저장
   ```

3. **실행 결과 UI 확장**
   - 각 스텝 카드에 "검증 결과 요약" 섹션 추가
   - `PASS N / FAIL N` 배지 표시

**완료 기준**: 스텝 실행 후 검증 결과가 자동으로 표시됨

---

### Phase D: 실행 이력 대시보드 (2~3일)

**목표**: 탭 3에서 파이프라인 실행 이력과 스텝별 Pass/Fail 추이를 확인할 수 있다.

**작업 목록**:
1. 파이프라인 선택 → 최근 실행 N건 목록 표시 (실행 ID, 시각, 상태, 소요 시간)
2. 실행 클릭 → 스텝별 상세 (영향 행 수, 검증 요약, 에러)
3. 재실행 버튼 (실패한 스텝부터 이어 실행 — 선택 사항)

---

## 6. 파일별 변경 계획

| 파일 | 변경 유형 | 내용 |
|------|-----------|------|
| `aetl_pipeline.py` | **신규 생성** | 파이프라인 CRUD + 실행 엔진 (~600줄) |
| `aetl_store.py` | **확장** | `_DDL`에 4개 테이블 추가, 저장/조회 함수 추가 (~100줄) |
| `etl_streamlit_app.py` | **확장** | 사이드바 메뉴 1개 추가, 신규 페이지 함수 (~500줄) |
| `documents/architecture/loadmap.md` | **갱신** | 파이프라인 기능 반영, 버전 업 |

**변경하지 않는 파일**:
`aetl_executor.py`, `aetl_export.py`, `etl_flow_component/` — 그대로 재사용

---

## 7. UI 상세 와이어프레임

### 7.1 파이프라인 정의 탭

```
┌─────────────────────────────────────────────────────────────┐
│  파이프라인 실행                               [Automation]  │
├─────────────────────────────────────────────────────────────┤
│  파이프라인 선택  [ODS_TO_DW_SALES ▼]  [+ 새 파이프라인]    │
│                                                             │
│  ┌─ 탭: 정의 ──────────────────────────────────────────┐   │
│  │                                                     │   │
│  │  스텝 목록                          [스텝 추가 ▼]   │   │
│  │  ┌───┬──────────────┬──────────────┬──────┬──────┐  │   │
│  │  │ # │  소스 테이블 │  타겟 테이블 │ 레이어│ 검증 │  │   │
│  │  ├───┼──────────────┼──────────────┼──────┼──────┤  │   │
│  │  │ 1 │ ODS_ORDER    │ FACT_SALES   │ODS→DW│  ✓   │  │   │
│  │  │ 2 │ ODS_CUSTOMER │ DIM_CUSTOMER │ODS→DW│  ✓   │  │   │
│  │  │ 3 │ FACT_SALES   │ DM_SALES_MNT │DW→DM │  ✓   │  │   │
│  │  └───┴──────────────┴──────────────┴──────┴──────┘  │   │
│  │                            [↑ 위로] [↓ 아래] [삭제]  │   │
│  │                                                     │   │
│  │  Flow 미리보기 ─────────────────────────────────    │   │
│  │  [ODS_ORDER]──▶[FACT_SALES]──▶[DM_SALES_MNT]       │   │
│  │  [ODS_CUSTOMER]──▶[DIM_CUSTOMER]                    │   │
│  │                                                     │   │
│  │                              [💾 파이프라인 저장]   │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 7.2 파이프라인 실행 탭

```
┌─────────────────────────────────────────────────────────────┐
│  ┌─ 탭: 실행 ───────────────────────────────────────────┐   │
│  │                                                      │   │
│  │  실행 옵션                                           │   │
│  │  ○ 전체 MERGE SQL 미리보기 후 일괄 승인 (권장)       │   │
│  │  ○ 스텝별 승인 (단계마다 확인)                       │   │
│  │  실패 시: ○ 중단  ○ 다음 스텝 계속                   │   │
│  │                                                      │   │
│  │  MERGE SQL 미리보기 (승인 전)                        │   │
│  │  [펼치기] 스텝 1 MERGE SQL ...                       │   │
│  │  [펼치기] 스텝 2 MERGE SQL ...                       │   │
│  │  [펼치기] 스텝 3 MERGE SQL ...                       │   │
│  │                                                      │   │
│  │              [▶  파이프라인 실행  (3 스텝)]          │   │
│  │                                                      │   │
│  │  ── 실행 중 ────────────────────────────────────     │   │
│  │  진행률  ████████░░░░  2/3                           │   │
│  │                                                      │   │
│  │  ✅ 스텝 1  ODS_ORDER → FACT_SALES                   │   │
│  │     3,241행 영향 | 0.8초 | 검증 3/3 PASS            │   │
│  │                                                      │   │
│  │  ✅ 스텝 2  ODS_CUSTOMER → DIM_CUSTOMER              │   │
│  │     1,084행 영향 | 0.3초 | 검증 3/3 PASS            │   │
│  │                                                      │   │
│  │  ⚡ 스텝 3  FACT_SALES → DM_SALES_MNT  실행 중...   │   │
│  │                                                      │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### 7.3 매핑 자동화 페이지 연동 (기존 페이지 확장)

```
[기존] Flow Map 생성  →  [추가] 파이프라인에 스텝으로 추가
                         └── 드롭다운: [파이프라인 선택 또는 신규 생성]
                                       [스텝 순서 지정]
                                       [레이어 지정 (ODS/DW/DM)]
                                       [추가] 버튼
```

---

## 8. 주요 기술 결정 및 트레이드오프

### 8.1 실시간 진행 표시 — Streamlit 한계와 해결책

**문제**: Streamlit은 기본적으로 전체 스크립트를 재실행하는 구조라
파이프라인 실행 중 실시간 상태 업데이트가 까다롭다.

**해결책** (Streamlit 공식 지원):
```python
with st.status("파이프라인 실행 중...", expanded=True) as status:
    for step_result in run_pipeline(pipeline_id, config_path):
        if step_result["status"] == "SUCCESS":
            st.success(f"✅ 스텝 {step_result['step_order']} 완료")
        elif step_result["status"] == "FAILED":
            st.error(f"❌ 스텝 {step_result['step_order']} 실패: {step_result['error']}")
    status.update(label="완료", state="complete")
```
→ `st.status` + generator 패턴으로 실시간처럼 보이는 순차 업데이트 가능.

### 8.2 매핑 영속화 전략

**옵션 1**: 매핑 자동화 "Flow Map 생성" 시 자동으로 `pipeline_step`에 저장
**옵션 2**: 파이프라인 정의 탭에서 "현재 세션 매핑에서 가져오기" 버튼으로 명시적 추가
**선택**: **옵션 2** — Human-in-the-Loop 원칙 준수. 사용자가 명시적으로 "파이프라인에 포함"을 결정.

### 8.3 MERGE SQL 스냅샷 vs 실시간 재생성

**문제**: 스텝 저장 후 매핑이 변경되면 저장된 MERGE SQL과 불일치 발생.

**결정**:
- 저장 시점 MERGE SQL을 `merge_sql_snapshot`으로 보관
- 실행 전 UI에서 스냅샷 vs 현재 매핑 기반 SQL의 차이를 경고로 표시 (Phase B+)
- 사용자가 "재생성"을 선택할 수 있음

### 8.4 트랜잭션 처리

**전략**: 각 스텝은 독립 트랜잭션. 전체 파이프라인 롤백은 지원하지 않음.
이유: 스텝 간 다른 테이블, DB가 다를 수 있어 분산 트랜잭션 불필요.
실패 시 수동 보정 또는 "실패 스텝부터 재실행" 기능으로 대응.

---

## 9. 현실적 제약 및 불가능한 부분

| 항목 | 가능 여부 | 이유 |
|------|-----------|------|
| 리스트 기반 파이프라인 정의 | ✅ 가능 | Streamlit + SQLite로 충분 |
| 순차 실행 + 이력 | ✅ 가능 | execute_dml 재사용 |
| 실시간 진행 표시 | ✅ 가능 | st.status + generator |
| 스텝 간 데이터 의존성 검사 | ✅ 가능 | 테이블명 기반 선행 스텝 완료 확인 |
| 드래그앤드롭 캔버스 (Talend 스타일) | ⚠️ 부분 가능 | React 컴포넌트 추가 개발 필요 (Phase D 선택사항) |
| 병렬 스텝 실행 | ⚠️ 제한적 | `concurrent.futures`로 구현 가능하나 DB 부하 고려 필요 |
| 스케줄러 (Cron) | ❌ Streamlit 내 불가 | Streamlit은 웹 서버 — 별도 스케줄러 프로세스 필요. Phase D에서 APScheduler 별도 서비스로 분리 필요 |
| 다중 DB 파이프라인 (스텝마다 다른 DB) | ⚠️ 구조적 가능 | `db_config.json` 경로를 스텝별로 지정하면 가능. 단, UI 복잡도 증가 |

---

## 10. 구현 우선순위 및 권장 착수 순서

```
Week 1:  Phase A — 파이프라인 정의 + 저장
         ├── aetl_store.py DDL 확장 (0.5일)
         ├── aetl_pipeline.py CRUD 파트 (1.5일)
         └── etl_streamlit_app.py 탭 1 UI (3일)

Week 2:  Phase B — 순차 실행 엔진
         ├── aetl_pipeline.py 실행 파트 (2일)
         ├── aetl_store.py pipeline_run 저장 (0.5일)
         └── etl_streamlit_app.py 탭 2 실행 UI (2.5일)

Week 3:  Phase C + D — 검증 연동 + 이력 대시보드
         ├── 스텝별 검증 연동 (2일)
         └── 이력 탭 UI (3일)

Week 4:  검증 테스트 + 버그 수정 + loadmap.md 갱신
```

---

## 11. 최종 판단

> **AETL의 설계 원칙(Human-in-the-Loop, Single Source of Truth, 멀티 DB)을 유지하면서
> 상용 ETL 수준의 파이프라인 실행 흐름을 구현하는 것은 현실적으로 가능하다.**

현재 가장 큰 가치를 만드는 순서:
1. **파이프라인 정의 + 저장** (Phase A) — 사용자가 흐름을 "명시"할 수 있게 됨
2. **순차 실행** (Phase B) — 실제로 ODS→DW→DM이 한 번에 실행됨
3. **검증 자동 연동** (Phase C) — 상용 툴 수준의 적재+검증 일체화

스케줄러(Cron)와 드래그앤드롭 캔버스는 현재 Streamlit 아키텍처 내에서
복잡도 대비 효용이 낮으므로 Phase D 이후로 미루는 것이 현실적이다.

---

*작성: WI사업부 안주현 | AETL v2.3 파이프라인 실행 구현 계획*
