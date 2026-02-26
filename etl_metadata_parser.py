"""
ETL 검증용 메타데이터 파서
Excel/CSV 테이블 정의서를 파싱하여 구조화된 메타데이터를 반환합니다.

지원 형식:
  - Excel (.xlsx, .xls): 단일 시트 또는 멀티 시트
  - CSV (.csv)

컬럼명 자동 감지 (한글/영문 모두 지원):
  테이블명: table_name, 테이블명, TABLE_NAME
  컬럼명:   column_name, 컬럼명, COLUMN_NAME, col_name
  타입:     data_type, 데이터타입, type, TYPE
  PK:       pk, PK, primary_key, 기본키, is_pk
  NOT NULL: nullable, null여부, not_null, NN
  설명:     description, 설명, comment, COMMENT, remarks
"""

import io
import re
from typing import Optional

import pandas as pd


# ─────────────────────────────────────────
# 컬럼명 후보 매핑
# ─────────────────────────────────────────
_COL_ALIASES = {
    "table_name":   ["table_name", "테이블명", "테이블", "table", "tbl_name", "tbl", "tablename", "table name"],
    "column_name":  ["column_name", "컬럼명", "컬럼", "col_name", "column", "col", "columnname", "column name", "field", "필드"],
    "data_type":    ["data_type", "데이터타입", "타입", "type", "dtype", "data type", "datatype", "자료형"],
    "pk":           ["pk", "primary_key", "기본키", "is_pk", "pk여부", "primarykey", "primary key", "키구분"],
    "nullable":     ["nullable", "null여부", "not_null", "nn", "null", "null허용", "notnull", "not null"],
    "description":  ["description", "설명", "comment", "remarks", "비고", "desc", "컬럼설명"],
}

# 소스/타겟 시트 이름 후보
_SOURCE_SHEET_NAMES = ["source", "소스", "src", "원천", "ods", "sheet1", "source_table"]
_TARGET_SHEET_NAMES = ["target", "타겟", "tgt", "dw", "dwh", "dest", "sheet2", "target_table"]

# 매핑 시트 이름 후보
_MAPPING_SHEET_NAMES = ["mapping", "매핑", "column_mapping", "컬럼매핑", "map", "sheet3"]


def _normalize(text: str) -> str:
    """소문자 + 공백 제거"""
    return str(text).strip().lower().replace(" ", "").replace("_", "")


def _detect_column(df_columns: list[str], field: str) -> Optional[str]:
    """df 컬럼 중 field 후보와 일치하는 것을 반환"""
    aliases = [_normalize(a) for a in _COL_ALIASES.get(field, [])]
    for col in df_columns:
        if _normalize(col) in aliases:
            return col
    return None


def _is_yes(value) -> bool:
    """PK/Not-Null 등 Y/N 값을 bool로 변환"""
    if pd.isna(value):
        return False
    s = str(value).strip().upper()
    return s in ("Y", "YES", "TRUE", "1", "O", "●", "○", "V", "✓", "✔")


def _parse_dataframe(df: pd.DataFrame, default_table_name: str = "UNKNOWN") -> dict:
    """
    DataFrame → 테이블 메타데이터 dict 변환

    Returns:
        {
          "table_name": "CUSTOMER",
          "columns": [
            {"name": "CUSTOMER_ID", "type": "VARCHAR2(20)", "pk": True, "nullable": False, "description": "고객ID"},
            ...
          ],
          "pk_columns": ["CUSTOMER_ID"]
        }
    """
    df = df.dropna(how="all").reset_index(drop=True)
    cols = list(df.columns)

    col_map = {field: _detect_column(cols, field) for field in _COL_ALIASES}

    # 테이블명: 별도 컬럼이 있으면 사용, 없으면 default
    table_col = col_map["table_name"]
    col_col   = col_map["column_name"]

    if col_col is None:
        # 컬럼명 컬럼을 찾지 못하면 첫 번째 컬럼을 사용
        col_col = cols[0]

    columns = []
    seen_table = default_table_name

    for _, row in df.iterrows():
        if table_col and not pd.isna(row.get(table_col)):
            val = str(row[table_col]).strip()
            if val:
                seen_table = val.upper()

        col_name_val = row.get(col_col)
        if pd.isna(col_name_val) or str(col_name_val).strip() == "":
            continue
        col_name = str(col_name_val).strip().upper()

        dtype = ""
        if col_map["data_type"] and not pd.isna(row.get(col_map["data_type"])):
            dtype = str(row[col_map["data_type"]]).strip()

        is_pk = False
        if col_map["pk"] and not pd.isna(row.get(col_map["pk"])):
            is_pk = _is_yes(row[col_map["pk"]])

        # nullable: "Y" = NULL 허용 = nullable=True / "N" = NOT NULL = nullable=False
        nullable = True
        if col_map["nullable"] and not pd.isna(row.get(col_map["nullable"])):
            raw = str(row[col_map["nullable"]]).strip().upper()
            # "NOT NULL" 계열이면 nullable=False
            if raw in ("N", "NO", "NOT NULL", "NN", "FALSE", "0"):
                nullable = False
            else:
                nullable = True

        desc = ""
        if col_map["description"] and not pd.isna(row.get(col_map["description"])):
            desc = str(row[col_map["description"]]).strip()

        columns.append({
            "name":        col_name,
            "type":        dtype,
            "pk":          is_pk,
            "nullable":    nullable,
            "description": desc,
        })

    table_name = seen_table if table_col else default_table_name
    pk_columns = [c["name"] for c in columns if c["pk"]]

    return {
        "table_name": table_name.upper(),
        "columns":    columns,
        "pk_columns": pk_columns,
    }


def _find_sheet(xl: pd.ExcelFile, candidates: list[str]) -> Optional[str]:
    """시트 이름 후보 중 일치하는 것 반환"""
    sheet_names_lower = {s.lower(): s for s in xl.sheet_names}
    for c in candidates:
        if c.lower() in sheet_names_lower:
            return sheet_names_lower[c.lower()]
    return None


# ─────────────────────────────────────────
# Public API
# ─────────────────────────────────────────

def parse_table_file(
    file,
    table_name_hint: str = "UNKNOWN",
    sheet_name: Optional[str] = None,
) -> dict:
    """
    Excel 또는 CSV 파일에서 테이블 메타데이터 파싱

    Args:
        file:            파일 경로(str) 또는 파일 객체 (Streamlit UploadedFile)
        table_name_hint: 파일 내에 테이블명이 없을 때 사용할 기본값
        sheet_name:      Excel의 경우 특정 시트명 지정 (None이면 자동 감지)

    Returns:
        {"table_name": ..., "columns": [...], "pk_columns": [...]}
    """
    if hasattr(file, "name"):
        filename = file.name
    else:
        filename = str(file)

    ext = filename.rsplit(".", 1)[-1].lower()

    if ext == "csv":
        df = pd.read_csv(file, dtype=str)
        return _parse_dataframe(df, default_table_name=table_name_hint)

    # Excel
    xl = pd.ExcelFile(file)

    if sheet_name:
        df = xl.parse(sheet_name, dtype=str)
        return _parse_dataframe(df, default_table_name=table_name_hint)

    # 시트가 1개면 그냥 사용
    if len(xl.sheet_names) == 1:
        df = xl.parse(xl.sheet_names[0], dtype=str)
        return _parse_dataframe(df, default_table_name=table_name_hint)

    # 여러 시트인데 지정 없음 → 첫 번째 시트
    df = xl.parse(xl.sheet_names[0], dtype=str)
    return _parse_dataframe(df, default_table_name=table_name_hint)


def parse_source_target_file(file) -> tuple[Optional[dict], Optional[dict]]:
    """
    소스/타겟이 하나의 Excel 파일에 시트로 구분된 경우 파싱

    Returns:
        (source_meta, target_meta) — 시트를 못 찾으면 None
    """
    xl = pd.ExcelFile(file)

    source_sheet = _find_sheet(xl, _SOURCE_SHEET_NAMES)
    target_sheet = _find_sheet(xl, _TARGET_SHEET_NAMES)

    source_meta = None
    target_meta = None

    if source_sheet:
        df = xl.parse(source_sheet, dtype=str)
        source_meta = _parse_dataframe(df, default_table_name="SOURCE_TABLE")

    if target_sheet:
        df = xl.parse(target_sheet, dtype=str)
        target_meta = _parse_dataframe(df, default_table_name="TARGET_TABLE")

    return source_meta, target_meta


def parse_mapping_file(file, sheet_name: Optional[str] = None) -> list[dict]:
    """
    컬럼 매핑 정의서 파싱

    기대 컬럼 (유연하게 감지):
      소스테이블, 소스컬럼, 타겟테이블, 타겟컬럼, 변환규칙

    Returns:
        [
          {"source_table": "ODS_CUSTOMER", "source_col": "CUST_ID",
           "target_table": "DW_CUSTOMER",  "target_col": "CUSTOMER_ID",
           "transform": ""},
          ...
        ]
    """
    _MAPPING_ALIASES = {
        "source_table":  ["source_table", "소스테이블", "src_table", "원천테이블", "source table"],
        "source_col":    ["source_column", "source_col", "소스컬럼", "src_col", "원천컬럼", "source column"],
        "target_table":  ["target_table", "타겟테이블", "tgt_table", "dest_table", "target table"],
        "target_col":    ["target_column", "target_col", "타겟컬럼", "tgt_col", "target column"],
        "transform":     ["transform", "변환규칙", "변환", "rule", "비고", "remarks"],
    }

    if hasattr(file, "name"):
        filename = file.name
    else:
        filename = str(file)

    ext = filename.rsplit(".", 1)[-1].lower()

    if ext == "csv":
        df = pd.read_csv(file, dtype=str)
    else:
        xl = pd.ExcelFile(file)
        target = sheet_name
        if not target:
            target = _find_sheet(xl, _MAPPING_SHEET_NAMES)
        if not target:
            target = xl.sheet_names[0]
        df = xl.parse(target, dtype=str)

    df = df.dropna(how="all").reset_index(drop=True)
    cols = list(df.columns)

    def detect(field):
        aliases = [_normalize(a) for a in _MAPPING_ALIASES.get(field, [])]
        for c in cols:
            if _normalize(c) in aliases:
                return c
        return None

    src_tbl_col = detect("source_table")
    src_col_col = detect("source_col")
    tgt_tbl_col = detect("target_table")
    tgt_col_col = detect("target_col")
    transform_col = detect("transform")

    rows = []
    for _, row in df.iterrows():
        entry = {
            "source_table": str(row[src_tbl_col]).strip().upper() if src_tbl_col and not pd.isna(row.get(src_tbl_col)) else "",
            "source_col":   str(row[src_col_col]).strip().upper()  if src_col_col and not pd.isna(row.get(src_col_col)) else "",
            "target_table": str(row[tgt_tbl_col]).strip().upper()  if tgt_tbl_col and not pd.isna(row.get(tgt_tbl_col)) else "",
            "target_col":   str(row[tgt_col_col]).strip().upper()  if tgt_col_col and not pd.isna(row.get(tgt_col_col)) else "",
            "transform":    str(row[transform_col]).strip()         if transform_col and not pd.isna(row.get(transform_col)) else "",
        }
        if entry["source_col"] or entry["target_col"]:
            rows.append(entry)

    return rows


# ─────────────────────────────────────────
# 매핑정의서 전용 파서 (DM/DW/ODS 표준 포맷)
# ─────────────────────────────────────────

def _val(row, idx) -> str:
    """행에서 idx 위치 값을 안전하게 문자열로 반환"""
    try:
        v = row.iloc[idx]
        return "" if pd.isna(v) else str(v).strip()
    except (IndexError, KeyError):
        return ""


def _is_number(v) -> bool:
    """No 컬럼 값이 숫자(행 번호)인지 확인"""
    try:
        float(str(v).strip())
        return True
    except (ValueError, TypeError):
        return False


def _detect_subheader_row(df: pd.DataFrame) -> int:
    """
    sub-header 행 인덱스 탐지
    '컬럼명'이 2회 이상 등장하는 행을 sub-header로 판단
    """
    for i in range(min(10, len(df))):
        row = df.iloc[i]
        count = sum(1 for v in row if str(v).strip() in ("컬럼명", "Column Name", "COLUMN_NAME"))
        if count >= 2:
            return i
    return -1


def _detect_col_positions(df: pd.DataFrame, subheader_idx: int) -> dict:
    """
    sub-header 행과 그 위 행(그룹 헤더)을 스캔하여 각 필드의 컬럼 인덱스 반환.
    병합 셀로 인해 헤더 위치와 데이터 위치가 1칸 어긋나는 경우를 data_rows로 보정.
    """
    sh = df.iloc[subheader_idx]
    gh: pd.Series = df.iloc[subheader_idx - 1] if subheader_idx > 0 else pd.Series([], dtype=object)

    col_name_idxs, pk_idxs, type_idxs, nn_idxs = [], [], [], []
    table_idxs, system_idxs, transform_idxs = [], [], []

    for i, v in enumerate(sh):
        s = str(v).strip()
        sl = s.lower()
        if s in ("컬럼명", "Column Name", "COLUMN_NAME"):
            col_name_idxs.append(i)
        elif s == "PK":
            pk_idxs.append(i)
        elif "data type" in sl or s in ("데이터타입", "타입", "TYPE"):
            type_idxs.append(i)
        elif "n.n" in sl or s in ("N.N여부", "nullable", "null여부", "NN여부"):
            nn_idxs.append(i)
        elif "테이블명" in s or "table" in sl:
            table_idxs.append(i)
        elif "시스템명" in s or "system" in sl:
            system_idxs.append(i)
        elif "변환" in s or "transform" in sl or "rule" in sl:
            transform_idxs.append(i)

    # 그룹 헤더에서 변환규칙 위치 탐색
    for i, v in enumerate(gh.values.tolist()):
        s = str(v).strip()
        if "변환" in s or "transform" in s.lower():
            if i not in transform_idxs:
                transform_idxs.append(i)

    pos = {
        "tgt_col":    col_name_idxs[0]   if len(col_name_idxs) > 0 else 1,
        "tgt_pk":     pk_idxs[0]         if len(pk_idxs) > 0       else 4,
        "tgt_type":   type_idxs[0]       if len(type_idxs) > 0     else 5,
        "tgt_nn":     nn_idxs[0]         if len(nn_idxs) > 0       else 7,
        "src_system": system_idxs[0]     if system_idxs             else 8,
        "src_table":  table_idxs[0]      if table_idxs              else 9,
        "src_col":    col_name_idxs[1]   if len(col_name_idxs) > 1 else 12,
        "src_pk":     pk_idxs[1]         if len(pk_idxs) > 1       else 15,
        "src_type":   type_idxs[1]       if len(type_idxs) > 1     else 16,
        "src_nn":     nn_idxs[1]         if len(nn_idxs) > 1       else 18,
        "transform":  transform_idxs[0]  if transform_idxs          else 19,
    }

    # ── 병합 셀 오프셋 보정 ──
    # 헤더에서 탐지한 src_table 위치에 실제 데이터가 없고 +1 위치에 있으면 보정
    data_start = subheader_idx + 1
    sample_vals = []
    for ri in range(data_start, min(data_start + 5, len(df))):
        row = df.iloc[ri]
        if _is_number(_val(row, pos["tgt_col"] - 1 if pos["tgt_col"] > 0 else 0)) or \
           _val(row, pos["tgt_col"]) != "":
            sample_vals.append((_val(row, pos["src_table"]), _val(row, pos["src_table"] + 1)))

    if sample_vals:
        at_pos     = sum(1 for v, _ in sample_vals if v and len(v) < 80)
        at_pos_p1  = sum(1 for _, v in sample_vals if v and len(v) < 80)
        if at_pos_p1 > at_pos:
            pos["src_table"] += 1

    return pos


def _extract_table_name_from_header(df: pd.DataFrame) -> str:
    """
    Row 0 에서 'Table 명' 다음 셀 값을 타겟 테이블명으로 반환.
    못 찾으면 빈 문자열.
    """
    for ri in range(min(5, len(df))):
        row = df.iloc[ri]
        for ci in range(len(row) - 1):
            v = str(row.iloc[ci]).strip()
            if v in ("Table 명", "Table명", "테이블명", "TABLE_NAME") or \
               ("table" in v.lower() and "명" in v):
                nxt = str(row.iloc[ci + 1]).strip()
                if nxt and nxt != "nan":
                    return nxt.upper()
                # 그 다음 non-NaN 셀 탐색
                for ci2 in range(ci + 1, min(ci + 4, len(row))):
                    nxt = str(row.iloc[ci2]).strip()
                    if nxt and nxt != "nan":
                        return nxt.upper()
    return ""


def is_mapping_definition_format(df: pd.DataFrame) -> bool:
    """
    매핑정의서 포맷 여부 자동 감지.
    sub-header에 '컬럼명'이 2회 이상 등장하면 True.
    """
    return _detect_subheader_row(df) >= 0


def parse_mapping_definition_sheet(df: pd.DataFrame) -> dict:
    """
    DM/DW/ODS 매핑정의서 시트(raw DataFrame) 파싱.

    Returns:
        {
          "target_meta": {"table_name": ..., "columns": [...], "pk_columns": [...]},
          "source_meta": {"table_name": ..., "columns": [...], "pk_columns": [...]},
          "mapping":     [{"source_table": ..., "source_col": ..., "target_table": ...,
                           "target_col": ..., "transform": ...}, ...],
        }
    """
    subheader_idx = _detect_subheader_row(df)
    if subheader_idx < 0:
        raise ValueError("매핑정의서 포맷을 인식할 수 없습니다. sub-header(컬럼명 행)를 찾지 못했습니다.")

    pos = _detect_col_positions(df, subheader_idx)

    # 타겟 테이블명 추출
    target_table = _extract_table_name_from_header(df)

    # 데이터 행 수집 (col 0 또는 tgt_col 위치에 값이 있는 행)
    data_start = subheader_idx + 1
    tgt_cols, src_cols, mapping_rows = [], [], []
    src_table_candidates = {}

    for ri in range(data_start, len(df)):
        row = df.iloc[ri]

        no_val   = _val(row, 0)
        tgt_name = _val(row, pos["tgt_col"]).upper()

        # 데이터 행 판단: No 컬럼이 숫자이거나 타겟 컬럼명이 있으면
        if not tgt_name and not _is_number(no_val):
            continue

        if not tgt_name:
            continue

        # 타겟 컬럼 정보
        tgt_pk   = _is_yes(_val(row, pos["tgt_pk"]))
        tgt_type = _val(row, pos["tgt_type"])
        tgt_nn   = _val(row, pos["tgt_nn"]).upper()
        tgt_nullable = tgt_nn not in ("NN", "N", "NOT NULL", "Y")  # NN = NOT NULL → nullable=False

        tgt_cols.append({
            "name":        tgt_name,
            "type":        tgt_type,
            "pk":          tgt_pk,
            "nullable":    tgt_nullable,
            "description": "",
        })

        # 소스 정보
        src_tbl_val  = _val(row, pos["src_table"]).upper()
        src_col_val  = _val(row, pos["src_col"]).upper()
        src_pk       = _is_yes(_val(row, pos["src_pk"]))
        src_type     = _val(row, pos["src_type"])
        src_nn       = _val(row, pos["src_nn"]).upper()
        src_nullable = src_nn not in ("NN", "N", "NOT NULL", "Y")
        transform    = _val(row, pos["transform"])

        # SQL이 아닌 실제 테이블명 수집 (80자 이하 = 테이블명, 이상 = SQL)
        if src_tbl_val and len(src_tbl_val) < 80:
            src_table_candidates[src_tbl_val] = src_table_candidates.get(src_tbl_val, 0) + 1

        if src_col_val:
            src_cols.append({
                "name":        src_col_val,
                "type":        src_type,
                "pk":          src_pk,
                "nullable":    src_nullable,
                "description": "",
                "_src_table":  src_tbl_val,
            })

            mapping_rows.append({
                "source_table": src_tbl_val,
                "source_col":   src_col_val,
                "target_table": target_table,
                "target_col":   tgt_name,
                "transform":    transform,
            })

    # 소스 테이블명: 가장 많이 등장한 값
    src_table = max(src_table_candidates, key=lambda k: src_table_candidates[k]) \
        if src_table_candidates else "SOURCE_TABLE"

    # 소스 컬럼에서 _src_table 키 제거
    for c in src_cols:
        c.pop("_src_table", None)

    tgt_pk_cols = [c["name"] for c in tgt_cols if c["pk"]]
    src_pk_cols = [c["name"] for c in src_cols if c["pk"]]

    return {
        "target_meta": {
            "table_name": target_table or "TARGET_TABLE",
            "columns":    tgt_cols,
            "pk_columns": tgt_pk_cols,
        },
        "source_meta": {
            "table_name": src_table,
            "columns":    src_cols,
            "pk_columns": src_pk_cols,
        },
        "mapping": mapping_rows,
    }


def parse_mapping_definition_excel(file, sheet_name: Optional[str] = None) -> dict:
    """
    DM/DW/ODS 매핑정의서 Excel 파일 파싱 (단일 파일에 소스+타겟 포함).

    Args:
        file:       파일 경로 또는 Streamlit UploadedFile
        sheet_name: 특정 시트 지정 (None이면 첫 번째 시트)

    Returns:
        {
          "target_meta": {...},
          "source_meta": {...},
          "mapping":     [...],
        }
    """
    xl = pd.ExcelFile(file)
    sheet = sheet_name or xl.sheet_names[0]
    # 병합 셀을 그대로 읽기 위해 header=None
    df = xl.parse(sheet, header=None, dtype=str)
    return parse_mapping_definition_sheet(df)


def schema_to_metadata(schema_dict: dict, table_name: str) -> dict:
    """
    기존 db_schema.py의 schema 딕셔너리를 ETL 메타데이터 형식으로 변환

    Args:
        schema_dict: db_schema.py가 반환하는 {table: {columns, pk, fk, ...}} 구조
        table_name:  변환할 테이블명

    Returns:
        {"table_name": ..., "columns": [...], "pk_columns": [...]}
    """
    tbl_info = schema_dict.get(table_name, {})
    col_list = tbl_info.get("columns", [])
    pk_set = set(tbl_info.get("pk", []))

    columns = []
    for col in col_list:
        if isinstance(col, dict):
            col_name = col.get("name", "").upper()
            dtype = col.get("type", "")
            nullable = col.get("nullable", True)
        else:
            col_name = str(col).upper()
            dtype = ""
            nullable = True

        columns.append({
            "name":        col_name,
            "type":        dtype,
            "pk":          col_name in pk_set,
            "nullable":    nullable,
            "description": "",
        })

    pk_columns = [c["name"] for c in columns if c["pk"]]

    return {
        "table_name": table_name.upper(),
        "columns":    columns,
        "pk_columns": pk_columns,
    }


def get_excel_sheets(file) -> list[str]:
    """Excel 파일의 시트 목록 반환"""
    try:
        xl = pd.ExcelFile(file)
        return xl.sheet_names
    except Exception:
        return []


def metadata_to_display(meta: dict) -> pd.DataFrame:
    """메타데이터를 Streamlit 표시용 DataFrame으로 변환"""
    rows = []
    for col in meta.get("columns", []):
        rows.append({
            "컬럼명":     col["name"],
            "데이터타입": col["type"],
            "PK":        "✓" if col["pk"] else "",
            "NULL허용":  "Y" if col["nullable"] else "N",
            "설명":      col["description"],
        })
    return pd.DataFrame(rows)
