from __future__ import annotations

import re

import pandas as pd


DATE_NAME_TOKENS = ("date", "time", "day", "month", "year")
COMMON_DATE_FORMATS = (
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%b %d, %Y",
    "%b %d %Y",
    "%B %d, %Y",
)


def _is_text_series(series: pd.Series) -> bool:
    return pd.api.types.is_string_dtype(series) or series.dtype == "object"


def _blank_string_mask(series: pd.Series) -> pd.Series:
    if not _is_text_series(series):
        return pd.Series(False, index=series.index)
    return series.apply(lambda value: isinstance(value, str) and value.strip() == "")


def _blank_string_count(series: pd.Series) -> int:
    return int(_blank_string_mask(series).sum())


def _missing_count(series: pd.Series) -> int:
    return int(series.isna().sum())


def _missing_or_blank_count(series: pd.Series) -> int:
    return _missing_count(series) + _blank_string_count(series)


def _standardized_column_name(column_name: str) -> str:
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", str(column_name).strip().lower())
    return re.sub(r"_+", "_", cleaned).strip("_")


def _column_name_issue(column_name: str) -> str | None:
    normalized = str(column_name)
    suggested = _standardized_column_name(normalized)
    if normalized == suggested and suggested:
        return None
    return suggested or "column"


def _is_likely_date_column(column_name: str, series: pd.Series) -> bool:
    lowered_name = str(column_name).lower()
    if any(token in lowered_name for token in DATE_NAME_TOKENS):
        return True

    if pd.api.types.is_datetime64_any_dtype(series):
        return True

    if not _is_text_series(series):
        return False

    non_null = series.dropna()
    if non_null.empty:
        return False

    cleaned = non_null.astype(str).str.strip()
    cleaned = cleaned[cleaned != ""]
    if cleaned.empty:
        return False

    sample = cleaned.head(min(30, len(cleaned)))
    best_match_rate = 0.0
    for date_format in COMMON_DATE_FORMATS:
        parsed = pd.to_datetime(sample, errors="coerce", format=date_format)
        best_match_rate = max(best_match_rate, float(parsed.notna().mean()))
        if best_match_rate >= 0.6:
            return True

    fallback = pd.to_datetime(sample, errors="coerce", format="mixed")
    return bool(fallback.notna().mean() >= 0.6)


def _is_likely_numeric_column(series: pd.Series) -> bool:
    if pd.api.types.is_numeric_dtype(series):
        return True

    if not _is_text_series(series):
        return False

    non_null = series.dropna()
    if non_null.empty:
        return False

    cleaned = (
        non_null.astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.replace("%", "", regex=False)
    )
    if cleaned.empty:
        return False

    parsed = pd.to_numeric(cleaned, errors="coerce")
    return bool(parsed.notna().mean() >= 0.8)


def _is_likely_categorical_column(series: pd.Series, row_count: int) -> bool:
    if row_count == 0:
        return False

    unique_count = int(series.nunique(dropna=True))
    if unique_count == 0:
        return False

    if pd.api.types.is_bool_dtype(series):
        return True

    if pd.api.types.is_numeric_dtype(series):
        return unique_count <= min(20, max(3, row_count // 10))

    if _is_text_series(series):
        return unique_count <= min(30, max(5, row_count // 2))

    return False


def profile_dataframe(df: pd.DataFrame) -> dict:
    row_count, column_count = df.shape
    duplicate_row_count = int(df.duplicated().sum())

    columns_summary = []
    missing_by_column: dict[str, int] = {}
    blank_strings_by_column: dict[str, int] = {}
    inconsistent_column_names: list[dict[str, str]] = []
    likely_numeric_columns: list[str] = []
    likely_categorical_columns: list[str] = []
    likely_date_columns: list[str] = []

    for column in df.columns:
        series = df[column]
        missing_count = _missing_count(series)
        blank_string_count = _blank_string_count(series)
        missing_or_blank = _missing_or_blank_count(series)
        unique_values = int(series.nunique(dropna=True))

        suggested_name = _column_name_issue(column)
        if suggested_name is not None:
            inconsistent_column_names.append(
                {
                    "original": str(column),
                    "suggested": suggested_name,
                }
            )

        if _is_likely_numeric_column(series):
            likely_numeric_columns.append(str(column))
        if _is_likely_categorical_column(series, row_count):
            likely_categorical_columns.append(str(column))
        if _is_likely_date_column(str(column), series):
            likely_date_columns.append(str(column))

        missing_by_column[str(column)] = missing_count
        if blank_string_count:
            blank_strings_by_column[str(column)] = blank_string_count

        columns_summary.append(
            {
                "column": str(column),
                "dtype": str(series.dtype),
                "missing_count": missing_count,
                "blank_string_count": blank_string_count,
                "missing_or_blank_count": missing_or_blank,
                "unique_values": unique_values,
                "likely_numeric": str(column) in likely_numeric_columns,
                "likely_categorical": str(column) in likely_categorical_columns,
                "likely_date": str(column) in likely_date_columns,
            }
        )

    missing_total = sum(missing_by_column.values())
    blank_string_total = sum(blank_strings_by_column.values())
    issue_count = sum(
        [
            1 if missing_total else 0,
            1 if duplicate_row_count else 0,
            1 if blank_string_total else 0,
            1 if inconsistent_column_names else 0,
            1 if likely_date_columns else 0,
        ]
    )

    issues = {
        "missing_values": {
            "total": missing_total,
            "by_column": missing_by_column,
        },
        "duplicate_rows": duplicate_row_count,
        "blank_strings": {
            "total": blank_string_total,
            "by_column": blank_strings_by_column,
        },
        "inconsistent_column_names": {
            "count": len(inconsistent_column_names),
            "details": inconsistent_column_names,
        },
    }

    inferred_column_types = {
        "numeric": likely_numeric_columns,
        "categorical": likely_categorical_columns,
        "date": likely_date_columns,
    }

    return {
        "shape": {"rows": row_count, "columns": column_count},
        "row_count": row_count,
        "column_count": column_count,
        "columns": columns_summary,
        "issues": issues,
        "inferred_column_types": inferred_column_types,
        "duplicate_rows": duplicate_row_count,
        "missing_total": missing_total,
        "missing_by_column": missing_by_column,
        "blank_string_total": blank_string_total,
        "blank_strings_by_column": blank_strings_by_column,
        "inconsistent_column_names": bool(inconsistent_column_names),
        "column_name_issues": inconsistent_column_names,
        "likely_numeric_columns": likely_numeric_columns,
        "likely_categorical_columns": likely_categorical_columns,
        "likely_date_columns": likely_date_columns,
        "numeric_columns": likely_numeric_columns,
        "categorical_columns": likely_categorical_columns,
        "text_columns": list(df.select_dtypes(include=["object", "string"]).columns),
        "issue_count": issue_count,
    }
