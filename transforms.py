from __future__ import annotations

import re

import pandas as pd


def _is_text_series(series: pd.Series) -> bool:
    return pd.api.types.is_string_dtype(series) or series.dtype == "object"


def _normalize_column_name(name: str) -> str:
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", str(name).strip().lower())
    normalized = re.sub(r"_+", "_", cleaned).strip("_")
    return normalized or "column"


def _resolve_column_name(df: pd.DataFrame, column_name: str) -> str | None:
    if column_name in df.columns:
        return column_name

    normalized_target = _normalize_column_name(column_name)
    for existing_column in df.columns:
        if _normalize_column_name(existing_column) == normalized_target:
            return existing_column

    return None


def standardize_column_names(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    result = df.copy()
    original_columns = [str(column) for column in result.columns]
    normalized_columns = [_normalize_column_name(column) for column in original_columns]
    result.columns = normalized_columns

    renamed_count = sum(
        1 for original, normalized in zip(original_columns, normalized_columns) if original != normalized
    )
    return result, f"Standardized {renamed_count} column names"


def trim_whitespace(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    result = df.copy()
    trimmed_columns = 0

    for column in result.columns:
        series = result[column]
        if not _is_text_series(series):
            continue

        updated = series.apply(lambda value: value.strip() if isinstance(value, str) else value)
        if not updated.equals(series):
            trimmed_columns += 1
        result[column] = updated

    return result, f"Trimmed whitespace in {trimmed_columns} text columns"


def remove_duplicates(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    result = df.drop_duplicates().reset_index(drop=True).copy()
    removed_rows = len(df) - len(result)
    return result, f"Removed {removed_rows} duplicate rows"


def fill_missing_numeric(
    df: pd.DataFrame,
    strategy: str = "median",
    columns: list[str] | None = None,
) -> tuple[pd.DataFrame, str]:
    result = df.copy()
    candidate_columns = columns or list(result.columns)
    filled_columns = 0

    for column in candidate_columns:
        if column not in result.columns:
            continue

        series = result[column]
        if not pd.api.types.is_numeric_dtype(series):
            continue
        if not series.isna().any():
            continue

        if strategy == "mean":
            fill_value = series.mean()
        else:
            fill_value = series.median()

        if pd.isna(fill_value):
            fill_value = 0

        result[column] = series.fillna(fill_value)
        filled_columns += 1

    return result, f"Filled numeric missing values in {filled_columns} columns using {strategy}"


def fill_missing_text(
    df: pd.DataFrame,
    fill_value: str = "Unknown",
    columns: list[str] | None = None,
) -> tuple[pd.DataFrame, str]:
    result = df.copy()
    candidate_columns = columns or list(result.columns)
    filled_columns = 0

    for column in candidate_columns:
        if column not in result.columns:
            continue

        series = result[column]
        if not _is_text_series(series):
            continue

        blank_mask = series.apply(lambda value: isinstance(value, str) and value.strip() == "")
        if not (series.isna().any() or blank_mask.any()):
            continue

        updated = series.mask(blank_mask, pd.NA).fillna(fill_value)
        result[column] = updated
        filled_columns += 1

    return result, f"Filled text missing values in {filled_columns} columns"


def parse_date_columns(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    output_format: str = "%Y-%m-%d",
) -> tuple[pd.DataFrame, str]:
    result = df.copy()
    candidate_columns = columns or list(result.columns)
    parsed_columns = 0

    for column in candidate_columns:
        resolved_column = _resolve_column_name(result, column)
        if resolved_column is None:
            continue

        parsed = pd.to_datetime(result[resolved_column], errors="coerce", format="mixed")
        if parsed.notna().sum() == 0:
            continue

        result[resolved_column] = parsed.dt.strftime(output_format).where(parsed.notna(), "")
        parsed_columns += 1

    return result, f"Parsed {parsed_columns} date columns"


def filter_rows(df: pd.DataFrame, query: str = "") -> tuple[pd.DataFrame, str]:
    if not query.strip():
        return df.copy(), "No row filter applied"

    try:
        result = df.query(query).reset_index(drop=True).copy()
        return result, f"Filtered rows with query: {query}"
    except Exception:
        return df.copy(), "Skipped row filter because the query was invalid"


def sort_rows(
    df: pd.DataFrame,
    sort_by: str = "",
    ascending: bool | str = True,
) -> tuple[pd.DataFrame, str]:
    if not sort_by or sort_by not in df.columns:
        return df.copy(), "No sorting applied"

    is_ascending = ascending
    if isinstance(ascending, str):
        is_ascending = ascending.lower() == "ascending"

    result = df.sort_values(by=sort_by, ascending=bool(is_ascending)).reset_index(drop=True).copy()
    direction = "ascending" if bool(is_ascending) else "descending"
    return result, f"Sorted rows by {sort_by} ({direction})"


def fill_missing_numeric_values(
    df: pd.DataFrame,
    strategy: str = "median",
) -> tuple[pd.DataFrame, str]:
    return fill_missing_numeric(df, strategy=strategy)


def fill_missing_text_values(
    df: pd.DataFrame,
    fill_value: str = "Unknown",
) -> tuple[pd.DataFrame, str]:
    return fill_missing_text(df, fill_value=fill_value)
