from __future__ import annotations

from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError, ParserError


def load_sample_dataframe(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except EmptyDataError as exc:
        raise ValueError("The bundled sample dataset is empty.") from exc
    except ParserError as exc:
        raise ValueError("The bundled sample dataset could not be parsed as CSV.") from exc


def load_csv_file(uploaded_file) -> pd.DataFrame:
    try:
        return pd.read_csv(uploaded_file)
    except EmptyDataError as exc:
        raise ValueError("The uploaded CSV is empty. Please choose a file with headers and rows.") from exc
    except ParserError as exc:
        raise ValueError(
            "The uploaded file could not be parsed as a valid CSV. Please check the delimiter, quotes, and file format."
        ) from exc


def format_issue_summary(profile: dict) -> list[str]:
    summary = []
    if profile["missing_total"]:
        summary.append(f"Missing or blank values detected: {profile['missing_total']}")
    if profile["duplicate_rows"]:
        summary.append(f"Duplicate rows detected: {profile['duplicate_rows']}")
    if profile["blank_string_total"]:
        summary.append(f"Blank strings detected: {profile['blank_string_total']}")
    if profile["inconsistent_column_names"]:
        summary.append("Column names look inconsistent and could be standardized.")
    if profile["likely_date_columns"]:
        summary.append(
            "Likely date columns: " + ", ".join(map(str, profile["likely_date_columns"]))
        )
    return summary


def summarize_pipeline_steps(selected_steps: list[str], step_options: dict) -> str:
    if not selected_steps:
        return "No steps selected."
    labels = [step_options[step]["label"] for step in selected_steps if step in step_options]
    if not labels:
        return "No steps selected."
    return " -> ".join(labels)


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def dataframe_download_name(source_name: str) -> str:
    stem = Path(source_name).stem if source_name else "cleaned_dataset"
    return f"{stem}_cleaned.csv"
