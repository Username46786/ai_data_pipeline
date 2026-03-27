from __future__ import annotations

from typing import Any

import pandas as pd

from transforms import (
    fill_missing_numeric,
    fill_missing_text,
    filter_rows,
    parse_date_columns,
    remove_duplicates,
    sort_rows,
    standardize_column_names,
    trim_whitespace,
)


PipelineStep = dict[str, Any]


STEP_REGISTRY = {
    "standardize_column_names": standardize_column_names,
    "trim_whitespace": trim_whitespace,
    "remove_duplicates": remove_duplicates,
    "fill_missing_numeric": fill_missing_numeric,
    "fill_missing_text": fill_missing_text,
    "parse_date_columns": parse_date_columns,
    "filter_rows": filter_rows,
    "sort_rows": sort_rows,
}


DEFAULT_PIPELINE_CONFIG = [
    "standardize_column_names",
    "trim_whitespace",
    "remove_duplicates",
    "fill_missing_numeric",
    "fill_missing_text",
    "parse_date_columns",
]


def create_pipeline_step(name: str, params: dict | None = None) -> PipelineStep:
    return {
        "name": name,
        "params": params or {},
    }


def _normalize_steps(selected_steps: list[str] | list[PipelineStep], configured_steps: dict) -> list[PipelineStep]:
    normalized_steps: list[PipelineStep] = []

    for step in selected_steps:
        if isinstance(step, dict):
            name = step.get("name", "")
            params = step.get("params", {})
        else:
            name = str(step)
            params = configured_steps.get(name, {})

        normalized_steps.append(create_pipeline_step(name, params))

    return normalized_steps


def build_step_options(df: pd.DataFrame, profile: dict) -> dict:
    return {
        "standardize_column_names": {
            "label": "Standardize column names",
            "params": {},
        },
        "trim_whitespace": {
            "label": "Trim whitespace",
            "params": {},
        },
        "remove_duplicates": {
            "label": "Remove duplicates",
            "params": {},
        },
        "fill_missing_numeric": {
            "label": "Fill missing numeric values",
            "params": {
                "strategy": {
                    "type": "selectbox",
                    "label": "Strategy",
                    "options": ["median", "mean"],
                    "default": "median",
                }
            },
        },
        "fill_missing_text": {
            "label": "Fill missing text values",
            "params": {
                "fill_value": {
                    "type": "text",
                    "label": "Text fill value",
                    "default": "Unknown",
                    "placeholder": "Unknown",
                }
            },
        },
        "parse_date_columns": {
            "label": "Parse date columns",
            "params": {
                "columns": {
                    "type": "multiselect",
                    "label": "Columns",
                    "options": list(df.columns),
                    "default": profile["likely_date_columns"],
                }
            },
        },
        "filter_rows": {
            "label": "Filter rows",
            "params": {
                "query": {
                    "type": "text",
                    "label": "Pandas query expression",
                    "default": "",
                    "placeholder": "sales > 100 and region == 'West'",
                }
            },
        },
        "sort_rows": {
            "label": "Sort rows",
            "params": {
                "sort_by": {
                    "type": "selectbox",
                    "label": "Sort by",
                    "options": [""] + list(df.columns),
                    "default": "",
                },
                "ascending": {
                    "type": "selectbox",
                    "label": "Direction",
                    "options": ["Ascending", "Descending"],
                    "default": "Ascending",
                },
            },
        },
    }


def suggest_pipeline(profile: dict) -> list[str]:
    suggestions: list[str] = []

    if profile["inconsistent_column_names"]:
        suggestions.append("standardize_column_names")
    if profile["blank_string_total"]:
        suggestions.append("trim_whitespace")
    if profile["duplicate_rows"]:
        suggestions.append("remove_duplicates")
    if profile["numeric_columns"] and profile["missing_total"]:
        suggestions.append("fill_missing_numeric")
    if profile["text_columns"] and (profile["missing_total"] or profile["blank_string_total"]):
        suggestions.append("fill_missing_text")
    if profile["likely_date_columns"]:
        suggestions.append("parse_date_columns")

    return suggestions or DEFAULT_PIPELINE_CONFIG[:2]


def run_pipeline(
    df: pd.DataFrame,
    selected_steps: list[str] | list[PipelineStep],
    configured_steps: dict | None = None,
) -> tuple[pd.DataFrame, list[str], int, int]:
    configured_steps = configured_steps or {}
    normalized_steps = _normalize_steps(selected_steps, configured_steps)

    result = df.copy()
    log_entries: list[str] = []
    before_row_count = len(result)

    for step in normalized_steps:
        step_name = step["name"]
        step_params = step.get("params", {})
        transform = STEP_REGISTRY.get(step_name)

        if transform is None:
            log_entries.append(f"Skipped '{step_name}' because the step is not supported")
            continue

        try:
            step_before_rows = len(result)
            result, message = transform(result, **step_params)
            step_after_rows = len(result)
            log_entries.append(f"{step_name}: {message} ({step_before_rows} -> {step_after_rows} rows)")
        except Exception as exc:
            log_entries.append(f"Skipped '{step_name}' because it could not be applied: {exc}")

    after_row_count = len(result)
    return result, log_entries, before_row_count, after_row_count
