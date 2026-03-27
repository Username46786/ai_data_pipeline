from __future__ import annotations

from pathlib import Path

import streamlit as st

from pipeline import build_step_options, run_pipeline, suggest_pipeline
from profiler import profile_dataframe
from utils import dataframe_download_name, dataframe_to_csv_bytes, load_csv_file, load_sample_dataframe


st.set_page_config(
    page_title="AI Data Pipeline Builder",
    page_icon="AI",
    layout="wide",
)


SAMPLE_DATA_PATH = Path("sample_data") / "messy_sales.csv"


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
            .stApp {
                background:
                    radial-gradient(circle at top left, rgba(8, 145, 178, 0.10), transparent 28%),
                    radial-gradient(circle at top right, rgba(22, 163, 74, 0.12), transparent 34%),
                    linear-gradient(180deg, #f8fafc 0%, #eef7f3 100%);
            }
            .hero-card, .panel-card, .status-card, .issue-card, .pipeline-card, .compare-card {
                background: rgba(255, 255, 255, 0.92);
                border: 1px solid rgba(148, 163, 184, 0.20);
                border-radius: 20px;
                box-shadow: 0 16px 45px rgba(15, 23, 42, 0.08);
            }
            .hero-card {
                padding: 1.7rem 1.8rem;
                margin-bottom: 1rem;
            }
            .panel-card, .status-card {
                padding: 1rem 1.1rem;
            }
            .issue-card {
                padding: 1rem;
                min-height: 132px;
            }
            .pipeline-card, .compare-card {
                padding: 1rem 1.1rem;
            }
            .eyebrow {
                font-size: 0.82rem;
                font-weight: 700;
                letter-spacing: 0.08em;
                color: #0f766e;
                text-transform: uppercase;
            }
            .hero-title {
                font-size: 2.2rem;
                font-weight: 800;
                color: #0f172a;
                line-height: 1.05;
                margin-top: 0.35rem;
            }
            .subtle-text {
                color: #475569;
                font-size: 0.96rem;
            }
            .section-title {
                font-size: 1.05rem;
                font-weight: 700;
                color: #0f172a;
                margin-bottom: 0.45rem;
            }
            .section-copy {
                color: #475569;
                font-size: 0.92rem;
                margin-bottom: 0.8rem;
            }
            .metric-label, .issue-label {
                color: #64748b;
                font-size: 0.82rem;
                font-weight: 600;
                text-transform: uppercase;
                letter-spacing: 0.03em;
            }
            .metric-value, .issue-value {
                color: #0f172a;
                font-size: 1.8rem;
                font-weight: 800;
                line-height: 1.05;
                margin-top: 0.25rem;
            }
            .metric-sub, .issue-sub {
                color: #475569;
                font-size: 0.86rem;
                margin-top: 0.35rem;
                line-height: 1.35;
            }
            .status-strip {
                border-radius: 16px;
                padding: 0.9rem 1rem;
                background: linear-gradient(135deg, #0f172a, #1e293b);
                color: #e2e8f0;
                margin-bottom: 1rem;
            }
            .step-pill {
                display: inline-block;
                padding: 0.32rem 0.62rem;
                border-radius: 999px;
                background: #ecfeff;
                border: 1px solid #a5f3fc;
                color: #155e75;
                font-size: 0.82rem;
                font-weight: 600;
                margin: 0 0.4rem 0.45rem 0;
            }
            .suggestion-banner {
                border-radius: 18px;
                padding: 1rem 1.05rem;
                background: linear-gradient(135deg, #ecfeff, #f0fdf4);
                border: 1px solid rgba(45, 212, 191, 0.35);
                margin-bottom: 0.9rem;
            }
            .suggestion-title {
                color: #0f172a;
                font-size: 0.95rem;
                font-weight: 700;
                margin-bottom: 0.35rem;
            }
            .suggestion-copy {
                color: #475569;
                font-size: 0.9rem;
                line-height: 1.45;
            }
            .step-flow {
                display: flex;
                flex-direction: column;
                gap: 0.7rem;
                margin-top: 0.2rem;
            }
            .step-row {
                display: flex;
                align-items: flex-start;
                gap: 0.8rem;
                padding: 0.8rem 0.85rem;
                border-radius: 16px;
                background: #ffffff;
                border: 1px solid rgba(148, 163, 184, 0.18);
            }
            .step-index {
                width: 30px;
                height: 30px;
                border-radius: 999px;
                background: #0f766e;
                color: #ffffff;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 0.82rem;
                font-weight: 700;
                flex: 0 0 auto;
                margin-top: 0.05rem;
            }
            .step-name {
                color: #0f172a;
                font-size: 0.95rem;
                font-weight: 700;
            }
            .step-description {
                color: #475569;
                font-size: 0.87rem;
                margin-top: 0.15rem;
                line-height: 1.4;
            }
            .log-box {
                background: #0f172a;
                color: #e2e8f0;
                border-radius: 16px;
                padding: 0.95rem 1rem;
                font-family: Consolas, monospace;
                font-size: 0.85rem;
                line-height: 1.55;
                white-space: pre-wrap;
            }
            div[data-testid="stDataFrame"] {
                border-radius: 16px;
                overflow: hidden;
                border: 1px solid rgba(148, 163, 184, 0.18);
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_stat_card(label: str, value: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <div class="status-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-sub">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_issue_card(label: str, value: str, subtitle: str) -> None:
    st.markdown(
        f"""
        <div class="issue-card">
            <div class="issue-label">{label}</div>
            <div class="issue-value">{value}</div>
            <div class="issue-sub">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_log(log_entries: list[str]) -> None:
    if not log_entries:
        st.info("Run the pipeline to generate a transformation log.")
        return

    st.markdown(
        f'<div class="log-box">{"<br>".join(log_entries)}</div>',
        unsafe_allow_html=True,
    )


def _render_selected_steps(selected_steps: list[str], step_options: dict) -> None:
    if not selected_steps:
        st.warning("No pipeline steps selected yet.")
        return

    pills = "".join(
        f'<span class="step-pill">{step_options[step]["label"]}</span>'
        for step in selected_steps
        if step in step_options
    )
    st.markdown(pills, unsafe_allow_html=True)


def _render_pipeline_flow(selected_steps: list[str], step_options: dict) -> None:
    if not selected_steps:
        st.info("No pipeline steps selected yet. Click `Suggest Pipeline` or choose steps manually.")
        return

    descriptions = {
        "standardize_column_names": "Normalizes headers into a consistent format that is easier to work with.",
        "trim_whitespace": "Removes stray spaces from text fields so values match more reliably.",
        "remove_duplicates": "Drops exact duplicate records from the dataset.",
        "fill_missing_numeric": "Fills empty numeric cells using a fixed summary statistic.",
        "fill_missing_text": "Replaces missing or blank text values with a predictable fallback label.",
        "parse_date_columns": "Converts likely date fields into a consistent date format.",
        "filter_rows": "Keeps only rows that match a pandas query expression.",
        "sort_rows": "Orders the final dataset for easier review and export.",
    }

    rows = []
    for index, step in enumerate(selected_steps, start=1):
        if step not in step_options:
            continue
        rows.append(
            f"""
            <div class="step-row">
                <div class="step-index">{index}</div>
                <div>
                    <div class="step-name">{step_options[step]["label"]}</div>
                    <div class="step-description">{descriptions.get(step, "Pipeline step")}</div>
                </div>
            </div>
            """
        )

    st.markdown(f'<div class="step-flow">{"".join(rows)}</div>', unsafe_allow_html=True)


def _default_step_params(step_options: dict) -> dict:
    defaults = {}
    for step_name, config in step_options.items():
        defaults[step_name] = {}
        for key, spec in config["params"].items():
            defaults[step_name][key] = spec["default"]
    return defaults


def _load_dataframe() -> tuple[pd.DataFrame | None, str, str | None]:
    source = st.radio(
        "Choose a dataset",
        options=("Use bundled sample data", "Upload a CSV"),
        horizontal=True,
    )

    try:
        if source == "Use bundled sample data":
            if not SAMPLE_DATA_PATH.exists():
                return None, "sample", "The bundled sample dataset could not be found."
            return load_sample_dataframe(SAMPLE_DATA_PATH), "sample", None

        uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])
        if uploaded_file is None:
            return None, "upload", None
        return load_csv_file(uploaded_file), uploaded_file.name, None
    except ValueError as exc:
        return None, "error", str(exc)
    except Exception:
        return None, "error", "Unable to load the dataset. Please try a different CSV file."


def _render_issue_summary_cards(profile: dict) -> None:
    column_name_issue_count = len(profile.get("column_name_issues", []))
    issue_cols = st.columns(4)
    with issue_cols[0]:
        _render_issue_card(
            "Missing Values",
            f"{profile['missing_total']:,}",
            "Nulls detected across all columns.",
        )
    with issue_cols[1]:
        _render_issue_card(
            "Duplicate Rows",
            f"{profile['duplicate_rows']:,}",
            "Fully duplicated records in the dataset.",
        )
    with issue_cols[2]:
        _render_issue_card(
            "Blank Strings",
            f"{profile['blank_string_total']:,}",
            "Whitespace-only text values in text columns.",
        )
    with issue_cols[3]:
        _render_issue_card(
            "Column Name Issues",
            str(column_name_issue_count),
            "Headers that could be standardized for consistency.",
        )


def _render_suggested_pipeline(suggested_steps: list[str], step_options: dict) -> None:
    if not suggested_steps:
        st.info("No suggested steps were generated for this dataset.")
        return

    labels = [step_options[step]["label"] for step in suggested_steps if step in step_options]
    st.markdown(
        f"""
        <div class="suggestion-banner">
            <div class="suggestion-title">Suggested pipeline</div>
            <div class="suggestion-copy">
                Based on the detected issues, the app recommends this cleanup path:
                <strong>{" -> ".join(labels)}</strong>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_column_findings(profile: dict) -> None:
    findings = []

    missing_columns = [name for name, count in profile["missing_by_column"].items() if count > 0]
    if missing_columns:
        findings.append("Missing values by column: " + ", ".join(missing_columns[:8]))

    blank_columns = list(profile.get("blank_strings_by_column", {}).keys())
    if blank_columns:
        findings.append("Blank strings found in: " + ", ".join(blank_columns[:8]))

    if profile.get("column_name_issues"):
        preview = ", ".join(
            f"{item['original']} -> {item['suggested']}"
            for item in profile["column_name_issues"][:4]
        )
        findings.append("Suggested header cleanup: " + preview)

    if profile["likely_date_columns"]:
        findings.append("Likely date columns: " + ", ".join(profile["likely_date_columns"]))

    if not findings:
        st.success("No major profiling issues were found. The dataset already looks fairly clean.")
        return

    for item in findings:
        st.write(f"- {item}")


def _render_step_inputs(selected_steps: list[str], step_options: dict, state_key: str) -> dict:
    configured_steps = st.session_state[state_key]["configured_steps"]

    for step_name in selected_steps:
        configured_steps.setdefault(step_name, {})
        for key, spec in step_options[step_name]["params"].items():
            widget_key = f"{state_key}::{step_name}::{key}"
            label = f"{step_options[step_name]['label']} - {spec['label']}"
            current_value = configured_steps[step_name].get(key, spec["default"])

            if spec["type"] == "multiselect":
                configured_steps[step_name][key] = st.multiselect(
                    label,
                    options=spec["options"],
                    default=current_value,
                    key=widget_key,
                )
            elif spec["type"] == "selectbox":
                if current_value not in spec["options"]:
                    current_value = spec["default"]
                configured_steps[step_name][key] = st.selectbox(
                    label,
                    options=spec["options"],
                    index=spec["options"].index(current_value),
                    key=widget_key,
                )
            elif spec["type"] == "text":
                configured_steps[step_name][key] = st.text_input(
                    label,
                    value=current_value,
                    placeholder=spec.get("placeholder", ""),
                    key=widget_key,
                )
            elif spec["type"] == "number":
                configured_steps[step_name][key] = st.number_input(
                    label,
                    value=current_value,
                    step=spec.get("step", 1),
                    key=widget_key,
                )

    return configured_steps


def main() -> None:
    _inject_styles()

    st.markdown(
        """
        <div class="hero-card">
            <div class="eyebrow">Deterministic Data Cleanup</div>
            <div class="hero-title">AI Data Pipeline Builder</div>
            <div class="subtle-text" style="margin-top:0.75rem;max-width:760px;">
                Upload a CSV or use the bundled sample dataset, review rule-based quality checks, run a simple cleanup pipeline, and download a cleaned result.
            </div>
            <div class="subtle-text" style="margin-top:0.4rem;max-width:760px;">
                Built for demo readiness with pandas-only profiling and fixed transformation steps.
            </div>
            <div class="subtle-text" style="margin-top:0.55rem;max-width:760px;font-weight:600;color:#0f766e;">
                Turn messy CSVs into reviewable, cleaned datasets in a single guided workflow.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    df, source_name, load_error = _load_dataframe()
    if load_error:
        st.error(load_error)
        return

    if df is None:
        st.info("Choose the sample dataset or upload a CSV file to start profiling, cleanup, and export.")
        return

    if df.empty:
        st.warning("The selected dataset has headers but no rows. Upload a CSV with at least one data row to continue.")
        return

    try:
        profile = profile_dataframe(df)
    except Exception:
        st.error("The dataset loaded, but profiling could not be completed. Please try a simpler CSV format.")
        return

    step_options = build_step_options(df, profile)
    suggested_steps = suggest_pipeline(profile)
    state_key = f"pipeline_state::{source_name}"

    if state_key not in st.session_state:
        st.session_state[state_key] = {
            "selected_steps": suggested_steps,
            "configured_steps": _default_step_params(step_options),
            "has_run": False,
            "cleaned_df": df.copy(),
            "log_entries": [],
            "before_row_count": len(df),
            "after_row_count": len(df),
            "pipeline_error": None,
        }

    pipeline_state = st.session_state[state_key]

    st.markdown('<div class="section-title">Dataset Snapshot</div>', unsafe_allow_html=True)
    stat_cols = st.columns(4)
    with stat_cols[0]:
        _render_stat_card("Rows", f"{profile['row_count']:,}", "Rows available for profiling and cleanup.")
    with stat_cols[1]:
        _render_stat_card("Columns", str(profile["column_count"]), "Detected fields in the source dataset.")
    with stat_cols[2]:
        _render_stat_card("Rule-Based Issues", str(profile["issue_count"]), "Quality signals flagged by the profiler.")
    with stat_cols[3]:
        _render_stat_card("Likely Date Columns", str(len(profile["likely_date_columns"])), "Columns suitable for date parsing.")

    left_col, right_col = st.columns([1.05, 0.95], gap="large")

    with left_col:
        st.markdown('<div class="panel-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Issue Summary</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-copy">A quick quality snapshot of the uploaded dataset, surfaced by deterministic profiling rules.</div>',
            unsafe_allow_html=True,
        )
        st.caption(f"Source: `{source_name}`")
        _render_issue_summary_cards(profile)
        st.markdown("")
        _render_column_findings(profile)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("")
        st.markdown('<div class="panel-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Raw Data Preview</div>', unsafe_allow_html=True)
        st.dataframe(df.head(15), width="stretch")
        st.markdown("</div>", unsafe_allow_html=True)

    with right_col:
        st.markdown('<div class="panel-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Pipeline Builder</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-copy">Review the recommended cleanup path, adjust any step settings, and run the transformation pipeline when ready.</div>',
            unsafe_allow_html=True,
        )

        _render_suggested_pipeline(suggested_steps, step_options)

        if st.button("Suggest Pipeline", type="primary", width="stretch"):
            pipeline_state["selected_steps"] = suggested_steps
            defaults = _default_step_params(step_options)
            for step_name in suggested_steps:
                pipeline_state["configured_steps"][step_name] = defaults.get(step_name, {})
            pipeline_state["has_run"] = False
            pipeline_state["pipeline_error"] = None

        selected_steps = st.multiselect(
            "Selected cleanup steps",
            options=list(step_options.keys()),
            default=pipeline_state["selected_steps"],
            help="These are fixed, rule-based cleanup operations that can be applied in sequence.",
        )
        pipeline_state["selected_steps"] = selected_steps

        st.markdown("**Selected pipeline**")
        _render_selected_steps(selected_steps, step_options)

        st.markdown("")
        st.markdown("**Step-by-step flow**")
        _render_pipeline_flow(selected_steps, step_options)

        with st.expander("Review step settings", expanded=True):
            if not selected_steps:
                st.info("Select one or more steps to configure the pipeline. The suggested flow is a good starting point.")
            else:
                _render_step_inputs(selected_steps, step_options, state_key)

        if st.button("Run Pipeline", width="stretch"):
            if not selected_steps:
                pipeline_state["pipeline_error"] = "Select at least one pipeline step before running."
                pipeline_state["has_run"] = False
            else:
                try:
                    cleaned_df, log_entries, before_rows, after_rows = run_pipeline(
                        df,
                        selected_steps,
                        pipeline_state["configured_steps"],
                    )
                    pipeline_state["cleaned_df"] = cleaned_df
                    pipeline_state["log_entries"] = log_entries
                    pipeline_state["before_row_count"] = before_rows
                    pipeline_state["after_row_count"] = after_rows
                    pipeline_state["has_run"] = True
                    pipeline_state["pipeline_error"] = None
                except Exception as exc:
                    pipeline_state["pipeline_error"] = f"Unable to run the pipeline: {exc}"
                    pipeline_state["has_run"] = False

        if pipeline_state["pipeline_error"]:
            st.error(pipeline_state["pipeline_error"])
        elif not selected_steps:
            st.warning("Choose at least one cleanup step before running the pipeline.")

        st.markdown("</div>", unsafe_allow_html=True)

    cleaned_df = pipeline_state["cleaned_df"] if pipeline_state["has_run"] else df.copy()
    log_entries = pipeline_state["log_entries"] if pipeline_state["has_run"] else []

    st.markdown("")
    if pipeline_state["has_run"]:
        st.markdown(
            f"""
            <div class="status-strip">
                Pipeline run complete. Row count changed from <strong>{pipeline_state["before_row_count"]:,}</strong>
                to <strong>{pipeline_state["after_row_count"]:,}</strong>.
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.info("Nothing has been transformed yet. Review the selected steps, then click `Run Pipeline`.")

    log_col, preview_col = st.columns([0.95, 1.05], gap="large")
    with log_col:
        st.markdown('<div class="pipeline-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Transformation Log</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-copy">Every pipeline step records what happened, including row-count changes along the way.</div>',
            unsafe_allow_html=True,
        )
        _render_log(log_entries)
        st.markdown("</div>", unsafe_allow_html=True)
    with preview_col:
        st.markdown('<div class="compare-card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Before and After Comparison</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="section-copy">Compare a sample of the original dataset with the cleaned output produced by the selected pipeline.</div>',
            unsafe_allow_html=True,
        )
        compare_cols = st.columns(2)
        with compare_cols[0]:
            st.caption("Before")
            st.dataframe(df.head(12), width="stretch")
        with compare_cols[1]:
            st.caption("After")
            st.dataframe(cleaned_df.head(12), width="stretch")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("")
    st.markdown('<div class="section-title">Cleaned Dataset Preview</div>', unsafe_allow_html=True)
    if pipeline_state["has_run"]:
        st.dataframe(cleaned_df.head(50), width="stretch", height=320)
        st.download_button(
            "Download Cleaned CSV",
            data=dataframe_to_csv_bytes(cleaned_df),
            file_name=dataframe_download_name(source_name),
            mime="text/csv",
            width="stretch",
        )
    else:
        st.dataframe(df.head(50), width="stretch", height=320)
        st.caption("Run the pipeline to generate the cleaned dataset and enable CSV download.")


if __name__ == "__main__":
    main()
