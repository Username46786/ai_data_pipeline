# AI Data Pipeline Builder

## Project Overview

AI Data Pipeline Builder is a polished Streamlit app that helps users turn messy CSV files into cleaner, more analysis-ready datasets. The app profiles uploaded data, highlights quality issues, suggests a deterministic cleanup pipeline, and lets users review and run each step before downloading the cleaned result.

This project is intentionally designed to be simple, reliable, and demo-ready. It uses only pandas for profiling and transformations, with rule-based suggestions instead of freeform or LLM-generated actions.

## Demo Link

https://ai-data-pipeline-osgq.onrender.com

## Target User

This app is built for:

- analysts working with exported CSV files
- operations teams cleaning recurring reports
- business users preparing data for dashboards
- hackathon judges and demo audiences who want to understand the workflow quickly

It is especially useful for people who need a practical cleanup tool without writing pandas code by hand.

## Problem Solved

Real-world CSV files are often messy before they are useful. They may contain:

- missing values
- duplicate rows
- blank strings
- inconsistent column names
- date columns stored as text
- inconsistent formatting that makes analysis harder

Cleaning that data manually is repetitive, error-prone, and slow. AI Data Pipeline Builder turns that process into a guided workflow that helps users identify problems, review a recommended cleanup path, and produce a cleaner dataset in minutes.

## How It Works

1. The user loads the bundled sample dataset or uploads a CSV.
2. The app profiles the data using deterministic pandas-based rules.
3. The app surfaces issue summary cards and column-level findings.
4. A suggested cleanup pipeline is generated from the detected issues.
5. The user reviews the selected steps and optional settings.
6. The pipeline runs in order and logs each transformation.
7. The app shows before/after previews and provides a cleaned CSV download.

## Core Features

- CSV upload plus a bundled messy sample dataset
- Raw data preview for quick inspection
- Rule-based issue detection for:
  - missing values
  - duplicate rows
  - blank strings
  - inconsistent column names
  - likely numeric, categorical, and date columns
- Deterministic suggested pipeline based on detected issues
- Reusable cleanup steps:
  - standardize column names
  - trim whitespace
  - remove duplicates
  - fill missing numeric values
  - fill missing text values
  - parse date columns
  - filter rows
  - sort rows
- Transformation log with row-count changes
- Before/after comparison views
- Cleaned CSV download
- Friendly empty states and validation messages

## Local Setup

### Requirements

- Python 3.10+
- pip

### Install

```bash
pip install -r requirements.txt
```

### Run

```bash
streamlit run app.py
```

Then open the local Streamlit URL shown in the terminal.

## Demo Walkthrough

1. Launch the app.
2. Select **Use bundled sample data**.
3. Review the issue summary cards to see the detected problems.
4. Inspect the visible suggested pipeline section.
5. Click **Suggest Pipeline** to auto-select the recommended cleanup steps.
6. Review the step-by-step pipeline display and optional settings.
7. Click **Run Pipeline**.
8. Review the transformation log.
9. Compare the before and after previews.
10. Download the cleaned CSV.

## Why This Improves a Data Workflow

This app improves a data workflow by making cleanup:

- faster, because common cleanup actions are prepackaged and guided
- more consistent, because the suggestions are rule-based and deterministic
- easier to review, because users can see detected issues before applying changes
- more transparent, because every pipeline step is logged
- more accessible, because non-programmers can clean data without writing pandas code

Instead of jumping between spreadsheets, scripts, and trial-and-error cleanup, users get one focused workflow for profiling, reviewing, transforming, and exporting data.
