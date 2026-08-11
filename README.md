# Practical AI CA1 - Driving Safety ETL and Analytics

Recommended GitHub repository name: `practical-ai-ca1-driving-safety-etl`

Suggested GitHub description:

> Practical AI CA1 coursework repository for driving safety data cleaning, MySQL
> staging and warehouse schemas, PySpark ETL orchestration, Power BI analytics,
> and supporting documentation.

Suggested topics: `practical-ai`, `etl`, `data-warehouse`, `mysql`, `pyspark`,
`powerbi`, `jupyter-notebook`, `driving-safety`, `data-cleaning`,
`star-schema`.

## Evidence and interpretation

| Evidence-backed measure | Current repository evidence |
| --- | --- |
| Outlier methods | Sensor cleaning implements IQR rules and PCA-reconstruction diagnostics. |
| Warehouse outcome | Validated staging data is transformed into a documented dimensional/star schema for Power BI consumption. |

The qualitative outcome is a reproducible driver-safety ETL and analytics pipeline with validation, feature engineering, warehouse SQL, and offline tests. No row-count, throughput, Z-score, dashboard-adoption, or safety-impact claim is made without a pinned run artifact.

## Overview

This repository contains the cleaned-up submission package for the ST1508
Practical AI CA1 project. The project prepares driving sensor, driver, and
safety-label data for analytics by moving data through:

1. MySQL staging tables.
2. PySpark/Python cleaning and feature engineering scripts.
3. A MySQL star-schema data warehouse.
4. Power BI dashboard/reporting artifacts.

The original coursework deliverables are preserved under `docs/`,
`dashboard/`, `notebooks/`, and `sql/`. The executable ETL code is organized
under `src/pai_pipeline/`, with a safer package CLI for running and validating
the pipeline.

## Repository Structure

```text
.
|-- config/                  # Sanitized env templates
|-- dashboard/powerbi/       # Power BI dashboard artifact
|-- docs/
|   |-- diagrams/            # ERD source/images
|   |-- report/              # Written report
|   |-- scrum/               # Scrum documentation
|   `-- slides/              # Presentation deck
|-- notebooks/               # Cleaned notebook sources
|-- scripts/                 # Shell/PowerShell runner wrappers
|-- sql/                     # Staging and warehouse DDL
|-- src/pai_pipeline/        # Package CLI, helpers, and ETL scripts
|-- tests/                   # Offline unit tests and opt-in integration tests
|-- vendor/                  # MySQL JDBC jar used by Spark
`-- artifacts/private/       # Local-only private files, ignored by git
```

## Setup

Create a virtual environment and install development dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

For full ETL execution, install the runtime stack:

```bash
python -m pip install -e ".[etl]"
```

PySpark jobs also require Java and the MySQL Connector/J jar. A local connector
jar is stored under `vendor/`; set `PAI_MYSQL_JDBC_JAR` if you use a different
location.

## Configuration

Copy the sanitized template and fill in private values:

```bash
cp config/.env.example config/.env
```

The CLI automatically checks `.env`, `config/.env`, `config/.env.local`, and
`~/.pai_env`. You can also point to a specific file:

```bash
export PAI_ENV_FILE=config/.env
```

Never commit real credentials. If the original database password is still live,
rotate it before publishing this repository.

## Running The Pipeline

List available jobs:

```bash
python -m pai_pipeline list
```

Validate local configuration:

```bash
python -m pai_pipeline check-config --strict
```

Dry-run the full orchestration without touching the database:

```bash
python -m pai_pipeline run all --dry-run
```

Run all ETL scripts after configuring database credentials:

```bash
python -m pai_pipeline run all
```

Windows users can also run:

```powershell
.\scripts\run_all.ps1 -DryRun
```

Linux/macOS users can run:

```bash
./scripts/run_all.sh --dry-run
```

## Testing

Default tests are offline and safe for CI:

```bash
python -m pytest
python -m compileall src tests scripts
```

Live database tests are deliberately opt-in:

```bash
RUN_DB_TESTS=1 PAI_ENV_FILE=config/.env python -m pytest -m integration
```

GitHub Actions runs the offline test suite and Python compile checks on push and
pull request.

## SQL And Dashboard Artifacts

- Staging schema and load script: `sql/01_create_staging.sql`
- Data warehouse star schema: `sql/02_create_warehouse.sql`
- ERDs: `docs/diagrams/`
- Power BI dashboard: `dashboard/powerbi/PAI_CA1.pbix`

The SQL staging script still contains local `LOAD DATA LOCAL INFILE` paths from
the coursework environment. Update those paths before loading a fresh dataset on
another machine.

## Privacy Notes

Private declaration forms were moved to `artifacts/private/declarations/` and
are ignored by git. Keep them out of public GitHub repositories because the
filenames contain names and student identifiers.

Notebook outputs were stripped during cleanup to reduce repository size and
avoid publishing local paths, outputs, or accidental secrets.

## License

Code and documentation added for this cleaned repository are released under the
MIT License. Third-party assets such as the MySQL Connector/J jar remain under
their own upstream licenses.
