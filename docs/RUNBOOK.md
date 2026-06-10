# Runbook

## 1. Prepare Python

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[etl]"
```

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[etl]"
```

## 2. Configure Environment

Copy `config/.env.example` to `config/.env` and fill in the staging and
warehouse connection details. Keep the real file private.

```bash
python -m pai_pipeline check-config --strict
```

## 3. Create Databases

Run the SQL files in order against MySQL:

1. `sql/01_create_staging.sql`
2. `sql/02_create_warehouse.sql`

Update `LOAD DATA LOCAL INFILE` paths in the staging script before loading data
on a new machine.

## 4. Run ETL

Use a dry run first:

```bash
python -m pai_pipeline run all --dry-run
```

Then run the full pipeline:

```bash
python -m pai_pipeline run all
```

The runner queries each staging source table for `cleaned_date IS NULL` rows and
skips a job when there is no pending data.

## 5. Validate

Run offline tests:

```bash
python -m pytest
```

Run live DB smoke tests only when credentials point to a safe test database:

```bash
RUN_DB_TESTS=1 PAI_ENV_FILE=config/.env python -m pytest -m integration
```
