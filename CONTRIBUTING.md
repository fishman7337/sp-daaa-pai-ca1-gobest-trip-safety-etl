# Contributing

## Development Setup

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

For full ETL execution, install runtime dependencies:

```bash
python -m pip install -e ".[etl]"
```

## Before Committing

Run the offline checks:

```bash
python -m pytest
python -m compileall src tests scripts
```

Live database tests are opt-in:

```bash
RUN_DB_TESTS=1 PAI_ENV_FILE=config/.env python -m pytest -m integration
```

## Data And Secrets

Do not commit `.env`, `.pai_env*`, source datasets, generated extracts, or
private declaration forms. Keep credentials in `config/.env` or another private
path referenced by `PAI_ENV_FILE`.
