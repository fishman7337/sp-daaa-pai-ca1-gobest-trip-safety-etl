# Repository agent instructions

## Scope and precedence

These instructions apply to the entire repository. If a future directory contains a more specific `AGENTS.md`, follow that file for work inside its directory. Direct maintainer instructions take precedence.

## Project context

- **Project:** Practical AI CA1 - Driving Safety ETL and Analytics
- **Repository shape:** Python, Jupyter notebooks
- Read `README.md` before changing behaviour, architecture, data handling, or public claims.
- Treat `CONTRIBUTING.md` as the authoritative development workflow and command reference.
- Follow `SECURITY.md` for vulnerability reporting and `CODE_OF_CONDUCT.md` for collaboration.
- Use the workflows under `.github/workflows/` as the final cross-platform CI contract.

## Working agreement

1. Keep each change focused on a clear problem; avoid unrelated cleanup.
2. Inspect nearby tests, configuration, documentation, and generated artifacts before editing.
3. Add or update tests for behavioural changes, including failure and boundary cases.
4. Preserve public APIs and file formats unless the change explicitly includes a documented migration.
5. Never commit credentials, personal data, local environments, caches, or unlicensed datasets.
6. Do not weaken lint, coverage, security, or dependency gates merely to obtain a passing run.
7. After code or architecture changes, run `graphify update .` and review the graph diff; do not commit cache-only churn.
8. Do not stage, commit, push, or publish changes unless the maintainer explicitly requests that action.

## Required validation

Run commands from the repository root. Use a clean environment when dependency or packaging behaviour changes.

### Standard development gate

```text
python -m venv .venv
python -m pip install -e ".[dev]"
python -m ruff check .
python -m ruff format --check .
python -m pytest
python -m compileall src tests
```

### Repository-specific CI-equivalent checks

```text
python -m pai_pipeline list
python -m pai_pipeline check-config
```

Also run `git diff --check`. Record exactly which commands ran, their outcomes, and any documented prerequisite that prevented a check. Do not describe an unexecuted check as passing.

### Conditional or integration setup

- Install the ETL runtime with `python -m pip install -e ".[etl]"` only for PySpark/MySQL execution.
- Full ETL execution requires Java, MySQL Connector/J, authorized MySQL credentials, and corrected local input paths.

## Managed artifacts and data

- Notebook outputs are intentionally stripped; do not reintroduce local paths, stored errors, or accidental secrets.
- Keep `artifacts/private/` and all real environment files outside Git.
- Treat SQL, ERDs, the Power BI file, and the vendored JDBC jar as preserved artifacts; review their paths and third-party licences before changing them.

## Integration and claim boundaries

- Default tests and dry-runs are offline; live database tests require explicit intent plus `RUN_DB_TESTS=1` and `PAI_ENV_FILE=config/.env`.
- No disposable MySQL integration service exists, so do not report live integration as passing when credentials or a database are unavailable.
- Never publish credentials or private declaration forms.
- **Verified local capability:** Python 3.11/3.12, CLI dry-run, 4 schema-valid notebooks, security checks.
- **Known boundary:** Live MySQL integration requires credentials and no disposable service exists.

## Code, documentation, and evidence standards

- For Python, follow PEP 8, PEP 257, and Google-style docstrings in the production scopes configured by Ruff.
- Prefer small, typed, testable units and explicit error handling. Avoid silent fallbacks that hide invalid data, missing models, credentials, or services.
- Update README, architecture, data/model documentation, examples, and changelog material when interfaces or behaviour change.
- Every quantitative claim must retain its denominator, dataset/version, split, date range, run/configuration, and calculation method.
- Distinguish measured results from targets, examples, heuristics, previews, and prior runs. Do not infer deployment, accuracy, security, causality, or impact from tests or scaffolding alone.
- Update generated files through their source script. If no generator exists, document the manual process and verify semantic equivalence.

## Review and handoff

Before handing off a change, provide a concise summary, list changed files, report validation evidence, and call out residual risks or unavailable integrations. Keep commits imperative and scoped; do not add tool-attribution or automated co-author trailers.
