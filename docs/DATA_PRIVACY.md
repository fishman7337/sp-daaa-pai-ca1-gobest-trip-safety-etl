# Data And Privacy

## Secrets

The repository should not contain real database passwords, API keys, access
tokens, or private connection strings. Keep secrets in `config/.env`, another
ignored env file, or the hosting platform's secret manager.

If a credential was present in the original submission package, treat it as
compromised and rotate it before reusing the database.

## Student Artifacts

Declaration files include student names and identifiers in their filenames.
They were moved into `artifacts/private/declarations/`, which is ignored by
git. Keep this directory local or publish only to a private repository.

## Dataset Files

Raw datasets are not included. The staging SQL file contains original local
dataset paths for traceability, but those paths must be changed before running
on a new machine.

## Notebook Outputs

Notebook cell outputs should remain stripped before publication. This keeps the
repository smaller and reduces the chance of exposing local paths, data samples,
or credentials.
