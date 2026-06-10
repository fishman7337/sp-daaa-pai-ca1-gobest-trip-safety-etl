# Security Policy

## Supported Scope

This is a coursework repository. Security fixes should focus on preventing
credential leakage, unsafe publication of private student artifacts, and
accidental writes to production-like databases.

## Reporting

If you find a secret, vulnerable configuration, or privacy issue, report it
privately to the repository owner. Do not open a public issue containing
credentials, tokens, student IDs, or database endpoints.

## Credential Handling

No live credentials should be committed. If a credential was ever committed or
shared, treat it as compromised and rotate it before using the project again.
