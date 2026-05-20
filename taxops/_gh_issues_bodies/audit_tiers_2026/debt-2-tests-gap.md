## Parent epic
Technical debt epic

## Depends on
SEC CSRF/password/upload work ideally lands first — tests anchor real behaviors.

## Scope
Cover login/session flags, uploads (size/extension/path), dynamic field whitelist rejects, importer smoke (stretch), POST without CSRF after SEC-1.

## Definition of done
- New pytest modules exercising auth + uploads + whitelist + csrf regressions once implemented
- `python -m pytest tests/ -v` passes in CI/local
