# RULES.md

## Repository Rules
- Use English for code and technical documentation.
- Keep naming explicit and consistent.
- Avoid hidden magic and implicit coupling.
- Prefer configuration over hardcoded values.
- Keep functions/modules small and testable.
- Do not mix research prototypes into production paths without review.
- Create and switch to a dedicated branch before doing task work; do not work directly on `main`.
- When task work is finished, restore the main workspace checkout to `main`.
- After the PR is merged or the work is cancelled, delete the created branch locally and remotely unless there is a specific reason not to.

## Backend-specific
- **Working directory:** Treat `apps/api` as the Python package root for the API service (`from app....`).
- **Secrets:** Never commit `KIS_APP_KEY`, `KIS_APP_SECRET`, BOK/Korea Exim keys, or `UPSTREAM_BEARER_TOKEN`. Document new env vars in `apps/api/.env.example`.
- **Auth:** When `UPSTREAM_BEARER_TOKEN` is set, all non-OPTIONS requests must present a valid `Authorization: Bearer` header (see `app/main.py`).
- **Live trading:** No production orders, balance-changing calls, or unapproved execution paths.

## AI-assisted changes (Codex)
- After editing `apps/api`, run `python -c "from app.main import app"` from `apps/api` before considering the change done.
- Update **`docs/API_BUILD_ORDER.md`** when routes are added or removed.
- Repository agent config lives under **`.codex/`**; see [AGENTS.md](./AGENTS.md).
