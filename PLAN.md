# Add REST Catalog Integration — Docs & Examples

## Context

The library is already catalog-agnostic at the code level — `IcebergLoader` and all public APIs accept any PyIceberg `Catalog` object, and PyIceberg's `RestCatalog` is available. However, the docs still say *"Currently tested against Hive Metastore; REST Catalog support is planned"*, and all examples use Hive-only `HiveCatalog`. This plan updates docs and adds examples to make REST Catalog a first-class documented option.

## Approach

Update existing documentation to remove the "planned" caveat and add REST Catalog usage examples. Add a new example script showing REST Catalog setup with both the `pyiceberg.yaml` + `load_catalog()` approach and the direct `RestCatalog()` constructor approach.

## Files to modify

| File | What changes |
|------|--------------|
| `README.md` | Replace "REST Catalog support is planned" with a note that it's supported; add a REST Catalog snippet in quickstart/usage |
| `docs/index.md` | Same textual update; add a REST Catalog config section |
| `examples/rest_catalog_example.py` | **New file** — self-contained example using `RestCatalog` with MinIO/S3 and common REST catalog servers (Tabular, Polaris, self-hosted) |
| `examples/README.md` | Add the new example to the table; update prereqs to mention both Hive and REST options |
| `src/iceberg_loader/catalog.py` | **New file** — `get_rest_catalog()` factory (env/arg resolution, S3 properties, path-style access) |
| `tests/test_catalog.py` | **New file** — tests for `get_rest_catalog()` |
| `docs/CODEMAPS/` | Update `FILES.md` / `MODULES.md` to document `catalog.py` and the new example |

## Reuse

- Existing `examples/catalog.py` — reference it as the Hive setup, contrast with REST setup
- Existing `examples/settings.py` — reuse its pattern of env-var-based configuration for the REST example
- `LoaderConfig`, `load_data_to_iceberg` — same API, no changes needed

## Steps

- [x] 1. Update `README.md`: remove "REST Catalog support is planned" caveat, add REST Catalog quickstart snippet alongside the existing Hive one
- [x] 2. Update `docs/index.md`: same caveat removal, add a "REST Catalog Setup" subsection under Usage with both `pyiceberg.yaml` and direct `RestCatalog()` approaches
- [x] 3. Create `examples/rest_catalog_example.py`: self-contained example that demonstrates REST Catalog setup with env vars, creates a table, appends data, and reads back — working with the local `docker-compose` MinIO stack (the REST catalog endpoint would be configurable via env vars for real servers like Tabular/Polaris)
- [x] 4. Update `examples/README.md`: add `rest_catalog_example.py` to the example summary table and running instructions

## Verification

- Run `uv run ruff check . && uv run ruff format --check .` for lint/format
- Run `uv run python examples/rest_catalog_example.py` against a running REST Catalog (or validate it parses/imports correctly without one)
- Review rendered docs with `uv run mkdocs serve` (if mkdocs is set up)
- Read through updated README and docs for consistency
