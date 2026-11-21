# Ecomm

Declarative Automation Bundle `ecommerce_sdp_cicd`: a Lakeflow Spark Declarative Pipeline (bronze, silver, gold materialized views) over a synthetic ecommerce dataset, with unit and integration tests run by a Lakeflow Job.

## Layout

```
databricks.yml                         bundle + targets (dev / test / prod)
resources/
  ecommerce_storage.schema.yml         UC schema + raw_data volume
  ecommerce_medallion.pipeline.yml     SDP pipeline (serverless)
  ecommerce_cicd.job.yml               unit tests -> data gen -> pipeline -> integration tests
src/pipeline/
  ecommerce_lib/                       shared, unit-tested logic (schemas, synthetic data, transforms, rules)
  transformations/bronze|silver|gold/  one materialized view per file
src/notebooks/generate_synthetic_data  writes raw CSV / JSON / Parquet into the volume
tests/unit/                            pytest: schema contract + transformation tests
tests/run_unit_tests                   notebook that runs pytest (job task)
tests/integration/integration_tests    checks schemas, row counts, DQ rules and gold reconciliation
```

## Targets

| Target | Mode | Schema | Notes |
| --- | --- | --- | --- |
| dev (default) | development | dev_<user>_ecommerce_dev | isolated per developer, schedule paused |
| test | default | ecommerce_test | shared CI target, schedule paused |
| prod | production | ecommerce_prod | main branch only, schedule unpaused |

## Commands

```
databricks bundle validate --strict -t dev
databricks bundle deploy -t dev
databricks bundle run ecommerce_cicd -t dev
```
