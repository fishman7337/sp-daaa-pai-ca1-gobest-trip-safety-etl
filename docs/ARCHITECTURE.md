# Architecture

## Pipeline Flow

```text
Raw CSV files
  -> MySQL staging tables
  -> PySpark/Python cleaning scripts
  -> MySQL warehouse star schema
  -> Power BI dashboard and report artifacts
```

## Main Components

- `sql/01_create_staging.sql` creates raw mirror tables and typed views.
- `sql/02_create_warehouse.sql` creates the warehouse star schema.
- `src/pai_pipeline/pipelines/sensor_cleaning.py` cleans sensor records and
  builds trip-level features for `Dim_Sensor`.
- `src/pai_pipeline/pipelines/driver_cleaning.py` cleans driver records for
  `Dim_Driver`.
- `src/pai_pipeline/pipelines/safety_cleaning.py` loads safety labels into
  `Fact_Label`.
- `src/pai_pipeline/cli.py` provides a single CLI for listing, validating, and
  running pipeline scripts.

## Warehouse Grain

- `Dim_Driver`: one row per driver/car combination.
- `Dim_Sensor`: one row per trip or booking profile.
- `Fact_Label`: one row per booking safety label, linked to driver and sensor
  dimensions.

## Operational Notes

The large ETL scripts are notebook exports kept close to their submitted form.
Smaller package modules provide safer orchestration, configuration validation,
and tests around them. This avoids a risky business-logic rewrite while making
the repository maintainable.
