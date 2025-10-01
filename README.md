# NYC Taxi Data Warehouse (Snowflake + dbt)

Production-style mini-warehouse on the **NYC Yellow Taxi** dataset.  
Raw Parquet ➜ Snowflake **RAW** ➜ **ANALYTICS** views ➜ **dbt** models, tests & lineage.

- **Warehouse:** Snowflake (X-Small warehouse)
- **Modeling:** dbt (Snowflake adapter)
- **Orchestration/Load:** simple stage → SQL inserts (safe timestamp parsing) + optional proc/task
- **Docs:** dbt docs (catalog + lineage)

---

## ✨ Highlights

- **Clean ingestion:** Parquet files uploaded to an internal stage, loaded with idempotent SQL.
- **Safe parsing:** `TRY_TO_TIMESTAMP_NTZ()` prevents bad rows from breaking loads.
- **Curated layer:** `ANALYTICS` views for BI (daily KPIs, by payment type, top pickup zones).
- **Quality:** dbt tests (`not_null`, `accepted_values`) + searchable docs & lineage.
- **Cost control:** X-Small warehouse, `AUTO_SUSPEND=60`, `AUTO_RESUME=TRUE`.

---

## 🗺️ Architecture

```mermaid
flowchart TD
  A[NYC TLC Parquet (Yellow trips)] --> B[@YELLOW_STAGE (internal stage)]
  B --> C[RAW.YELLOW_TRIPS (table)]
  C --> D[ANALYTICS.V_DAILY_METRICS]
  C --> E[ANALYTICS.V_DAILY_BY_PAYMENT]
  C --> F[ANALYTICS.V_TOP_PU_ZONES]
  C -. source .-> S[(dbt source)]
  S --> T[dbt stg_yellow_trips (view)]
  T --> M[dbt fact_daily_metrics (table)]
  M --> X[((dbt Exposure: nyc_taxi_daily_metrics))]
