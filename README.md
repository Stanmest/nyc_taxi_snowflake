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

```
flowchart TD
  A["NYC TLC Parquet (Yellow trips)"] --> B["@YELLOW_STAGE (internal stage)"]
  B --> C["RAW.YELLOW_TRIPS (table)"]

  C --> D["ANALYTICS.V_DAILY_METRICS (view)"]
  C --> E["ANALYTICS.V_DAILY_BY_PAYMENT (view)"]
  C --> F["ANALYTICS.V_TOP_PU_ZONES (view)"]

  %% dbt lineage (optional, for docs)
  C -. "dbt source" .-> S["nyctaxi.YELLOW_TRIPS"]
  S --> T["stg_yellow_trips (dbt view)"]
  T --> M["fact_daily_metrics (dbt table)"]
  M --> X["Exposure: nyc_taxi_daily_metrics"]

```

## 📸 Screenshots

**Stage files**
![Stage list](assets/stage_list.png)

**Monthly counts**
![Monthly counts](assets/monthly_counts.png)

**dbt lineage — staging**
![dbt lineage stg](assets/dbt_lineage_stg.png)

**dbt lineage — fact**
![dbt lineage fact](assets/dbt_lineage_fact.png)

