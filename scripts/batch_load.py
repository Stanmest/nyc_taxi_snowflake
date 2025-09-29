import os, pathlib
from snowflake.snowpark import Session

# months to load
MONTHS = ["2024-01","2024-02","2024-03"]

# Build Snowflake config from your .env
SF_ACCOUNT = os.environ["SF_ACCOUNT"].lower()
cfg = {
  "account": SF_ACCOUNT,
  "user": os.environ["SF_USER"],
  "role": os.environ["SF_ROLE"],
  "warehouse": os.environ["SF_WAREHOUSE"],
  "database": "NYCTAXI",
  "schema": "RAW",
  "authenticator": os.environ.get("SF_AUTH","snowflake"),
  "host": f"{SF_ACCOUNT}.snowflakecomputing.com",
}
if cfg["authenticator"] == "snowflake":
    cfg["password"] = os.environ["SF_PASSWORD"]

# Connect + print context
s = Session.builder.configs(cfg).create()
print("CTX:", s.sql("select current_account(), current_role(), current_database(), current_schema()").collect())

# Ensure objects exist
s.sql("create database if not exists NYCTAXI").collect()
s.sql("create schema  if not exists NYCTAXI.RAW").collect()
s.sql("create or replace file format NYCTAXI.RAW.FF_PARQUET type=parquet").collect()
s.sql("create or replace stage NYCTAXI.RAW.YELLOW_STAGE file_format=NYCTAXI.RAW.FF_PARQUET").collect()
s.sql("""
create table if not exists NYCTAXI.RAW.YELLOW_TRIPS (
  VENDOR_ID SMALLINT,
  TPEP_PICKUP_DATETIME TIMESTAMP_NTZ,
  TPEP_DROPOFF_DATETIME TIMESTAMP_NTZ,
  PASSENGER_COUNT NUMBER,
  TRIP_DISTANCE NUMBER,
  RATECODE_ID SMALLINT,
  STORE_AND_FWD_FLAG STRING,
  PU_LOCATION_ID INT,
  DO_LOCATION_ID INT,
  PAYMENT_TYPE SMALLINT,
  FARE_AMOUNT NUMBER,
  EXTRA NUMBER,
  MTA_TAX NUMBER,
  TIP_AMOUNT NUMBER,
  TOLLS_AMOUNT NUMBER,
  IMPROVEMENT_SURCHARGE NUMBER,
  TOTAL_AMOUNT NUMBER,
  CONGESTION_SURCHARGE NUMBER,
  AIRPORT_FEE NUMBER
)
""").collect()

# PUT + COPY each month
for m in MONTHS:
    p = pathlib.Path(f"data/raw/yellow_tripdata_{m}.parquet")
    print(f"\n== {m} ==")
    if not p.exists():
        print(f"Local file missing: {p}"); continue

    put_res = s.file.put(p.as_posix(), f"@NYCTAXI.RAW.YELLOW_STAGE/{m}/",
                         auto_compress=True, overwrite=True)
    print("PUT uploaded:", len(put_res))

    copy_sql = f"""
    copy into NYCTAXI.RAW.YELLOW_TRIPS
    from (
      select
        $1:VendorID::SMALLINT,
        $1:tpep_pickup_datetime::TIMESTAMP_NTZ,
        $1:tpep_dropoff_datetime::TIMESTAMP_NTZ,
        $1:passenger_count::NUMBER,
        $1:trip_distance::NUMBER,
        $1:RatecodeID::SMALLINT,
        $1:store_and_fwd_flag::STRING,
        $1:PULocationID::INT,
        $1:DOLocationID::INT,
        $1:payment_type::SMALLINT,
        $1:fare_amount::NUMBER,
        $1:extra::NUMBER,
        $1:mta_tax::NUMBER,
        $1:tip_amount::NUMBER,
        $1:tolls_amount::NUMBER,
        $1:improvement_surcharge::NUMBER,
        $1:total_amount::NUMBER,
        $1:congestion_surcharge::NUMBER,
        $1:airport_fee::NUMBER
      from @NYCTAXI.RAW.YELLOW_STAGE/{m} (file_format => NYCTAXI.RAW.FF_PARQUET)
      where metadata$filename ilike '%yellow_tripdata_{m}.parquet%'
    )
    ON_ERROR = 'CONTINUE'
    """
    res = s.sql(copy_sql).collect()
    print("COPY result rows:", len(res))
    if res:
        keys = ['FILE','ROWS_LOADED','ERROR_COUNT','FIRST_ERROR_MESSAGE']
        print([{k: r.get(k) for k in keys if k in r} for r in res][:3])

# Final counts by month
rows = s.sql("""
  select to_varchar(date_trunc('month', TPEP_PICKUP_DATETIME),'YYYY-MM') m, count(*) c
  from NYCTAXI.RAW.YELLOW_TRIPS
  group by 1 order by 1
""").collect()
print("\nCounts by month:", [(r['M'], r['C']) for r in rows])
