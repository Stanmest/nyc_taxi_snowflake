from snowflake.snowpark import Session
import os
cfg = {
  "account": os.environ.get("SF_ACCOUNT","lftmjfv-wx64756"),
  "user": os.environ.get("SF_USER","MEST77"),
  "role": os.environ.get("SF_ROLE","ACCOUNTADMIN"),
  "warehouse": os.environ.get("SF_WAREHOUSE","WH_NYC_TAXI"),
  "database": os.environ.get("SF_DATABASE","NYCTAXI"),
  "schema": os.environ.get("SF_SCHEMA_RAW","RAW"),
  "authenticator": os.environ.get("SF_AUTH","snowflake"),
  "host": "lftmjfv-wx64756.snowflakecomputing.com",
}
if cfg["authenticator"] == "snowflake":
    cfg["password"] = os.environ["SF_PASSWORD"]
s = Session.builder.configs(cfg).create()
print(s.sql("select current_account(), current_user(), current_role()").collect())
