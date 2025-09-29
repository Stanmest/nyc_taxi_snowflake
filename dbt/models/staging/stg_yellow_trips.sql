select
  vendor_id,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  date_trunc('day', tpep_pickup_datetime) as pickup_day,
  passenger_count::int as passenger_count,
  trip_distance::float as trip_distance,
  pu_location_id, do_location_id,
  fare_amount::float as fare_amount,
  tip_amount::float as tip_amount,
  total_amount::float as total_amount
from {{ source('nyctaxi','YELLOW_TRIPS') }}
