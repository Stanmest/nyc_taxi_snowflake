with t as (select * from {{ ref('stg_yellow_trips') }})
select
  pickup_day as day,
  count(*) as trips,
  sum(total_amount) as revenue,
  avg(datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime)) as avg_trip_minutes,
  avg(trip_distance) as avg_miles,
  avg(tip_amount) as avg_tip
from t
group by 1
order by 1
