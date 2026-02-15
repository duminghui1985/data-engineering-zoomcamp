-- Count of records in fct_monthly_zone_revenue
SELECT COUNT(*) FROM taxi_rides_ny.prod.fct_monthly_zone_revenue;

-- Check total revenue for Green taxis in 2020 by pickup zone
SELECT 
    pickup_zone, 
    SUM(revenue_monthly_total_amount) AS total_revenue_2020
FROM taxi_rides_ny.prod.fct_monthly_zone_revenue
WHERE 
    service_type = 'Green' 
    AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY 1
ORDER BY total_revenue_2020 DESC
LIMIT 1;

-- Check total trips for Green taxis in October 2019
SELECT 
    SUM(total_monthly_trips) AS total_trips_oct_2019
FROM taxi_rides_ny.prod.fct_monthly_zone_revenue
WHERE 
    service_type = 'Green' 
    AND EXTRACT(YEAR FROM revenue_month) = 2019
    AND EXTRACT(MONTH FROM revenue_month) = 10;

-- Check count of records in stg_fhv_tripdata
SELECT count(*) FROM taxi_rides_ny.prod.stg_fhv_tripdata