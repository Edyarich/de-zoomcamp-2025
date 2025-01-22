-- Question 3. Trip Segmentation Count
-- During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

-- Up to 1 mile
-- In between 1 (exclusive) and 3 miles (inclusive),
-- In between 3 (exclusive) and 7 miles (inclusive),
-- In between 7 (exclusive) and 10 miles (inclusive),
-- Over 10 miles

SELECT 
    SUM(CASE WHEN trip_distance <= 1 THEN 1 ELSE 0 END) AS "Up to 1 mile",
    SUM(CASE WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 ELSE 0 END) AS "1 to 3 miles",
    SUM(CASE WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 ELSE 0 END) AS "3 to 7 miles",
    SUM(CASE WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 ELSE 0 END) AS "7 to 10 miles",
    SUM(CASE WHEN trip_distance > 10 THEN 1 ELSE 0 END) AS "Over 10 miles"
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-10-01' 
  AND lpep_pickup_datetime < '2019-11-01';


-- Question 4. Longest trip for each day
-- Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

WITH DailyLongestTrips AS (
    SELECT 
        DATE(lpep_pickup_datetime) AS pickup_date,
        MAX(trip_distance) AS max_distance
    FROM green_taxi_trips
    GROUP BY DATE(lpep_pickup_datetime)
)
SELECT 
    pickup_date
FROM DailyLongestTrips
ORDER BY max_distance DESC
LIMIT 1;


-- Question 5. Three biggest pickup zones
-- Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?
-- Consider only lpep_pickup_datetime when filtering by date.

SELECT 
    z."Zone" AS pickup_zone,
    SUM(g.total_amount) AS total_amount
FROM green_taxi_trips g
	JOIN taxi_zone_lookup z
		ON z."LocationID" = g."PULocationID"
WHERE DATE(lpep_pickup_datetime) = '2019-10-18'
GROUP BY z."Zone"
HAVING SUM(g.total_amount) > 13000
ORDER BY total_amount DESC
LIMIT 3;


-- Question 6. Largest tip
-- For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?
SELECT 
    dz."Zone" AS dropoff_zone,
    MAX(g.tip_amount) AS largest_tip
FROM green_taxi_trips g
JOIN taxi_zone_lookup dz ON g."DOLocationID" = dz."LocationID"
JOIN taxi_zone_lookup pz ON g."PULocationID" = pz."LocationID"
WHERE pz."Zone" = 'East Harlem North'
  AND DATE(lpep_pickup_datetime) >= '2019-10-01' 
  AND DATE(lpep_pickup_datetime) < '2019-11-01'
GROUP BY dz."Zone"
ORDER BY largest_tip DESC
LIMIT 1;
