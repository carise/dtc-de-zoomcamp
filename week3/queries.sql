-- week 3

-- question 1
SELECT count(*) FROM `cfz-data-eng-zoomcamp.trips_data_all.fhv_tripdata` WHERE extract (year from pickup_datetime) = 2019;


-- question 2
SELECT count(distinct dispatching_base_num) FROM `cfz-data-eng-zoomcamp.trips_data_all.fhv_tripdata` WHERE extract (year from pickup_datetime) = 2019;

-- question 3
CREATE OR REPLACE TABLE `cfz-data-eng-zoomcamp.trips_data_all.fhv_tripdata_partitioned`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `cfz-data-eng-zoomcamp.trips_data_all.fhv_tripdata`
);

-- question 4
select count(*) from `cfz-data-eng-zoomcamp.trips_data_all.fhv_tripdata_partitioned` where dispatching_base_num in ('B00987', 'B02060', 'B02279')
and pickup_datetime between parse_timestamp('%Y/%m/%d', '2019/01/01') and parse_timestamp('%Y/%m/%d', '2019/03/31');

-- question 5
-- SR_Flag and dispatching_base_num seem to be columns we'd filter on.
-- dispatching_base_num has a high number of distinct values, so likely makes sense to cluster and not partition.
-- dispatching_base_num is a string value column
-- SR_Flag is an integer column with a relatively small number of distinct values. 
-- the table is about 1.6GiB in size, so number of partitions are less than 1GiB.
-- given this, probably can cluster on both columns


