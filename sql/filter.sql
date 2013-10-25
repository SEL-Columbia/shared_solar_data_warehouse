-- Populate the wh_diff and wh_used columns of
-- circuit_reading via taking wh difference between
-- current and last record, accounting for negatives
-- (setting them to 0) and summing

-- clear out any prior tmp table
DROP TABLE IF EXISTS calc_tmp;

-- populate the tmp table with calculations
SELECT 
  site_id, 
  ip_addr, 
  time_stamp, 
  b.watt_hours_delta, 
  sum(b.watt_hours_delta) OVER 
    (PARTITION BY site_id, ip_addr ORDER BY time_stamp) watt_hours_used
INTO calc_tmp 
FROM 
  (SELECT
    site_id,
    ip_addr,
    time_stamp,
    CASE WHEN watt_hours_delta >= 0 THEN watt_hours_delta
         ELSE 0 -- handle the reset case (i.e. a negative)
    END watt_hours_delta
   FROM 
     (SELECT 
       site_id, 
       ip_addr, 
       time_stamp, 
       coalesce(watt_hours_sc20 - lag(watt_hours_sc20) over w, 0) watt_hours_delta
      FROM circuit_reading 
      WINDOW w AS (PARTITION BY site_id, ip_addr ORDER BY time_stamp)) a
   ) b;
   
-- populate the columns of circuit_reading
UPDATE circuit_reading 
SET watt_hours_delta=calc_tmp.watt_hours_delta, watt_hours_used=calc_tmp.watt_hours_used
FROM calc_tmp 
WHERE circuit_reading.site_id=calc_tmp.site_id and 
      circuit_reading.ip_addr=calc_tmp.ip_addr and 
      circuit_reading.time_stamp=calc_tmp.time_stamp;
