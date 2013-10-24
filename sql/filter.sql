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
  b.wh_diff, 
  sum(b.wh_diff) OVER 
    (PARTITION BY site_id, ip_addr ORDER BY time_stamp) wh_used
INTO calc_tmp 
FROM 
  (SELECT
    site_id,
    ip_addr,
    time_stamp,
    CASE WHEN wh_diff >= 0 THEN wh_diff
         ELSE 0 -- handle the reset case (i.e. a negative)
    END wh_diff
   FROM 
     (SELECT 
       site_id, 
       ip_addr, 
       time_stamp, 
       coalesce(watt_hours_sc20 - lag(watt_hours_sc20) over w, 0) wh_diff 
      FROM circuit_reading 
      WINDOW w AS (PARTITION BY site_id, ip_addr ORDER BY time_stamp)) a
   ) b;
   
-- populate the columns of circuit_reading
UPDATE circuit_reading 
SET wh_diff=calc_tmp.wh_diff, wh_used=calc_tmp.wh_used
FROM calc_tmp 
WHERE circuit_reading.site_id=calc_tmp.site_id and 
      circuit_reading.ip_addr=calc_tmp.ip_addr and 
      circuit_reading.time_stamp=calc_tmp.time_stamp;
