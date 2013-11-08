-- Populate the wh_diff and wh_used columns of
-- circuit_reading via taking wh difference between
-- current and last record, accounting for negatives
-- (setting them to 0) and summing

-- HACK:  Make table scans prohibitively expensive in order to 
-- use index scan 
SET enable_seqscan=false;

UPDATE circuit_reading 
SET watt_hours_delta=circuit_reading_wh_delta.watt_hours_delta
FROM 
 (SELECT 
   site_id, 
   ip_addr, 
   time_stamp, 
   GREATEST(COALESCE(watt_hours_sc20 - lag(watt_hours_sc20) over w, 0),0) watt_hours_delta
  FROM circuit_reading 
  WINDOW w AS (PARTITION BY site_id, ip_addr ORDER BY time_stamp)) circuit_reading_wh_delta
WHERE circuit_reading.site_id=circuit_reading_wh_delta.site_id and 
      circuit_reading.ip_addr=circuit_reading_wh_delta.ip_addr and 
      circuit_reading.time_stamp=circuit_reading_wh_delta.time_stamp;

-- HACK-END: Allow table scans again
SET enable_seqscan=true;
