-- remove duplicate timestamps for circuits
-- simply choose the one that appeared first in the logs
-- (hence the "order by line_num" statement)

-- clear out circuit_reading 1st
delete from circuit_reading;
ALTER TABLE circuit_reading DROP CONSTRAINT IF EXISTS circuit_reading_pkey;

-- HACK:  Make table scans prohibitively expensive in order to 
-- use index scan
SET enable_seqscan=false;

INSERT
INTO circuit_reading 
SELECT
  site_id,
  ip_addr,
  time_stamp,
  watts,
  watt_hours_sc20,
  credit
FROM
  (SELECT 
    *, 
    row_number() over (PARTITION BY site_id, ip_addr, time_stamp 
                       ORDER BY line_num) row_num 
    FROM raw_circuit_reading) raw 
WHERE raw.row_num=1;

-- HACK-END: Allow table scans again
SET enable_seqscan=true;


