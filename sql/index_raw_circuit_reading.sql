-- If this is used in de-duplication, it should speed things up significantly
CREATE INDEX raw_circuit_reading_ix ON raw_circuit_reading (site_id, ip_addr, machine_id, time_stamp, line_num);
