-- This should speed up filtering step
ALTER TABLE circuit_reading ADD CONSTRAINT circuit_reading_pkey PRIMARY KEY (site_id, ip_addr, time_stamp);
CLUSTER circuit_reading USING circuit_reading_pkey;
