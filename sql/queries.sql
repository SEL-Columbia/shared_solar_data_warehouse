-- Queries for data analysis go here
select count(*) as ct, site_id, machine_id, ip_addr, time_stamp from raw_circuit_reading group by site_id, machine_id, ip_addr, time_stamp having count(*) > 2;
