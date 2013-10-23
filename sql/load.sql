-- insert distinct circuit data from raw_circuit_reading temp table
-- "except" any that might exist already
-- (annoying that postgresql uses "except" rather than "minus")
insert into circuit (machine_id, site_id, ip_addr, main_circuit) select distinct machine_id, site_id, ip_addr, cast(case circuit_type when 'MAINS' then 1 else 0 end as boolean) as main_circuit from raw_circuit_reading except select machine_id, site_id, ip_addr, main_circuit from circuit;

-- insert distinct power_reading data from raw_circuit_reading temp table (lookup circuit by machine_id, site_id, ip_addr)
insert into power_reading (circuit, time_stamp, watts, watt_hours_sc20, credit) select c.pk, to_timestamp(rcr.time_stamp, 'YYYYMMDDHH24MISS'), rcr.watts, rcr.watt_hours_sc20, rcr.credit from raw_circuit_reading rcr, circuit c where rcr.machine_id=c.machine_id and rcr.site_id=c.site_id and rcr.ip_addr=c.ip_addr;
