-- table definitions for the Shared Solar SD Log database

-- CREATE TABLE circuit (
--   pk bigserial primary key,
--   --pk           SERIAL PRIMARY KEY,
--   machine_id   bigint NOT NULL,
--   site_id      varchar(8) NOT NULL,
--   ip_addr      varchar(16) NOT NULL, -- size is ipv6 ready
--   main_circuit boolean DEFAULT false,
--   UNIQUE(machine_id, site_id, ip_addr)
-- );
-- 
-- CREATE TABLE power_reading (
--   pk bigserial primary key,
--   --pk               SERIAL PRIMARY KEY,
--   circuit          bigint REFERENCES circuit (pk),
--   time_stamp       timestamp without time zone,
--   watts            real,
--   watt_hours_sc20  double precision,
--   credit           real 
-- );

-- table for the *raw* Shared Solar SD Logs
CREATE TABLE IF NOT EXISTS raw_circuit_reading (
  drop_id          varchar(8) NOT NULL,  
  line_num         smallint NOT NULL,
  site_id          varchar(8) NOT NULL,
  machine_id       bigint NOT NULL,
  ip_addr          varchar(16) NOT NULL,
  circuit_type     varchar(10),
  time_stamp       timestamp without time zone NOT NULL,
  watts            real,
  watt_hours_sc20  double precision,
  credit           real
);

-- table for the *clean* Shared Solar circuit data
CREATE TABLE IF NOT EXISTS circuit_reading (
  site_id          varchar(8) NOT NULL,
  ip_addr          varchar(16) NOT NULL,
  time_stamp       timestamp without time zone NOT NULL,
  watts            real,
  watt_hours_sc20  double precision,
  credit           real,
  watt_hours_delta real
);
