-- table definitions for the Shared Solar SD Log database

CREATE TABLE circuit (
  pk           uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
  machine_id   bigint NOT NULL,
  site_id      varchar(8) NOT NULL,
  ip_addr      varchar(16) NOT NULL, -- size is ipv6 ready
  main_circuit boolean DEFAULT false,
  UNIQUE(machine_id, site_id, ip_addr)
);

CREATE TABLE power_reading (
  pk               uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
  circuit          uuid REFERENCES circuit (pk),
  time_stamp       timestamp,
  watts            real,
  watt_hours_sc20  double precision,
  credit           real 
);

-- table definitions for the *raw* Shared Solar SD Logs
CREATE TABLE raw_circuit_reading (
  drop_id          varchar(8) NOT NULL,  --YYYYMMDD format
  line_num         smallint NOT NULL,
  site_id          varchar(8) NOT NULL,
  machine_id       bigint NOT NULL,
  ip_addr          varchar(16) NOT NULL,
  circuit_type     varchar(10) NOT NULL,
  time_stamp       varchar(14) NOT NULL, --YYYYMMDDHHmmss format
  watts            real,
  watt_hours_sc20  double precision,
  credit           real
);
