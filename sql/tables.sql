-- table definitions for the Shared Solar SD Log database

CREATE TABLE circuit (
  pk           uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
  machine_id   varchar(18) NOT NULL,
  site_id      varchar(12) NOT NULL,
  ip_addr      varchar(46) NOT NULL, -- size is ipv6 ready
  main_circuit boolean DEFAULT false,
  UNIQUE(machine_id, site_id, ip_addr)
);

CREATE TABLE power_reading (
  pk               uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
  circuit          uuid REFERENCES circuit (pk),
  time_stamp       timestamp,
  watts            numeric,
  volts            numeric,
  amps             numeric,
  watt_hours_sc20  numeric,
  watt_hours_today numeric,
  max_watts        numeric,
  max_volts        numeric,
  max_amps         numeric,
  min_watts        numeric,
  min_volts        numeric,
  min_amps         numeric,
  power_factor     numeric,
  power_cycle      numeric,
  frequency        numeric,
  volt_amps        numeric,
  relay_not_closed boolean,
  send_rate        numeric,
  credit           numeric
);
