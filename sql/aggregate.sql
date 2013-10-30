-- Aggregate circuit_reading values into minutely, hourly and daily tables
-- summing watt_hours_delta and credit_delta over those resolutions

-- Clear out existing tables
DROP TABLE IF EXISTS circuit_reading_minutely;
DROP TABLE IF EXISTS circuit_reading_hourly;
DROP TABLE IF EXISTS circuit_reading_daily;

select site_id, ip_addr, sum(watt_hours_delta) watt_hours_delta, max(credit) max_credit, min(credit) min_credit, date_trunc('minute', time_stamp) time_stamp into circuit_reading_minutely from circuit_reading group by site_id, ip_addr, date_trunc('minute', time_stamp);

select site_id, ip_addr, sum(watt_hours_delta) watt_hours_delta, max(max_credit) max_credit, min(min_credit) min_credit, date_trunc('hour', time_stamp) time_stamp into circuit_reading_hourly from circuit_reading_minutely group by site_id, ip_addr, date_trunc('hour', time_stamp);

select site_id, ip_addr, sum(watt_hours_delta) watt_hours_delta, max(max_credit) max_credit, min(min_credit) min_credit, date_trunc('day', time_stamp) time_stamp into circuit_reading_daily from circuit_reading_hourly group by site_id, ip_addr, date_trunc('day', time_stamp);
