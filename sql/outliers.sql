select * into tmp_circuit_reading_outliers from circuit_reading where watt_hours_delta > 1.25;
