shared_solar_data_warehouse
===========================

Description
-----------

This repository contains tools for assembling, loading, cleaning and aggregating <a href="http://sharedsolar.org/" target="_blank">Shared Solar</a> "raw" usage data into a database designed for simple retrieval at minutely, hourly and daily temporal resolutions.


Installation
------------

The current version is based on <a href="http://www.postgresql.org/" target="_blank">postgresql</a> and <a href="http://initd.org/psycopg/" target="_blank">psycopg</a> with <a href="http://python.org/" target="_blank">python</a>.

For ubuntu/debian, use the following standard packages as root or sudo:

```
    sudo apt-get install python python-all-dev python-pip
    sudo apt-get install postgresql postgresql-contrib
    sudo pip install psycopg2
```

Next, as user <tt>postgres</tt>, create a database, and install the <a href="http://www.postgresql.org/docs/current/static/uuid-ossp.html" target="_blank">uuid-ossp</a> extension:

```
$ sudo su - postgres
postgres:~$ createdb [database_name_goes_here]
postgres:~$ psql -d [database_name_goes_here]
psql (9.1.9)
Type "help" for help.

sd_log=# CREATE EXTENSION "uuid-ossp";
CREATE EXTENSION
```

Create the appropriate <a href="http://www.postgresql.org/docs/9.3/static/database-roles.html" target="_blank">database roles</a> or <a href="http://www.postgresql.org/docs/9.3/static/client-authentication.html" target="_blank">authorized clients</a> and update the <tt>processor/settings.py</tt> in this repo with the correct <tt>dbname</tt>, <tt>dbuser</tt> and optional <tt>pwd</tt>.

Finally, install the tables via <tt>psql</tt>:

```
$ psql -d [database_name_goes_here] -U [authorized_user_goes_here] < sql/tables.sql
```

If successful, you should see:

```
NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "circuit_pkey" for table "circuit"
NOTICE:  CREATE TABLE / UNIQUE will create implicit index "circuit_machine_id_site_id_ip_addr_key" for table "circuit"
CREATE TABLE
NOTICE:  CREATE TABLE / PRIMARY KEY will create implicit index "power_reading_pkey" for table "power_reading"
CREATE TABLE
```

Usage
-----

SharedSolar "raw" usage data consists of power meter data (i.e. watts, watt_hours...) recorded at 3 second intervals in iso-8859-1 encoded files (1 file per circuit hour).  

The basic workflow for loading data is:

> SharedSolar Raw Data Drop (individual circuit hour log files)  
> |  
> v  
> Assemble into CSV (via python denormalize_to_csv.py script)  
> |  
> v  
> Load into Postgresql database (via bulk load "copy")  
> |  
> v  
> De-duplicate and clean data (via sql scripts)  
> |  
> v  
> Aggregate data into minutely, hourly and daily resolution tables (via sql)  

This workflow is encapsulated by the processor/load_script.sh which assumes the following folder structure in addition to what is in this repository:

> shared_solar_data_warehouse (THIS repo...i.e. the PROJECT_DIR)  
> \  
>  load (this is where the SharedSolar raw data drops to be loaded go)  
> \  
>  processed (this is where the files that have been processed go)  

The SharedSolar raw data drop directory needs to conform to the following directory structure/naming convention (the top level drop directory name refers to the date of the drop):

```
/[YYYYMMDD]/[site id]/[YYYY]/[MM]/[DD]/[HH]/[circuit ip address].log
```

If the log loader run is successful, the <tt>$loader_log</tt> will be empty.

Assuming the database and dependencies have been setup as in the Installation section of this document, you can run a load via:

```
    ./processor/load_drop.sh [database_name] > load.log 2> error.log
```

Depending on the amount of data to be loaded and amount of data already in the database, this may take some time (~20 hours for 200 million records)

Once loaded, you can query the data either from the cleaned full resolution table, circuit_reading, or any of the aggregated resolution tables, circuit_reading_minutely, circuit_reading_hourly and circuit_reading_daily.

Some sample queries:

```
-- get the number of minutely records, sum up the watt_hours consumed for all minutes and find the max/min of credit and time_stamp by site_id/ip_addr (circuit) combination
select site_id, ip_addr, count(*) num_records, sum(watt_hours_delta) sum_watt_hours, max(max_credit), min(min_credit), min(time_stamp), max(time_stamp) from circuit_reading_hourly group by site_id, ip_addr;

-- get the average watt_hours consumed over a day for site=ug01, circuit=192_168_1_202 
select avg(watt_hours_delta) avg_watt_hours from circuit_reading_daily where site_id='ug01' and ip_addr='192_168_1_202';
```

Running the Log Loader
----------------------

From a command-line prompt, go to the <tt>processor</tt> folder in this repo.

Next, locate the root folder containing the Shared Solar log csv files you wish to parse and load into the database.

If the log files are in <tt>/var/log/shared_solar/sd_logs/</tt> run this command:

```
$ python log_loader.py /var/log/shared_solar/sd_logs/
```

Since the log loader reports bad data through standard error, it is recommended to capture both streams into a separate file:

```
$ run_date=`date +"%b_%d_%Y_%H%M%S" --utc`
$ loader_log=`echo ${run_date}-load.log`
$ python log_loader.py /var/log/shared_solar/sd_logs/ > $loader_log 2>&1
```

The log loader expects that all the folders and individual circuit log files under the designated root folder have this structure:

```
/[site id]/[YYYY]/[MM]/[DD]/[HH]/[circuit ip address].log
```

If the log loader run is successful, the <tt>$loader_log</tt> will be empty.

