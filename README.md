shared_solar_data_warehouse
===========================

Description
-----------

This repository contains tools for assembling, loading, cleaning and aggregating <a href="http://sharedsolar.org/" target="_blank">Shared Solar</a> "raw" usage data into a database designed for simple retrieval at minutely, hourly and daily temporal resolutions.


Installation
------------

The current version depends on python to process the raw log files and postgresql to store the processed data.  

For ubuntu/debian, use apt-get to install the following packages:

```
sudo apt-get -y install git python-all-dev postgresql postgresql-contrib libicu-dev
```

We recommend setting up a sharedsolar user on your machine, you use the following command to do so:

```
useradd -p $(perl -e'print crypt("'<some_user_pwd>'", "<seed>")') -s "/bin/bash" -U -m -G sudo sharedsolar
```

Log back in as the sharedsolar user and setup the repo on your machine:  

```
git clone git@github.com:modilabs/shared_solar_data_warehouse.git
```

Next, as user <tt>postgres</tt> create the sharedsolar role and database:

```
# login as postgres
sudo -u postgres -i

# create DB user and set password as needed
psql -c "CREATE ROLE sharedsolar SUPERUSER LOGIN PASSWORD '<password_here>';"
# create DB
psql -c "CREATE DATABASE sharedsolar OWNER sharedsolar;"
# logout postgres user
exit
```

To simplify interaction with postgresql, add a .pgpass file to eliminate the need to enter a password each time (assumes a sharedsolar user has been created on your system):

```
# add .pgpass pwd file to eliminate password prompt for user
echo "*:*:*:sharedsolar:<password_here>" > .pgpass
chmod 600 .pgpass

```

Finally, install the tables via <tt>psql</tt> (you may not need the -U depending on how you setup the pgpass file above):

```
$ psql -d [database_name_goes_here] -U [authorized_user_goes_here] < sql/tables.sql
```

If successful, you should see:

```
CREATE TABLE
CREATE TABLE
```

Usage
-----

SharedSolar "raw" usage data consists of power meter data (i.e. watts, watt_hours...) recorded at 3 second intervals in iso-8859-1 encoded files (1 file per circuit hour).  

The basic workflow for loading data is:

1. SharedSolar Raw Data Drop (individual circuit hour log files)  
2. Assemble into CSV (via python denormalize_to_csv.py script)  
3. Load into Postgresql database (via bulk load "copy")  
4. De-duplicate and clean data (via sql scripts)  
5. Aggregate data into minutely, hourly and daily resolution tables (via sql)  

This workflow is encapsulated by the processor/load_script.sh which assumes the following folder structure in addition to what is in this repository:

```
PROJECT_DIR=shared_solar_data_warehouse # (THIS repo...i.e. the PROJECT_DIR)  
$PROJECT_DIR\load # (this is where the SharedSolar raw data drops to be loaded go)  
$PROJECT_DIR\processed # (this is where the files that have been processed go)  
```

The SharedSolar raw data drop directory needs to conform to the following directory structure/naming convention (the top level drop directory name refers to the date of the drop):

```
[YYYYMMDD]/[site id]/[YYYY]/[MM]/[DD]/[HH]/[circuit ip address].log
```

Assuming the database and dependencies have been setup as in the Installation section of this document, you can run a load via:

```
./processor/load_drop.sh [database_name] > load.log 2> error.log
```

Depending on the amount of data to be loaded, data already in the database and the computing resources available, this may take some time (~20 hours for 200 million records).

Any errors that occur during the load should halt the process and be output to stderr (or in error.log if command from above is used).  

All files originally in the load directory should be moved to the processed directory once they are no longer needed.  This should allow you to resume the load from where it left off if something fails.  

Once loaded, you can query the data either from the cleaned full resolution table, circuit_reading, or any of the aggregated resolution tables, circuit_reading_minutely, circuit_reading_hourly and circuit_reading_daily.

Some sample queries:

```
-- get the number of minutely records, sum up the watt_hours consumed for all minutes and find the max/min of credit and time_stamp by site_id/ip_addr (circuit) combination
select site_id, ip_addr, count(*) num_records, sum(watt_hours_delta) sum_watt_hours, max(max_credit), min(min_credit), min(time_stamp), max(time_stamp) from circuit_reading_hourly group by site_id, ip_addr;

-- get the average watt_hours consumed over a day for site=ug01, circuit=192_168_1_202 
select avg(watt_hours_delta) avg_watt_hours from circuit_reading_daily where site_id='ug01' and ip_addr='192_168_1_202';
```

To output the results of a query to a csv you can use a command like the following:  
```
psql -d sharedsolar -A -F, -c "select * from circuit_reading;" > circuit_reading.csv
```


