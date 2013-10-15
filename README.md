shared_solar_data_warehouse
===========================

About
-----

This repository contains tools for analyzing <a href="http://sharedsolar.org/" target="_blank">Shared Solar</a> actual usage data.

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

