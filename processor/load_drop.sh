#!/bin/bash

# Script to load a sharedsolar drop directory into the 
# "data warehouse"
#
# Drop directories should be placed ins DIR/load directory
# (see README for "drop" directory structure)

usage ()
{
  echo "usage: $0 database"
  exit
}

if [ $# -lt 1 ] 
then
  usage
  exit 1
fi

# get the current directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$( cd $SCRIPT_DIR/.. && pwd )"

database=$1
unique_date=`date +%Y%m%d%m%S`
sql_dir="$PROJECT_DIR/sql"
load_dir="$PROJECT_DIR/load"
processed_dir="$PROJECT_DIR/processed"


echo "`date +"%Y%m%d %H%M%S"`: Running script on PROJECT_DIR $PROJECT_DIR..."

if ! [[ -d $load_dir ]] ; then
    echo "`date +"%Y%m%d %H%M%S"`: Load directory $load_dir doesn't exist, exiting"
    exit 1
fi

# create processed dir if it doesn't exist
mkdir -p $processed_dir

echo "`date +"%Y%m%d %H%M%S"`: Creating denormalized csvs..."
for drop_dir in `find $load_dir -maxdepth 1 -mindepth 1 -type d`; do
    echo "`date +"%Y%m%d %H%M%S"`: Running denormalize_to_csv on $drop_dir..."
    python $PROJECT_DIR/processor/denormalize_to_csv.py $drop_dir || { echo "denormalize_to_csv failed for $drop_dir, exiting"; exit 1; }

    denorm_csv=$(basename "$drop_dir").csv
    echo "`date +"%Y%m%d %H%M%S"`: Concatenating csv's into $load_dir/$denorm_csv..."
    # NOTE:  Convert from iso-8859-1 to utf-8 since postgres db is setup for utf-8
    find $drop_dir -name '*.csv' -exec cat {} \; | grep -v '^drop' | uconv -f iso-8859-1 -t utf-8 > $load_dir/$denorm_csv || { echo "concatenating csv's failed for $drop_dir, exiting"; exit 1; }

    echo "`date +"%Y%m%d %H%M%S"`: Moving $drop_dir to processed..."
    mv $drop_dir $processed_dir
done

# DROP raw_circuit_reading index prior to loading (to speed up load)
echo "`date +"%Y%m%d %H%M%S"`: DROP INDEX on raw_circuit_reading table..."
psql -v ON_ERROR_STOP=1 -d $database < $sql_dir/drop_raw_index.sql

if [ "$?" -ne 0 ]; 
then 
  echo "`date +"%Y%m%d %H%M%S"`: Dropping index on raw_circuit_reading table failed, exiting"; exit 1;
fi

echo "`date +"%Y%m%d %H%M%S"`: Loading denormalized csvs into postgres..."
for csv_file in `find $load_dir -maxdepth 1 -mindepth 1 -type f -name '*.csv'`; do
    echo "`date +"%Y%m%d %H%M%S"`: Loading denormalized csv $csv_file into raw_circuit_reading table..."

    # HERE DOC formatting is ugly because I don't know how to get bash 
    # to ignore leading whitespace
    psql -v ON_ERROR_STOP=1 -d $database <<HERE
COPY raw_circuit_reading (drop_id,site_id,ip_addr,machine_id,time_stamp,line_num,circuit_type,watts,watt_hours_sc20,credit) FROM '$csv_file' (FORMAT csv)
HERE
    
    if [ "$?" -ne 0 ]; 
    then 
      echo "`date +"%Y%m%d %H%M%S"`: Loading denormalized table failed, exiting"
      exit 1 
    fi
    
    echo "`date +"%Y%m%d %H%M%S"`: Moving $csv_file to processed..."
    mv $csv_file $processed_dir
done

# Index raw_circuit_reading to speed up de-dup
echo "`date +"%Y%m%d %H%M%S"`: Index raw_circuit_reading table..."
psql -v ON_ERROR_STOP=1 -d $database < $sql_dir/index_raw_circuit_reading.sql

if [ "$?" -ne 0 ]; 
then 
  echo "`date +"%Y%m%d %H%M%S"`: Indexing raw_circuit_reading table failed, exiting"; exit 1;
fi

# De-dup
echo "`date +"%Y%m%d %H%M%S"`: de-duplicate raw table and loading circuit_reading table..."
psql -v ON_ERROR_STOP=1 -d $database < $sql_dir/de_dup.sql

if [ "$?" -ne 0 ]; 
then 
  echo "`date +"%Y%m%d %H%M%S"`: loading circuit_reading table failed, exiting"; exit 1;
fi

# Index circuit_reading to speed up filter
echo "`date +"%Y%m%d %H%M%S"`: Index circuit_reading table..."
psql -v ON_ERROR_STOP=1 -d $database < $sql_dir/index_circuit_reading.sql

if [ "$?" -ne 0 ]; 
then 
  echo "`date +"%Y%m%d %H%M%S"`: Indexing circuit_reading table failed, exiting"; exit 1;
fi


echo "`date +"%Y%m%d %H%M%S"`: cleaning circuit_reading table..."
psql -v ON_ERROR_STOP=1 -d $database < $sql_dir/filter.sql

if [ "$?" -ne 0 ]; 
then 
  echo "`date +"%Y%m%d %H%M%S"`: cleaning circuit_reading table failed, exiting"; exit 1;
fi

echo "`date +"%Y%m%d %H%M%S"`: Aggregating timeseries data..."
psql -v ON_ERROR_STOP=1 -d $database < $sql_dir/aggregate.sql

if [ "$?" -ne 0 ]; 
then 
  echo "`date +"%Y%m%d %H%M%S"`: aggregating data failed, exiting"; exit 1;
fi

echo "`date +"%Y%m%d %H%M%S"`: Complete!"
