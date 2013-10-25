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

if ! [[ -d $load_dir ]] ; then
    echo "Load directory $load_dir doesn't exist, exiting"
    exit 1
fi

# create processed dir if it doesn't exist
mkdir -p $processed_dir

echo "creating denormalized csvs..."
for drop_dir in `find $load_dir/* -maxdepth 0 -type d`; do
    echo "Running denormalize_to_csv on $drop_dir..."
    python denormalize_to_csv.py $drop_dir || { echo "denormalize_to_csv failed for $drop_dir, exiting"; exit 1; }

    denorm_csv=$(basename "$drop_dir").csv
    echo "concatenating csv's into $load_dir/$denorm_csv..."
    find $drop_dir -name '*csv' | xargs cat | grep -v '^drop' > $load_dir/$denorm_csv || { echo "concatenating csv's failed for $drop_dir, exiting"; exit 1; }

    echo "moving $drop_dir to processed..."
    mv $drop_dir $processed_dir
done

echo "loading denormalized csvs into postgres..."
for csv_file in `find $load_dir/* -maxdepth 0 -type f -name '*.csv'`; do
    echo "loading denormalized csv $csv_file into raw_circuit_reading table..."

    # HERE DOC formatting is ugly because I don't know how to get bash 
    # to ignore leading whitespace
    psql -v ON_ERROR_STOP=1 -d $database <<HERE
COPY raw_circuit_reading (drop_id,site_id,ip_addr,machine_id,time_stamp,line_num,circuit_type,watts,watt_hours_sc20,credit) FROM '$csv_file' (FORMAT csv)
HERE
    
    if [ "$?" -ne 0 ]; 
    then 
      echo "loading denormalized table failed, exiting"
      exit 1 
    fi
    
    echo "moving $csv_file to processed..."
    mv $csv_file $processed_dir
done

# This might be a good place to index the raw data to speed up de-dup
echo "de-duplicate raw table and loading circuit_reading table..."
psql -v ON_ERROR_STOP=1 -d $database < $sql_dir/de_dup.sql

if [ "$?" -ne 0 ]; 
then 
  echo "loading circuit_reading table failed, exiting"; exit 1;
fi

echo "cleaning circuit_reading table..."
psql -v ON_ERROR_STOP=1 -d $database < $sql_dir/filter.sql

if [ "$?" -ne 0 ]; 
then 
  echo "cleaning circuit_reading table failed, exiting"; exit 1;
fi
