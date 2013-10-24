#!/bin/bash

# Script to load a sharedsolar drop directory into the 
# "data warehouse"

usage ()
{
  echo "usage: $0 drop_dir database output_dir"
  exit
}

if [ $# -lt 3 ] 
then
  usage
  exit 1
fi

drop_dir=$1
database=$2
output_dir=$3
unique_date=`date +%Y%m%d%m%S`
outfile="$output_dir/denorm_$unique_date.csv"

# create output dir for denormalized files if it doesn't exist
mkdir -p $output_dir

echo "creating denormalized csvs..."
python denormalize_to_csv.py $drop_dir || { echo "denormalize_to_csv failed, exiting"; exit 1; }

echo "concatenating csv's into $outfile..."
find $drop_dir -name '*csv' | xargs cat | grep -v '^drop' > $outfile || { echo "concatenating csv's failed, exiting"; exit 1; }

echo "loading denormalized csv $outfile into raw_circuit_reading table..."
psql -v ON_ERROR_STOP=1 -d $database <<HERE
COPY raw_circuit_reading (drop_id,site_id,ip_addr,machine_id,time_stamp,line_num,circuit_type,watts,watt_hours_sc20,credit) FROM '$outfile' (FORMAT csv)
HERE

if [ "$?" -ne 0 ]; 
then 
  echo "loading denormalized table failed, exiting"
  exit 1 
fi

echo "normalizing the raw data into circuit and power_reading tables"
psql -d $database < ../sql/load.sql || { echo "normalizing raw data failed, exiting"; exit 1; }



