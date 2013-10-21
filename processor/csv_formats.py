#!/usr/bin/env python

"""
This file conatins the logical definition of the two types of log files:

(1) A Main Circuit log file :	MAIN_LOG

(2) A Regular Circuit log file:	REGR_LOG

Along with parsing and type conversion functions for this data.

"""

import sys, datetime, os

MAIN_LOG = [ # header, example
    'Time Stamp',	# 20130812020002,
    'Watts', 		# 80.6,
    'Volts',		# 231.6,
    'Amps',		# 0.514,
    'Watt Hours SC20',	# 96163.7,
    'Watt Hours Today',	# 160.7,
    'Max Watts',	# 807,
    'Max Volts',	# 231.7,
    'Max Amps',		# 516,
    'Min Watts',	# 806,
    'Min Volts',	# 231.5,
    'Min Amps',		# 511,
    'Power Factor',	# 60,
    'Power Cycle',	# 148,
    'Frequency',	# 50.0,
    'Volt Amps',	# 1339,
    'Relay Not Closed',	# 0,
    'Send Rate',	# 3,
    'Machine ID',	# 3512488618,
    'Type'		# MAINS,
]
MAIN_LEN = len(MAIN_LOG)

REGR_LOG = [ # header, example
    'Time Stamp',	# 20130812020006,
    'Watts',		# 10.6,
    'Volts',		# 231.2,
    'Amps',		# 0.081,
    'Watt Hours SC20',	# 30905.7,
    'Watt Hours Today',	# 20.5,
    'Max Watts',	# 107,
    'Max Volts',	# 231.3,
    'Max Amps',		# 84,
    'Min Watts',	# 106,
    'Min Volts',	# 231.1,
    'Min Amps',		# 81,
    'Power Factor',	# 37,
    'Power Cycle',	# 102,
    'Frequency',	# 50.0,
    'Volt Amps',	# 282,
    'Relay Not Closed',	# 0,
    'Send Rate',	# 3,
    'Machine ID',	# 337793706,
    'Type',		# CIRCUIT,
    'Credit'		# 8252.0
]
REGR_LEN = len(REGR_LOG)

FIELDS_TO_IGNORE = [18, 19] # b/c these are normalized in the db
TIMESTAMP_FIELD  = 0
RELAY_FIELD      = 16

def parse_timestamp (ts_str):
    """Convert the ts_str string (in YYYYMMDDHHMISS format,
    e.g. 20130812020002) into a datetime.datetime object"""

    if len(ts_str) == 14:
        ts_parts = [
            ts_str[0:4],   # '2013'
            ts_str[4:6],   # '08'
            ts_str[6:8],   # '12'
            ts_str[8:10],  # '02'
            ts_str[10:12], # '00'
            ts_str[12:14]  # '02'
            ]
        try:
            return datetime.datetime( *tuple(map(int, ts_parts)) )
        except ValueError, val_err:
            print >> sys.stderr, 'Error: could not parse', ts_str, val_err
        
def convert_relay_closed (rc_str): 
    """Convert the 'Relay Not Closed' value string in the csv data into
    a boolean (0=False, 1=True)"""

    try:
        rc_val = int(rc_str)
        return rc_val == 1
    except ValueError, val_err:
        print >> sys.stderr, ' '.join(['Error: could not parse relay data',
                                       rc_str,
                                       val_err])
    
def convert_field_name (field_name):
    """Turn the name string into the column name of the table (make
    lowercase and replace space with underscore)"""

    return field_name.lower().replace(' ', '_')

def parse_field (field_data):
    """Parse the field data with the default conversion method (everything
    is numeric except for timestamp and relay closed)"""

    try:
        return float(field_data)
    except ValueError, val_err:
        print >> sys.stderr, 'Error: could not parse', field_data, val_err

def get_site_id_from_path (root_path, path):
    """Get the site id from the first part of the path,
    right after the root folder"""

    return filter(None, path.split(root_path)[1].split(os.path.sep))[0]

def reformat_ip_addr (ip_str):
    """The IP address defined in the file name is in '127_0_0_1' format,
    so reformat as a dot-separated string."""

    return ip_str.replace('_', '.')
