#!/usr/bin/env python

"""
A script to open, parse, and insert Shared Solar SD log file csv data into the database.

There are two types of log files:

(1) A Main Circuit log file
Time Stamp,Watts,Volts,Amps,Watt Hours SC20,Watt Hours Today,Max Watts,Max Volts,Max Amps,Min Watts,Min Volts,Min Amps,Power Factor,Power Cycle,Frequency,Volt Amps,Relay Not Closed,Send Rate,Machine ID,Type
20130812020002,80.6,231.6,0.514,96163.7,160.7,807,231.7,516,806,231.5,511,60,148,50.0,1339,0,3,3512488618,MAINS

(2) A Regular Circuit log file
Time Stamp,Watts,Volts,Amps,Watt Hours SC20,Watt Hours Today,Max Watts,Max Volts,Max Amps,Min Watts,Min Volts,Min Amps,Power Factor,Power Cycle,Frequency,Volt Amps,Relay Not Closed,Send Rate,Machine ID,Type,Credit
20130812020006,10.6,231.2,0.081,30905.7,20.5,107,231.3,84,106,231.1,81,37,102,50.0,282,0,3,337793706,CIRCUIT,8252.0

"""

MAIN_LOG = ['Time Stamp','Watts','Volts','Amps','Watt Hours SC20','Watt Hours Today','Max Watts','Max Volts','Max Amps','Min Watts','Min Volts','Min Amps','Power Factor','Power Cycle','Frequency','Volt Amps','Relay Not Closed','Send Rate','Machine ID','Type']
REGR_LOG = ['Time Stamp','Watts','Volts','Amps','Watt Hours SC20','Watt Hours Today','Max Watts','Max Volts','Max Amps','Min Watts','Min Volts','Min Amps','Power Factor','Power Cycle','Frequency','Volt Amps','Relay Not Closed','Send Rate','Machine ID','Type','Credit']

import os, sys, datetime, threading
from db_utils import connect, search, insert
from settings import dbname, dbuser, pwd

def get_or_create_circuit (machine_id, site_id, ip_addr, is_main=False):
    """Lookup the machine_id, site_id and ip_addr in the circuit table and return its pk,
    creating a new entry only if the combination doesn't already exist"""

    conn = connect(dbname, dbuser, pwd)
    if conn:
        circuit_data = {'machine_id':machine_id, 'site_id':site_id, 'ip_addr':ip_addr, 'main_circuit':is_main}
        res = search (conn, """SELECT pk FROM circuit WHERE machine_id = %(machine_id)s and site_id = %(site_id)s and ip_addr = %(ip_addr)s""", circuit_data)
        if res is not None and len(res) > 0:
            return res[0][0]
        else:
            insert (conn, 'circuit', inserts=[circuit_data])
            return get_or_create_circuit (machine_id, site_id, ip_addr)

def parse_timestamp (ts):
    """Convert the ts string (in YYYYMMDDHHMISS format, e.g. 20130812020002) into a datetime.datetime object"""

    if len(ts) == 14:
        ts_parts = [
            ts[0:4],   # '2013'
            ts[4:6],   # '08'
            ts[6:8],   # '12'
            ts[8:10],  # '02'
            ts[10:12], # '00'
            ts[12:14]  # '02'
            ]
        try:
            return datetime.datetime( *tuple(map(int, ts_parts)) )
        except ValueError, e:
            print >>sys.stderr, 'Error: could not parse', ts, e
        
def convert_relay_closed (rc): 
    """Convert the 'Relay Not Closed' value string in the csv data into a boolean (0=False, 1=True)"""
    try:
        rc_val = int(rc)
        return rc == 1
    except ValueError, e:
        print >>sys.stderr, 'Error: could not parse relay data', rc, e
    
def convert_field_name (n):
    """Turn the name string into the column name of the table (make lowercase and replace space with underscore)"""
    return n.lower().replace(' ', '_')

def parse_log_line (circuit_id, l, ignore=[18,19]):
    """Convert the list of data found in a single line of the csv file into a dict for insertion into the power_reading table"""

    data = {'circuit':circuit_id}
    for i, d in enumerate(l):
        if i not in ignore:
            if i == 0:
                data[ convert_field_name(MAIN_LOG[i]) ] = parse_timestamp(d)
            elif i == 16:
                data[ convert_field_name(MAIN_LOG[i]) ] = convert_relay_closed(d)
            else:
                if i > len(MAIN_LOG):
                    field = convert_field_name(MAIN_LOG[i])
                else:
                    field = convert_field_name(REGR_LOG[i])

                try:
                    val = float(d) # everything else is numeric
                    data[field] = val
                except ValueError, e:
                    data[field] = None
                    print >>sys.stderr, 'Error: could not parse', d, 'at index', i, e
    return data

def load_log (path, filename, site_id, ip_addr):
    """Load and parse the csv log files, and insert them into the circuit and power_reading tables

    Two types of log files:
    (1) A Main Circuit log file    -- column headers in MAIN_LOG list
    (2) A Regular Circuit log file --                   REGR_LOG

    """
    try:
        f = open(os.path.join(path, filename), "r")
        data = f.read()
        f.close()

        circuit_pk_list = {} # memoize these here, to avoid calling get_or_create_circuit() too much

        data_dicts = [] # list of dicts to bulk insert
        for i, line in enumerate(data.splitlines()):
            log_data = line.split(',')
            # ignore the csv file header line
            if log_data[0] != """Time Stamp""":

                # get the circuit id (circuit table pk)
                machine_id = log_data[18]
                circuit_id = '-'.join([machine_id, site_id, ip_addr]) 
                if not circuit_pk_list.has_key(circuit_id):
                    circuit_pk_list[circuit_id] = get_or_create_circuit(machine_id, site_id, ip_addr, (len(log_data) == len(MAIN_LOG)))
                circuit_pk = circuit_pk_list[circuit_id]

                if len(log_data) == len(MAIN_LOG) or len(log_data) == len(REGR_LOG):
                    line_dict = parse_log_line (circuit_pk, log_data)
                    if None in line_dict:
                        print >>sys.stderr, "Error: bad content in line", i, line, "from file", os.path.join(path, filename)
                    else:
                        data_dicts.append( line_dict )

        if len(data_dicts) > 0:
            conn = connect(dbname, dbuser, pwd)
            if conn:
                insert (conn, 'power_reading', inserts=data_dicts, close_conn=True)

    except IOError:
        print >>sys.stderr, "Error: could not open", os.path.join(path, filename)

def get_log_files (root_path):
    """Walk the file system starting at the log file root folder and return a dict:
    { key=site_id, value=[ {path, log filename, ip address} ] }
    """

    res = {}
    for f in os.walk(root_path):
        path  = f[0]
        files = f[-1:][0]
        if len(files) > 0:
            # get the site id from the first part of the path after the root folder
            site_id = filter(None, path.split(root_path)[1].split(os.path.sep))[0]
            if res.has_key(site_id):
                file_data = res[site_id]
            else:
                file_data = []
            
            # find each log file and its ip address from the filename
            for file_tuple in map(lambda x: os.path.splitext(x), files):
                if file_tuple[1] == '.log':
                    ip_addr = file_tuple[0].replace('_', '.') # the file name is '127_0_0_1' so reformat as a real ip
                    res_key = ' '.join([site_id, ip_addr])
                    file_data.append({'path':path, 'file':''.join(file_tuple), 'ip':ip_addr})

            if len(file_data) > 0:
                res[site_id] = file_data
    return res


class DoLogLoading (threading.Thread):
    """This class uses python's threading library in an "embarrassingly parallel" way,
    to invoke load_log() for a specific set of csv files by site id, as an independent thread."""

    site_id = None
    file_dicts = []

    def __init__ (self, site_id, file_dicts):
        threading.Thread.__init__(self)
        self.site_id = site_id
        self.file_dicts = file_dicts

    def run(self):
        for fd in self.file_dicts:
            load_log (fd['path'], fd['file'], self.site_id, fd['ip'])


def main():
    """Command-line entry point: provide the root folder of the csv log files and this module will parse + load all the csv files it finds under it"""

    if len(sys.argv[1:]) != 1:
        print "\nUsage:\n\tpython "+sys.argv[0]+" [SharedSolar SD Log Files Root Folder]\n\n"
    else:
        # see if there are log files in the root folder specified
        log_files = get_log_files(sys.argv[1])
        # dispatch them as several independent threads by site id
        for site_id in log_files.keys():
            ll = DoLogLoading(site_id, log_files[site_id])
            ll.start()

if __name__ == "__main__":
    main()

