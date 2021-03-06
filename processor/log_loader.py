#!/usr/bin/env python

"""
A script to open, parse, and insert Shared Solar SD log file csv data
into the database.

There are two types of log files, a main circuit, and a regular circuit,
defined in processor.csv_format.py.

"""

import os, sys, threading
from db_utils import connect, search, insert
from settings import DBNAME, DBUSER, PWD
from csv_formats import MAIN_LOG, MAIN_LEN, REGR_LOG, REGR_LEN, FIELDS_TO_IGNORE, TIMESTAMP_FIELD, RELAY_FIELD
from csv_formats import parse_timestamp, convert_relay_closed, convert_field_name, parse_field, get_site_id_from_path, reformat_ip_addr

def get_or_create_circuit (machine_id, site_id, ip_addr, is_main=False):
    """Lookup the machine_id, site_id and ip_addr in the circuit table
    and return its pk, creating a new entry only if the combination
    doesn't already exist"""

    conn = connect(DBNAME, DBUSER, PWD)
    if conn:
        circuit_data = {'machine_id':machine_id,
                        'site_id':site_id,
                        'ip_addr':ip_addr,
                        'main_circuit':is_main}
        res = search (conn,
                      """SELECT pk FROM circuit WHERE
                      machine_id = %(machine_id)s and
                      site_id = %(site_id)s and
                      ip_addr = %(ip_addr)s""",
                      circuit_data)
        if res is not None and len(res) > 0:
            return res[0][0]
        else:
            insert (conn, 'circuit',
                    columns=circuit_data.keys(),
                    inserts=[circuit_data])
            return get_or_create_circuit (machine_id, site_id, ip_addr)


def parse_log_line (circuit_id, line, ignore=FIELDS_TO_IGNORE):
    """Convert the list of data found in a single line of the csv file
    into a dict for insertion into the power_reading table"""

    data = {'circuit':circuit_id}
    for i, datum in enumerate(line):
        if i not in ignore:
            if i > MAIN_LEN:
                field = convert_field_name(REGR_LOG[i])
            else:
                field = convert_field_name(MAIN_LOG[i])

            if i == TIMESTAMP_FIELD:
                data[field] = parse_timestamp(datum)
            elif i == RELAY_FIELD:
                data[field] = convert_relay_closed(datum)
            else:
                data[field] = parse_field(datum)

    return data

def load_log (path, filename, site_id, ip_addr):
    """Load and parse the csv log files, and insert them into the circuit
    and power_reading tables

    Two types of log files:
    (1) A Main Circuit log file    -- column headers in MAIN_LOG list
    (2) A Regular Circuit log file --                   REGR_LOG

    """
    try:
        file_obj = open(os.path.join(path, filename), "r")
        data = file_obj.read()
        file_obj.close()

        # memoize these here, to avoid calling get_or_create_circuit() too much
        circuit_pk_list = {} 

        data_dicts = [] # list of dicts to bulk insert
        for i, line in enumerate(data.splitlines()):
            log_data = line.split(',')
            if len(log_data) == MAIN_LEN or len(log_data) == REGR_LEN:
                
                # ignore the csv file header line
                if log_data[0] != """Time Stamp""":

                    # get the circuit id (circuit table pk)
                    machine_id = log_data[18]
                    circuit_id = '-'.join([machine_id, site_id, ip_addr]) 
                    if not circuit_pk_list.has_key(circuit_id):
                        circuit_pk = get_or_create_circuit(
                            machine_id, site_id, ip_addr,
                            (len(log_data) == len(MAIN_LOG)))
                        circuit_pk_list[circuit_id] = circuit_pk
                    circuit_pk = circuit_pk_list[circuit_id]

                    line_dict = parse_log_line (circuit_pk, log_data)
                    if None in line_dict:
                        print >> sys.stderr, ' '.join(["Error: bad content line",
                                                       i,
                                                       line,
                                                       "from file",
                                                       os.path.join(path,
                                                                    filename)])
                    else:
                        data_dicts.append( line_dict )

        if len(data_dicts) > 0:
            conn = connect(DBNAME, DBUSER, PWD)
            if conn:
                insert (conn, 'power_reading',
                        columns=data_dicts[0].keys(),
                        inserts=data_dicts,
                        close_conn=True)

    except IOError:
        print >> sys.stderr, "Error: could not open", os.path.join(path, filename)

def get_log_files (root_path):
    """Walk the file system starting at the log file root folder and
    return a dict:

    { key=site_id, value=[ {path, log filename, ip address} ] }

    """

    res = {}
    for file_obj in os.walk(root_path):
        path  = file_obj[0]
        files = file_obj[-1:][0]
        if len(files) > 0:
            site_id = get_site_id_from_path(root_path, path)
            if res.has_key(site_id):
                file_data = res[site_id]
            else:
                file_data = []
            
            # find each log file and its ip address from the filename
            for file_tuple in map(lambda x: os.path.splitext(x), files):
                if file_tuple[1] == '.log':
                    file_data.append({'path':path,
                                      'file':''.join(file_tuple),
                                      'ip':reformat_ip_addr(file_tuple[0])})

            if len(file_data) > 0:
                res[site_id] = file_data
    return res


class DoLogLoading (threading.Thread):
    """This class uses python's threading library in an "embarrassingly
    parallel" way, to invoke load_log() for a specific set of csv files
    by site id, as an independent thread."""

    site_id = None
    file_dicts = []

    def __init__ (self, site_id, file_dicts):
        threading.Thread.__init__(self)
        self.site_id = site_id
        self.file_dicts = file_dicts

    def run(self):
        for file_dict in self.file_dicts:
            load_log (file_dict['path'],
                      file_dict['file'],
                      self.site_id,
                      file_dict['ip'])


def main():
    """Command-line entry point: provide the root folder of the csv log
    files and this module will parse + load all the csv files it finds
    under it"""

    if len(sys.argv[1:]) != 1:
        print ' '.join(["\nUsage:\n\tpython",
                        sys.argv[0],
                        "[SharedSolar SD Log Files Root Folder]\n\n"])
    else:
        # see if there are log files in the root folder specified
        log_files = get_log_files(sys.argv[1])
        # dispatch them as several independent threads by site id
        for site_id in log_files.keys():
            log_load = DoLogLoading(site_id, log_files[site_id])
            log_load.start()

if __name__ == "__main__":
    main()

