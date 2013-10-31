import os
import datetime
import csv
import sys

"""
denormalize_to_csv.py

usage:  python denormalize_to_csv.py logs_dir

description:  Script to take a directory of sharedsolar log files
              in csv format and denormalizes them such that they
              can be concatenated together into "one big table"
              of the same structure without losing any information
              (while duplicating some...hence "denormalize")
"""

# validation function
# returns a function that can be used to check a value against
# the given type and field length
# TODO:  Add range checks?  regex based?
def field_type_size_check(type_func, max_len):
    def type_size_fun(value):
        try:
           x = type_func(value)
           return len(value) <= max_len
        except:
           return False
    
    return type_size_fun
    
def validate_timestamp(value):
    (day, timestamp) =  (value[:8], value[9:]) # assume "YYYYMMDD HHMMSS" format
    try:
       day_int = int(day)
       timestamp_int = int(timestamp)
    except:
       return False 
    return len(day) <= 8 and len(timestamp) <= 6

# Determines the input/output field mapping and field type validation
FIELD_MAP = {
             'drop_id': ('drop_id', field_type_size_check(int, 8)),
             'site_id': ('site_id', field_type_size_check(str, 8)),
             'ip_addr': ('ip_addr', field_type_size_check(str, 16)),
             'time_stamp': ('Time Stamp', validate_timestamp), # account for added space
             'line_num': ('line_num', field_type_size_check(int, 8)),
             'watts': ('Watts', field_type_size_check(float, 10)),
             'volts': ('Volts', field_type_size_check(float, 10)),
             'amps': ('Amps', field_type_size_check(float, 10)),
             'watt_hours_sc20': ('Watt Hours SC20', field_type_size_check(float, 10)),
             'watt_hours_today': ('Watt Hours Today', field_type_size_check(float, 10)),
             'max_watts': ('Max Watts',field_type_size_check(float, 10)),
             'max_volts': ('Max Volts',field_type_size_check(float, 10)),
             'max_amps': ('Max Amps',field_type_size_check(float, 10)),
             'min_watts': ('Min Watts',field_type_size_check(float, 10)),
             'min_volts': ('Min Volts',field_type_size_check(float, 10)),
             'min_amps': ('Min Amps',field_type_size_check(float, 10)),
             'power_factor': ('Power Factor',field_type_size_check(float, 10)),
             'power_cycle': ('Power Cycle',field_type_size_check(float, 10)),
             'frequency': ('Frequency', field_type_size_check(float, 10)),
             'volt_amps': ('Volt Amps', field_type_size_check(float, 10)),
             'relay_not_closed': ('Relay Not Closed', field_type_size_check(int, 1)),
             'send_rate': ('Send Rate', field_type_size_check(int, 5)),
             'machine_id': ('Machine ID', field_type_size_check(int, 20)),
             'circuit_type': ('Type', field_type_size_check(str, 10)),
             'credit': ('Credit', field_type_size_check(float, 15)) 
}


REVERSE_FIELD_MAP = {
             'Time Stamp': 'time_stamp',
             'Watts': 'watts',
             'Volts': 'volts',
             'Amps': 'amps',
             'Watt Hours SC20': 'watt_hours_sc20',
             'Watt Hours Today': 'watt_hours_today',
             'Max Watts': 'max_watts',
             'Max Volts': 'max_volts',
             'Max Amps': 'max_amps',
             'Min Watts': 'min_watts',
             'Min Volts': 'min_volts',
             'Min Amps': 'min_amps',
             'Power Factor': 'power_factor',
             'Power Cycle': 'power_cycle',
             'Frequency': 'frequency',
             'Volt Amps': 'volt_amps',
             'Relay Not Closed': 'relay_not_closed',
             'Send Rate': 'send_rate',
             'Machine ID': 'machine_id',
             'Type': 'circuit_type',
             'Credit': 'credit'
}

INPUT_FIELDS = ['Time Stamp', 'Watts', 'Watt Hours SC20', 'Machine ID', 'Type', 'Credit']

# determines which fields are output and their order
HEADER=['drop_id','site_id','ip_addr','machine_id','time_stamp','line_num','circuit_type','watts','watt_hours_sc20','credit']


def write_denormalized_csv(logfile, drop_id, site_id, ip_addr):
    outfile = logfile.replace(".log", ".csv")
    with open(logfile,'r') as csvinput:
        with open(outfile, 'w') as csvoutput:  # note:  replaces existing files
    
            try: 
                first_line = csvinput.readline()
                # Simple check for properly formatted file (NOTE:  MAINS files will not have a credit field at the end)
                if (first_line.startswith("Time Stamp,Watts,Volts,Amps,Watt Hours SC20,Watt Hours Today,Max Watts,Max Volts,Max Amps,Min Watts,Min Volts,Min Amps,Power Factor,Power Cycle,Frequency,Volt Amps,Relay Not Closed,Send Rate,Machine ID,Type")):
                    # reset read ptr
                    csvinput.seek(0)
                    reader = csv.DictReader(csvinput)
                    writer = csv.writer(csvoutput, lineterminator='\n')
                    writer.writerow(HEADER)
            
                    line_num = 0
                    for row in reader:
                        new_row = []
                        # add missing fields
                        row['drop_id'] = drop_id
                        row['site_id'] = site_id
                        row['ip_addr'] = ip_addr
                        row['line_num'] = str(line_num)
                         
                        # format the time according to iso std for postgres timestamp field
                        timestamp = row['Time Stamp']
                        row['Time Stamp'] = "%s %s" % (timestamp[:8], timestamp[8:])
                        # output fields in HEADER order
                        for field in HEADER:
                            (input_field, validate_func) = FIELD_MAP[field]
                            input_val = "0" # default to "0" if field doesn't exist (i.e. credit field)
                            if input_field in row:
                                input_val = row[input_field]
                            # check if the value is OK
                            # TODO:  Do we want to just skip this line? (currently throws out the file)
                            if not validate_func(input_val):
                                raise Exception("Invalid field (%s) value (%s) at line %s" % (input_field, input_val[:20], line_num))
                            new_row.append(input_val)

                        writer.writerow(new_row)
                        line_num = line_num + 1
            
                    line_num = 0
    
                else:
                    raise Exception("Empty or corrupted")

            except Exception, e:
                sys.stderr.write("%s file: %s\n" % (e, logfile))

def denormalize_to_csv(logs_dir):

    for (dirpath,dirnames,filenames) in os.walk(logs_dir):
        for f in filenames:
            if f.endswith(".log"):
                # Note:  dir_info contents are drop_id/site_id/YYYY/MM/DD/HH
                dir_info = dirpath.split("/")
                drop_id = dir_info[-6] # get the drop_id from the parent of site dir
                site_id = dir_info[-5] # get the site from the dir (site is always 5 dirs up in the path)
                ip_addr = f[0:f.find(".")] # get the ip from the filename
                full_filename = os.path.join(dirpath, f)
                write_denormalized_csv(full_filename, drop_id, site_id, ip_addr)
 


if __name__=="__main__":
    import sys
    assert len(sys.argv) == 2, \
	"Usage: python denormalize_to_csv.py drop_dir"
    logs_dir = sys.argv[1]
    denormalize_to_csv(logs_dir) 
