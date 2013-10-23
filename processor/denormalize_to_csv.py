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

# Determines the input/output field mapping
FIELD_MAP = {
             'drop_id': 'drop_id',
             'site_id': 'site_id',
             'ip_addr': 'ip_addr',
             'time_stamp': 'Time Stamp',
             'line_num': 'line_num',
             'watts': 'Watts',
             'volts': 'Volts',
             'amps': 'Amps',
             'watt_hours_sc20': 'Watt Hours SC20',
             'watt_hours_today': 'Watt Hours Today',
             'max_watts': 'Max Watts',
             'max_volts': 'Max Volts',
             'max_amps': 'Max Amps',
             'min_watts': 'Min Watts',
             'min_volts': 'Min Volts',
             'min_amps': 'Min Amps',
             'power_factor': 'Power Factor',
             'power_cycle': 'Power Cycle',
             'frequency': 'Frequency',
             'volt_amps': 'Volt Amps',
             'relay_not_closed': 'Relay Not Closed',
             'send_rate': 'Send Rate',
             'machine_id': 'Machine ID',
             'circuit_type': 'Type',
             'credit': 'Credit'
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
        with open(outfile, 'w') as csvoutput:
    
            first_line = csvinput.readline()
            # Simple check for properly formatted file (NOTE:  MAINS files will not have a credit field at the end)
            if (first_line.startswith("Time Stamp,Watts,Volts,Amps,Watt Hours SC20,Watt Hours Today,Max Watts,Max Volts,Max Amps,Min Watts,Min Volts,Min Amps,Power Factor,Power Cycle,Frequency,Volt Amps,Relay Not Closed,Send Rate,Machine ID,Type")):
		# reset read ptr
		csvinput.seek(0)
                reader = csv.DictReader(csvinput)
                writer = csv.writer(csvoutput, lineterminator='\n')
                writer.writerow(HEADER)

                """
                has_credit = True
                # handle the header row
                row = next(reader)
                # If the header row doesn't contain the Credit field, add it
		if row[-1] != 'Credit':
                    row.append('Credit')
                    has_credit = False

                # convert field names
                for field in row:
                    if field not in FIELD_MAP:
                        print("Skipping field: %s in file: %s skipping..." % (field, logfile))
                    else:
                        all.append(FIELD_MAP[field])

                row.insert(0, 'line_num')
                row.insert(1, 'site_id')
                row.insert(2, 'ip_addr')
                all.append(row)
                """
        
                all_rows = []
		line_num = 0
                for row in reader:
                    new_row = []
                    # add missing fields
                    row['drop_id'] = drop_id
                    row['site_id'] = site_id
                    row['ip_addr'] = ip_addr
                    row['line_num'] = line_num
                    # output fields in HEADER order
                    for field in HEADER:
                        input_field = FIELD_MAP[field]
                        input_val = 0 # default to 0 if field doesn't exist (i.e. credit field)
                        if input_field in row:
                            input_val = row[input_field]
                        new_row.append(input_val)
                    all_rows.append(new_row)
                    
                    line_num = line_num + 1
        
                writer.writerows(all_rows)
                line_num = 0

            else:
		sys.stderr.write("Empty or corrupted file: %s\n" % logfile)
    

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
