#!/usr/bin/env python

"""
A definition of the two types of log files:

(1) A Main Circuit log file :	MAIN_LOG

(2) A Regular Circuit log file:	REGR_LOG

"""

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

