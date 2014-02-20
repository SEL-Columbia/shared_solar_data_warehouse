#Readme for Data_checker.py

Run datareader as follows:

$ python data_checker.py

Script connects to the database and outputs the result log to results., and after the script completes execution - the high level statistics are added to stats.txt. The dictionary in this case is stored in dict.dat and can be retrived using Python's cPickle module.

We will soon be adding new scripts to this system.

The code has recently been refactored- the processing task has been redesigned and broken into 3 components:

1) A 'History' class which includes all relevant data from the fraction of the query that has been processed.
2) A 'Process_task' class that performs most of the real processing in the lazy loaded dictionary.
3) A init function that initializes the history dictionary


General Outlier Output Format:
  A csv with the following fields (specific to shared_solar_data_warehouse):
  site_id, ip, timestamp, outlier_code, outlier_specifics
  
  outlier_specifics:  key_value pairs with outlier specific values
  
Outlier Definitions:
OUTLIER_WATT_HOUR_DECREASE:  
  Circuit's watt hours decrease from previous timestamp (it's a cumulative value)
  outlier_specifics:  decrease=$amount_of_decrease
  
OUTLIER_MACHINE_SWAP:  
  Circuit's machine id changes from previous timestamp
  output_specifics:  from_machine=$from_machine, to_machine=$to_machine
  
OUTLIER_ROLLING_THREE_SIGMA:
  Circuit's watts value for this record are outside of 3 std deviations from the mean of the nearest
  k records (ordered by timestamp)
  output_specifics:  mean=$mean, std_deviation=$std, value=$watts
  
