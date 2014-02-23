#Readme for Data_checker.py

Run datareader as follows:

$ python data_checker.py

Script connects to the database and outputs the result log to results., and after the script completes execution - the high level statistics are added to stats.txt. The dictionary in this case is stored in dict.dat and can be retrived using Python's cPickle module.

We will soon be adding new scripts to this system.

The code has recently been refactored- the processing task has been redesigned and broken into 3 components:

1) A 'History' class which includes all relevant data from the fraction of the query that has been processed.
2) A 'Process_task' class that performs most of the real processing in the lazy loaded dictionary.
3) A init function that initializes the history dictionary


<b>General Outlier Output Format:</b>
  A csv with the following fields (specific to shared_solar_data_warehouse):
  site_id, ip, timestamp, outlier_code, outlier_specifics
  
  <i>outlier_specifics</i>:  key_value pairs with outlier specific values
  
<b>Outlier Definitions:
<br>OUTLIER_WATT_HOUR_DECREASE:</b>
  Circuit's watt hours decrease from previous timestamp (it's a cumulative value)
  <br><i>outlier_specifics</i>:  decrease=$amount_of_decrease
  
<b>OUTLIER_MACHINE_SWAP:</b>
  Circuit's machine id changes from previous timestamp
  <br><i>output_specifics</i>:  from_machine=$from_machine, to_machine=$to_machine
  
<b>OUTLIER_ROLLING_THREE_SIGMA:</b>
  Circuit's watts value for this record are outside of 3 std deviations from the mean of the nearest
  k records (ordered by timestamp)
  <br><i>output_specifics</i>:  mean=$mean, std_deviation=$std, value=$watts
  
