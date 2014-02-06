#Readme for Data_checker.py

Run datareader as follows:

$ python data_checker.py

Script connects to the database and outputs the result log to results., and after the script completes execution - the high level statistics are added to stats.txt. The dictionary in this case is stored in dict.dat and can be retrived using Python's cPickle module.

We will soon be adding new scripts to this system.

The code has recently been refactored- the processing task has been redesigned and broken into 3 components:

1) A 'History' class which includes all relevant data from the fraction of the query that has been processed.
2) A 'Process_task' class that performs most of the real processing in the lazy loaded dictionary.
3) A init function that initializes the history dictionary
