import psycopg2 as psql
import collections
import cPickle as pickle
#from pylib import *

#Script to find instances where the machine_id changes


def lazyLoad(query, con):
    #Setting up a namedcursor to enable lazy loading
    cur = con.cursor("First")
    try:
        cur.execute(query)
        #Now the result is loaded (supposedly)
    except Exception as e:
        print e.pgerror
    print "Lazy loading Cursor initialized"
    return cur

def processOutput(con, query, process_batch, history_init=None, final_commit=None, batchsize=10000):
    """
    cur is a namedcursor to the output- batchsize is a tuning parameter to determine how many lines
    are to be read from the table in one operation.

    "batchsize" can be used as a tuning parameter - increasing it means storing a larger chunk of the
    data in memory, which in turn means that more
    """    
    cur = lazyLoad(query,con)
    batch = cur.fetchmany(batchsize)
    history = history_init()
    while(batch!=[]):
        #Get a 'batchsize' number of entries from the DB
        history = process_batch(batch,history)
        batch  = cur.fetchmany(batchsize)
        """
        Execution of query traversal complete
        Now executing final commit
        """
    if (final_commit!=None):
        final_commit(history)
    return

"""
headings = ['drop_id','line_num','site_id','machine_id','ip_addr','circuit_type','time_stamp','watts','watt_hours','credit']
history["maxlines"] = 261014681
Computed via SELECT count(*); on the database
"""
class UMIDQuery():
    """
    Naming conventions still need to be discussed.
    Wrapping them in this class so that the general names query, history_init, process_batch and final_commit
    can be reused with still maintaining quality.

    I think this function can be converted into a class to incorporate more features in the future-
    Possible future structure of the class-
    - field: query
    - field: results_file
    - field: anomaly_name
    - func : history_init -> defaultdict
    - func : sub_process (a sub_process can be called in a generalized way within
    a generic process_batch function to perform processing for multiple queries simultaneously)
    - func : output_summary (renamed version of final_commit)

    """

    query = "SELECT * from {0} ORDER BY site_id, ip_addr,time_stamp;"    
    resultsfile = "results.txt"
    statsfile = "stats.txt"
    watthours_anomaly = "OUTLIER_WATT_HOUR_DECREASE"
    machineswap_anomaly = "OUTLIER_MACHINE_SWAP"
    table = 'raw_circuit_reading'
    testTable = "raw_circuit_reading_small_test"
    dbname = 'sharedsolar'
    active_query = query.format(table)
    # -- -- Utility Function --  -- #

    def setTest(self):
        self.active_query = query.format(self.table)
        return
        
    def setDev(self):
        self.active_query = query.format(self.testTable)
        return
        
    def setResultsFile(self,filename):
        self.resultsfile = filename
        return

    def setStatsFile(self, filename):
        self.statsfile = filename
        return

    def setDBName(self, dbname):
        self.dbname = dbname
    # -- -- Utilies end -- -- #    

    def history_init(self):
        history = {}
        window_size = 100
        print("Initializing History")
        history["window"] = [] #Array initialized
        history["linecount"] =0
        history["count"] = 0
        history["wattanomalies"]= 0
        history["prev"] = -float("inf")
        history["prevc"] = float("inf")

        history["prev_row"] = [None, None, None, None, None, None, None, -float("inf"), float("inf")]

        """
        The 'prev_row' key stores an entire row of values in format
        headings = ['drop_id','line_num','site_id','machine_id','ip_addr','circuit_type','time_stamp','watts','watt_hours','credit']
        """

        """
        Experimental idea - replace the history list with
        a dict for comprehensibility? Might have to be wrapped in a class
        for easy initialization from a row of data
        history["prev_row"] = collections.defaultdict(int)
        history["watt_hours"] = -float("inf")
        history["credit"] = float("inf")
        """
        
        history["thousand"] = ['Initialized']
        history["dic"] = collections.defaultdict(list)

        """
        Erase the contents of  resultsfile
        """
        open(self.resultsfile,'w').close()
        return history
	
    def process_row_watt_reduction(self, row, history):
        text = ""
        (drop_id, line_num , site_id ,
         machine_id, ip, circuit_type ,
         timestamp, watts, watt_hours , credit ) = row
        if history["linecount"]%1000000==0:
            print (history["linecount"]/1000000,
                                    "Million lines parsed")
            print "Passed:", history["linecount"]
        if watt_hours<history["prev_row"][8]:
                """
                Following check to make sure the two roles belong to
                the same class and that there is no
                transition.      """
                if (history["prev_row"][3]==machine_id
                    and history["prev_row"][4]==ip
                    and history["prev_row"][2]==site_id):
                    history["wattanomalies"]+=1
                    text +=  (site_id + "," + ip + "," + str(timestamp) + ","
                              + self.watthours_anomaly + "," + " decrease="
                              + str(history["prev_row"][8]-watt_hours) + '\n')
        return text, history

    def process_row_machineId_swap (self, row, history):
        text = "" 
        (drop_id, line_num , site_id ,
         machine_id, ip, circuit_type ,
         timestamp, watts, watt_hours , credit ) = row
        inDic = False
        for tup in history["dic"][( site_id, ip )]:
            if machine_id == tup[0]:
                inDic = True
                break
        if not inDic:
            history["dic"][(site_id,ip)] += [(machine_id,timestamp)]
            if len(history["dic"][(site_id,ip)])>1:
                history['count']+=1
                print "Count:", history["count"], history["linecount"], len(history["dic"])
                #Picks the last machine from the list stored in history["dic"]
                to_machine   = history["dic"][(site_id,ip)][-1][0]
                from_machine = history["dic"][(site_id,ip)][-2][0]
                text +=  (site_id + "," + ip + "," + str(timestamp) + ","
                          + self.machineswap_anomaly + ","
                          + "from_machine="+ str(from_machine)
                          +" "+ "to_machine="+ str(to_machine) + '\n')
        return text, history
                            
    def process_batch(self, batch,history=None):
        text = ""
        for row in batch:
                #print len(row)
                if len(row)==10:
                    history["linecount"] +=1

                    """
                    Key for the mappings of different parameters in each row of the Database

                    drop id = row[1] ;      site_id = row[2]
                    machine_id = row[3] ;   ip = row[4]
                    circuit_type = row[5];  timestamp = row[6]
                    watts = row[7];         watt_hours = row[8]
                    credit = row[9]
                    """

                    #This needs to be put into a for loop
                    temptext, history = self.process_row_watt_reduction(row, history)
                    text+=temptext
                    temptext, history = self.process_row_machineId_swap(row, history)
                    text+=temptext
                    
                    history["prev_row"] = row
                    """Indicator boolean - indicates whether machineID has been
                    Seen before for a given site_id/ip pair"""

        f = open(self.resultsfile,'a')
        f.write(text)
        f.close()
        f = open("dict.dat",'w')
        pickle.dump(history["dic"],f)
        f.close()
        return history

    def final_commit(self, history):
        f = open(self.statsfile,'w')
        strbuilder = ""
        strbuilder += "Length of Site_id, IP Pairs:" + str(len(history["dic"])) +'\n'
        pickle.dump(history["dic"],open("dict.dat",'w'))
        strbuilder += "Number of lines parsed: "+ str(history["linecount"]) +'\n'
        strbuilder += "Number of switch anomalies encountered: " + str(history["count"]) + '\n'
        strbuilder += "Number of watt decrease anomalies encountered:"+ str(history["wattanomalies"])+'\n'
        f.write(strbuilder)
        f.close()
        
    def lazyLoad(self, query, con):
        #Setting up a namedcursor to enable lazy loading
        cur = con.cursor("First")
        try:
            cur.execute(query)
            #Now the result is loaded (supposedly)
        except Exception as e:
            print e.pgerror
        print "Lazy loading Cursor initialized"
        return cur

    def processOutput(con, query, process_batch, history_init=None, final_commit=None, batchsize=10000):
        """
    cur is a namedcursor to the output- batchsize is a tuning parameter to determine how many lines
    are to be read from the table in one operation.

    "batchsize" can be used as a tuning parameter - increasing it means storing a larger chunk of the
    data in memory, which in turn means that more
        """
        print "Process output activated"
        cur = self.lazyLoad(query,con)
        batch = cur.fetchmany(batchsize)
        history = self.history_init()
        while(batch!=[]):
            #Get a 'batchsize' number of entries from the DB
            history = self.process_batch(batch,history)
            batch  = cur.fetchmany(batchsize)
            """
        Execution of query traversal complete
        Now executing final commit
            """
        if (self.final_commit!=None):
            self.final_commit(history)
        cur.close()
        return

    def run(self):
        con = psql.connect(dbname=self.dbname,
                 user="sharedsolar")
        processOutput(con, self.active_query, self.process_batch, self.history_init, self.final_commit)
        con.close()
        

def mainrun():
    obj =  UMIDQuery()
    obj.run()
    return

mainrun()

"""
These functions are for the first query
----------------------------------------------------
Details:
- Query finds the number of instances in which the MachineIDs of of the DB were changed.
- Naming conventions for the query functions have not yet been decided
- Also considering using the 'Factory' design pattern to abstract the generation of histories.

Structure:
query: The SQL query this class is operating on

history: A dictionary/defaultdictionary containing useful metadata from previous passes of the data

"""
def UniqueMachineIDQuery():

    """
    Naming conventions still need to be discussed.
    Wrapping them in this class so that the general names query, history_init, process_batch and final_commit
    can be reused with still maintaining quality.

    I think this function can be converted into a class to incorporate more features in the future-
    Possible future structure of the class-
    - field: query
    - field: results_file
    - field: anomaly_name
    - func : history_init -> defaultdict
    - func : sub_process (a sub_process can be called in a generalized way within
    a generic process_batch function to perform processing for multiple queries simultaneously)
    - func : output_summary (renamed version of final_commit)

    """

    query = "SELECT * from {0} ORDER BY site_id, ip_addr,time_stamp;"
    resultsfile = "results.txt"
    statsfile = "stats.txt"
    watthours_anomaly = "OUTLIER_WATT_HOUR_DECREASE"
    machineswap_anomaly = "OUTLIER_MACHINE_SWAP"


    def history_init():
        history = {}

        """
        history["maxlines"] = 261014681
        Computed via SELECT count(*); on the database
        """

        history["linecount"] =0
        history["count"] = 0
        history["wattanomalies"]= 0
        history["prev"] = -float("inf")
        history["prevc"] = float("inf")

        history["prev_row"] = [None, None, None, None, None, None, None, -float("inf"), float("inf")]

        """
        The 'prev_row' key stores an entire row of values in format
        headings = ['drop_id','line_num','site_id','machine_id','ip_addr','circuit_type','time_stamp','watts','watt_hours','credit']
        """

        """
        Experimental idea - replace the history list with
        a dict for comprehensibility? Might have to be wrapped in a class
        for easy initialization from a row of data
        history["prev_row"] = collections.defaultdict(int)
        history["watt_hours"] = -float("inf")
        history["credit"] = float("inf")
        """

        history["thousand"] = ['Initialized']
        history["dic"] = collections.defaultdict(list)

        """
        Erase the contents of  resultsfile
        """
        open(resultsfile,'w').close()
        return history

    def process_batch(batch,history=None):
        text = ""
        for row in batch:
                #print len(row)
                if len(row)==10:
                    
                    history["linecount"] +=1
                    """
                    Key for the mappings of different parameters in each row of the Database

                    drop id = row[1] ;      site_id = row[2]
                    machine_id = row[3] ;   ip = row[4]
                    circuit_type = row[5];  timestamp = row[6]
                    watts = row[7];         watt_hours = row[8]
                    credit = row[9]
                    """
                    (drop_id, line_num , site_id ,
                         machine_id, ip, circuit_type ,
                             timestamp, watts, watt_hours , credit ) = row

                    if history["linecount"]%1000000==0:

                        print (history["linecount"]/1000000,
                                    "Million lines parsed")
                        print "Passed:", history["linecount"]

                    if watt_hours<history["prev_row"][8]:
                        """
                        Following check to make sure the two roles belong to
                        the same class and that there is no
                        transition.
                        """
                        if (history["prev_row"][3]==machine_id
                                and history["prev_row"][4]==ip
                                    and history["prev_row"][2]==site_id):
                            """
                            -------------------------------------
                                     <  <---------->  >
                            -------------------------------------
                            """

                            history["wattanomalies"]+=1
                            text +=  (site_id + "," + ip + "," + str(timestamp) + ","
                                     + watthours_anomaly + ","
                                     + " decrease=" + str(history["prev_row"][8]-watt_hours) + '\n')



                    history["prev_row"] = row
                    """Indicator boolean - indicates whether machineID has been
                    Seen before for a given site_id/ip pair"""
                    inDic = False

                    for tup in history["dic"][( site_id, ip )]:
                        if machine_id == tup[0]:
                            inDic = True
                            break

                    if not inDic:
                        history["dic"][(site_id,ip)] += [(machine_id,timestamp)]
                        if len(history["dic"][(site_id,ip)])>1:
                            history['count']+=1

                            print "Count:", history["count"], history["linecount"], len(history["dic"])
                            #Picks the last machine from the list stored in history["dic"]

                            to_machine   = history["dic"][(site_id,ip)][-1][0]
                            from_machine = history["dic"][(site_id,ip)][-2][0]

                            text +=  (site_id + "," + ip + "," + str(timestamp) + ","
                                     + machineswap_anomaly + ","
                                     + "from_machine="+ str(from_machine) +" "+
                                     "to_machine="+ str(to_machine) + '\n')


        f = open(resultsfile,'a')
        f.write(text)
        f.close()
        f = open("dict.dat",'w')
        pickle.dump(history["dic"],f)
        f.close()
        return history

    def final_commit(history):
        f = open("stats.txt",'w')
        strbuilder = ""
        strbuilder += "Length of Site_id, IP Pairs:" + str(len(history["dic"])) +'\n'
        pickle.dump(history["dic"],open("dict.dat",'w'))
        strbuilder += "Number of lines parsed: "+ str(history["linecount"]) +'\n'
        strbuilder += "Number of switch anomalies encountered: " + str(history["count"]) + '\n'
        strbuilder += "Number of watt decrease anomalies encountered:"+ str(history["wattanomalies"])+'\n'
        f.write(strbuilder)
        f.close()

    return query, history_init, process_batch, final_commit


"""
Run the code (Only if the file is executed directly)
"""
import argparse
@main
@timed
def mainfn(*args):
    #Defaults -

    tableName = 'raw_circuit_reading'
    testTableName = "raw_circuit_reading_small_test"
    
    """Parsing arguments"""
    
    parser=argparse.ArgumentParser()
    parser.add_argument('-d','--dbname',default="sharedsolar")
    parser.add_argument('-t','--test', default=False ,action="store_true")
    args = parser.parse_args()

    """Establishing a connection to the database"""

    print "DATABASE:",args.dbname
    con = psql.connect(dbname=args.dbname,
                 user="sharedsolar")
    
    """query for the data processing task - should eventually be
    encapsulated in a class along with the processing"""

    #query = "SELECT * from raw_circuit_reading ORDER BY site_id, ip_addr,time_stamp;"

    query, history_init, process_batch, final_commit = UniqueMachineIDQuery()
    if args.test==True:
        query=query.format(testTableName)
    else:
        query=query.format(tableName)

    print query
    
    processOutput(con, query, process_batch, history_init, final_commit)
