import psycopg2 as psql
import collections
import cPickle as pickle
from pylib import *

#Script to find instances where the machine_id changes

#Establishes connections
con = psql.connect(dbname="sharedsolar",
                 user="sharedsolar")
#Setting up a namedcursor to enable lazy loading

#query for the data processing task
query = "SELECT * from raw_circuit_reading ORDER BY site_id, ip_addr,time_stamp;"

"""
cur = con.cursor("First")
try: 
    cur.execute(query)
    #Now the result is loaded (supposedly)
except Exception as e:
    print e.pgerror
print "Lazy load cursor complete"
"""

def lazyLoad(query=query, con=con):
    #Setting up a namedcursor to enable lazy loading
    cur = con.cursor("First")
    try: 
        cur.execute(query)
        #Now the result is loaded (supposedly)
    except Exception as e:
        print e.pgerror
    print "Lazy loading Cursor initialized"
    return cur

def processOutput(query, process_batch, history_init=None, final_commit=None, batchsize=10000):
    """
    cur is a namedcursor to the output- batchsize is a tuning parameter to determine how many lines
    are to be read from the table in one operation.

    "batchsize" can be used as a tuning parameter - increasing it means storing a larger chunk of the
    data in memory, which in turn means that more 
    """
    cur = lazyLoad(query)
    
    #try:
    if True:
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
"""

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
    Wrapping them in this class so that the general names query, history_init, process_batch and final_commit can be reused
    Maintaining quality
    """
    query = "SELECT * from raw_circuit_reading ORDER BY site_id, ip_addr,time_stamp;"
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
        a dict for comprehensibility
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
                    drop id = row[1] ;      site_id = row[2]
                    machine_id = row[3] ;   ip = row[4]
                    circuit_type = row[5];  timestamp = row[6]
                    watts = row[7];         watt_hours = row[8]
                    credit = row[9]
                    """
                    (drop_id, line_num , site_id ,
                         machine_id, ip, circuit_type , 
                             timestamp, watts, watt_hours , credit ) = row
                    
                    #print "linecount: ", linecount, "site_id", site_id, "machine_id", machine_id, "ip", ip, "watt_hour", watt_hours, "credit", credit
                    if history["linecount"]%1000000==0:
                        
                        print (history["linecount"]/1000000,
                                    "Million lines parsed")
                        print "Passed:", history["linecount"]

                    if watt_hours<history["prev_row"][8]:
                        """
                        Following check to make sure the two roles belong to the same class and that there is no
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
                            #f = open(resultsfile,'a')
                            """
                            text = ("watt_hours anomaly - current: "+str(watt_hours)+" prev-:"
                                    + str(history["prev"])+
                                    ' at line: ' + str(history["linecount"]) +
                                    " Credits prev: "+ str(history["prevc"])+
                                    " curr: "+ str(credit)+ '\n')
                            """
                            text +=  (site_id + "," + ip + "," + str(timestamp) + ","
                                     + watthours_anomaly + ","
                                     + " decrease=" + str(history["prev_row"][8]-watt_hours) + '\n')
                            #f.write(text)
                            #f.close()
                                     
                    history["prev_row"] = row

                    inDic = False
                    
                    for tup in history["dic"][(site_id,ip)]:
                        if machine_id == tup[0]:
                            inDic = True
                            break
                    if inDic:
                        history["dic"][(site_id,ip)] += [(machine_id,timestamp)] 
                        if len(history["dic"][(site_id,ip)]):
                            history['count']+=1
                            print "Count:", history["count"], history["linecount"], len(history["dic"])
                            #f = open(resultsfile,'a')
                            #Picks the last machine from the list stored in history["dic"]
                            to_machine = history["dic"][(site_id,ip)][-1][0]
                            from_machine = history["dic"][(site_id,ip)][-2][0]
                            text +=  (site_id + "," + ip + "," + str(timestamp) + ","
                                     + machineswap_anomaly + ","
                                     + "from_machine="+ str(from_machine) +" "+ "to_machine="+ str(to_machine) + '\n')

                            
        #Batch file_write goes here
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

@main
@timed
def mainfn():
    query, history_init, process_batch, final_commit = UniqueMachineIDQuery()
    processOutput(query, process_batch, history_init, final_commit)

@timed
def test():
    print ("HEllO WORLD")
