import psycopg2 as psql
import collections
import cPickle as pickle

#Script to find instances where the machine_id changes

headings = ['drop_id','line_num','site_id','machine_id','ip_addr','circuit_type','time_stamp','watts','watt_hours','credit']

#Establishes connections
con = psql.connect( dbname="sharedsolar"
                    , user="sharedsolar"  )

#Setting up a namedcursor to enable lazy loading

#query for the data processing task
query = "SELECT * from raw_circuit_reading ORDER BY site_id, ip_addr,time_stamp;"

cur = con.cursor("First")
try: 
    cur.execute(query)
    #Now the result is loaded (supposedly)
except Exception as e:
    print e.pgerror
print "Lazy load cursor complete"


def lazyLoad(query=query, con=con):
    #Setting up a namedcursor to enable lazy loading
    cur = con.cursor("First")
    try: 
        cur.execute(query)
        #Now the result is loaded (supposedly)
    except Exception as e:
        print e.pgerror
    print "Lazy load complete"
    return cur

def processOutput(cur, tune, process_batch, history_init=None, final_commit=None):
    #cur is a namedcursor to the output- tune is a tuning parameter to determine how many lines
    #are to be read from the table in one operation. 
    try:
        batch = cur.fetchmany(tune)
        history = history_init()
        while(batch!=[]):
            #Get a 'tune' number of entries from the DB
            history = process_batch(batch,history)
            batch  = cur.fetchmany(tune)
        """
        Execution of query traversal complete
        Now executing final commit
        """
        if (final_commit!=None):
            final_commit(history)

    except Exception as e:
        raise e 

def history_init():
    history = {}
    history["maxlines"] = 261014681
    #Computed via SELECT count(*); on the database
    history["linecount"] =0
    history["count"] = 0
    history["wattanomalies"]= 0
    history["prev"] = -float("inf")
    history["prevc"] = float("inf")
    history["prev_one"] = None
    history["thousand"] = ['Initialized']
    history["dic"] = collections.defaultdict(list)
    return history
    
def process_batch(batch,history=None):
    for one in batch:
            #print len(one)
            if len(one)==10:
                history["linecount"] +=1
                site_id = one[2]
                machine_id = one[3]
                ip = one[4]
                timestamp = one[6]
                watt_hours = one[8]
                credit = one[9]
                #print "linecount: ", linecount, "site_id", site_id, "machine_id", machine_id, "ip", ip, "watt_hour", watt_hours, "credit", credit
                if history["linecount"]%1000000==0:
                    print history["linecount"]/1000000, "Million lines parsed"
                    print "Passed:", history["linecount"]
                if watt_hours<prev:
                    if history["prev_one"][3]==one[3] and history["prev_one"][4]==one[4] and history["prev_one"][2]==one[2]:
                        history["wattanomalies"]+=1
                        f = open("results.txt",'a')
                        text = "watt_hours anomaly - current: "+str(watt_hours)+" prev-: "+str(history["prev"])+  ' at line: '+str(history["linecount"]) + " Credits prev: "+ str(history["prevc"])+" curr: "+ str(credit)+ '\n'
                        f.write(text)
                        f.close()
                #Resetting prev
                history["prev"] = watt_hours
                history["prevc"] = credit
                history["prev_one"] = one
                #print dic[(site_id,ip)]
                #print machine_id 
                #print machine_id in dic[(site_id,ip)]
                inDic = False
                for tup in history["dic"][(site_id,ip)]:
                    if machine_id == tup[0]:
                        inDic = True
                        break
                if not inDic:
                    history["dic"][(site_id,ip)] += [(machine_id,timestamp)] 
                    if len(history["dic"][(site_id,ip)])>1:
                        count+=1
                        print "Count:", history["count"], history["linecount"], len(history["dic"])
                        f = open("results.txt",'a')
                        text = "count: "+ str(history["count"])+'at line: '+str(history["linecount"]) + " Unique IDs: "+ str(len(history["dic"]))+ '\n'
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

"""
Run the code
"""
print "Running test on query", query
processOutput(cur, tune, process_batch, history_init, final_commit)