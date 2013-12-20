import psycopg2 as psql
import collections
import cPickle as pickle

#Script to find instances where the machine_id changes

headings = ['drop_id','line_num','site_id','machine_id','ip_addr','circuit_type','time_stamp','watts','watt_hours','credit']

#Establishes connections
con = psql.connect(dbname="sharedsolar",
                 user="sharedsolar")
#Setting up a namedcursor to enable lazy loading

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
    
def processOutput(cur, tune, process_batch):
    #cur is a namedcursor to the output- tune is a tuning parameter to determine how many lines
    #are to be read from the table in one operation. 
    try:
        batch = cur.fetchmany(tune)
        filterdict = collections.defaultdict()        
        while(batch!=[]):
            #Get a 'tune' number of entries from the DB
            process_batch(batch)
            batch  = cur.fetchmany(1000)

    except Exception as e:
        raise e 

def process_batch(batch):
    return


    
dic = collections.defaultdict(list)

#Iterate over results

maxlines = 261014681 #Computed via SELECT count(*); on the database
linecount =0
count = 0
wattanomalies= 0
prev = -float("inf")
prevc = float("inf")
prev_one = None
thousand = ['Initialized']

print "Iterating..."

try:
    while(thousand!=[]):
        #Get a thousand entries from the DB
        thousand = cur.fetchmany(1000)
        for one in thousand:
            #print len(one)
            if len(one)==10:
                linecount +=1
                site_id = one[2]
                machine_id = one[3]
                ip = one[4]
                timestamp = one[6]
                watt_hours = one[8]
                credit = one[9]
                #print "linecount: ", linecount, "site_id", site_id, "machine_id", machine_id, "ip", ip, "watt_hour", watt_hours, "credit", credit
                if linecount%1000000==0:
                    print linecount/1000000, "Million lines parsed"
                    print "Passed:", linecount
                if watt_hours<prev:
                    if prev_one[3]==one[3] and prev_one[4]==one[4] and prev_one[2]==one[2]:
                        wattanomalies+=1
                        f = open("results.txt",'a')
                        text = "watt_hours anomaly - current: "+str(watt_hours)+" prev-: "+str(prev)+  ' at line: '+str(linecount) + " Credits prev: "+ str(prevc)+" curr: "+ str(credit)+ '\n'
                        f.write(text)
                        f.close()
                #Resetting prev
                prev = watt_hours
                prevc = credit
                prev_one = one
                #print dic[(site_id,ip)]
                #print machine_id 
                #print machine_id in dic[(site_id,ip)]
                inDic = False
                for tup in dic[(site_id,ip)]:
                    if machine_id == tup[0]:
                        inDic = True
                        break
                if not inDic:
                    dic[(site_id,ip)] += [(machine_id,timestamp)] 
                    if len(dic[(site_id,ip)])>1:
                        count+=1
                        print "Count:", count, linecount, len(dic)
                        f = open("results.txt",'a')
                        text = "count: "+ str(count)+'at line: '+str(linecount) + " Unique IDs: "+ str(len(dic))+ '\n'
                        f.write(text)
                        f.close()
                        f = open("dict.dat",'w')
                        pickle.dump(dic,f)
                        f.close()   
            
        f = open("stats.txt",'w')
        str1 = "Length of Site_id, IP Pairs:" + str(len(dic)) +'\n'
        pickle.dump(dic,open("dict.dat",'w'))
        str2 = "Number of lines parsed: "+ str(linecount) +'\n'
        str3 = "Number of switch anomalies encountered: " + str(count) + '\n'
        str4 = "Number of watt decrease anomalies encountered:"+ str(wattanomalies)+'\n'
        f.write(str1+str2+str3+str4)
        f.close()
            

except Exception as e:
    print "Exception occurred at line", linecount
    print "Exception details", type(e), e
    f = open("stats.txt",'w')
    str1 = "Length of Site_id, IP Pairs:" + str(len(dic)) +'\n'
    pickle.dump(dic,open("dict.dat",'w'))
    str2 = "Number of lines parsed: "+ str(linecount) +'\n'
    str3 = "Number of switch anomalies encountered: " + str(count) + '\n'
    str4 = "Number of watt decrease anomalies encountered:"+ str(wattanomalies)+'\n'
    str4 += "Excepted output"
    f.write(str1+str2+str3+str4)
    f.close()
