import db_connection
import myqueryclauses
import  myhelper
import myparser
import myqueryconstructor
import mycostoptimizer
import myinterrupthandler

import threading
import thread
import time
from sys import stdin
import select
import sys

db = db_connection.Db_connection()

def run_thread (threadname, count, sleeptime):
    print "I am thread"
    for i in range (1, 4 + 1):
        print "count = thread1:%s" % i
        time.sleep(1)
    thread.interrupt_main()
    print "I return from thread"
    return False #as in thread over

def getUserInput():
    query = ""
    entry = raw_input("Enter query, 'done' on its own line to quit: \n")
    while entry.lower() != "done":
        query+=str(entry)
        query+=" "
        entry = raw_input("")
    
    groupby = ""
    entry = raw_input("Enter GROUP BY attributes, 'done' on its own line to quit: \n")
    while entry.lower() != "done":
        groupby+=str(entry)
        groupby+=" "
        entry = raw_input("")
    
    orderby = ""
    entry = raw_input("Enter ORDER BY attributes, 'done' on its own line to quit: \n")
    while entry.lower() != "done":
        orderby+=str(entry)
        orderby+=" "
        entry = raw_input("")
    
    return (query, groupby, orderby)

if __name__ == "__main__":
    
    #Step 1: Get query from user
#    (mainQuery,groupAttr,orderAttr) = getUserInput()
    
    mainQuery = """ SELECT n_name, sum(l_extendedprice * (1 - l_discount))
                    FROM customer, orders, lineitem, supplier, nation, region
                    WHERE c_custkey = o_custkey and l_orderkey = o_orderkey
                        and l_suppkey = s_suppkey and c_nationkey = s_nationkey
                        and s_nationkey = n_nationkey and n_regionkey = r_regionkey
                        and r_name = 'EUROPE' and o_orderdate >= date '1993-01-01'
                        and o_orderdate < date '1993-01-01' + interval '1' year
                   GROUP BY n_name
                   ORDER BY sum(l_extendedprice * (1 - l_discount)) DESC
                   LIMIT ALL; """
    
    groupAttr = " n_name "
    orderAttr = " sum(l_extendedprice * (1 - l_discount)) DESC "
    
    #Step 2: Tokenize the query give by the user
    (queryobj,groupobj,orderobj) = myparser.createUserInputObject(mainQuery, groupAttr, orderAttr)
    
    #Step 3: Display the tokens in the user query
#    queryobj.printClauses()
    
    #Step 4: Generate the select distinct query
    (selectAttr, selectDistinctQuery) = myqueryconstructor.createSelectDistinctQuery(queryobj,groupobj,orderobj)
    
    #Step 5: Send main query and the select distinct query to the cost optimizer
    enableInterrupts = mycostoptimizer.costOptimizer(mainQuery, selectDistinctQuery)
    
    
    if (enableInterrupts):
        print "Interrupts have been enabled for the input query"
#    #Step 4: Find the ordered, distinct group by values for given query plan.
        distinctValues = myqueryconstructor.findDistinctGroupbyValues(selectDistinctQuery)
#        myinterrupthandler.interruptHandler(queryobj, groupobj, orderobj, selectAttr, distinctValues)

        try:
            thread.start_new_thread(myinterrupthandler.interruptHandler,(queryobj, groupobj, orderobj, selectAttr, distinctValues))
#            thread.start_new_thread(run_thread, ("Thread1", 5, 2))
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            print"excepthook"
            sys.excepthook(*sys.exc_info())
            
        running = True
        try:
            while(running):
#                timeout = 5
                (inputready,outputready,exceptready) = select.select([stdin], [], [])
                if (inputready):
                    input = stdin.readline()
                    
                    if (input == "quit\n"):
                        running = False
                        print "You have exited the program...BYE! \n"
                        break
                else:
                    running = False
        except:
            print"Threat 1 exited"
    else:
        print "Executing main query"
        db.make_pquery(mainQuery)
    
#    try:
#        thread.start_new_thread(run_thread, ("Thread1", 5, 1))
#    except (KeyboardInterrupt, SystemExit):
#        raise
#    except:
#        sys.excepthook(*sys.exc_info())
#    
#    running = True
#    try:
#        while(running):
#            timeout = 5
#            (inputready,outputready,exceptready) = select.select([stdin], [], [],timeout)
#            if (inputready):
#                input = stdin.readline()
#                print input
#                if (input == "quit\n"):
#                    running = False
#                    break
#            else:
#                running = False
#    except:
#        print "Thread1 exited...."
#    
    
    
    
    
    
    


#    #Step 5: construct the sub-selects for each distinct value in the group by/order by clauses 
#    (queryList, numRows, addBigWhere) = myqueryconstructor.constructSubSelects (queryobj, groupobj, orderobj, selectAttr, distinctValues)
    
#    #Step 6: Create the script finally!    
#    #In the final query, need only the original select attributes.
#    orgSelect = myqueryconstructor.findStringSelectAttributes(queryobj)
#    orgSelect = myhelper.remAggregate(orgSelect)
#    myqueryconstructor.constructBigQuery(queryList, numRows, orgSelect, addBigWhere)