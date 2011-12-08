
#Step 2: The optimizer determines if cost of main query is greater than cost of
#        finding ordered, distinct group by values.
#
#Step 2a:  If cost of executing main query is cheaper, then query is executed against database 
#          and the results are returned to the user.
#
#Step2b: If the main query is more expensive, then the sub queries are generated
#        according to the distinct, ordered group by values.
#
#Step 2c: If the total cost of finding the distinct values + the cost of executing the sub-queries is 
#         significantly greater than the cost of executing the main query, then again,
#         the main query is executed against the database, and results are returned to the user.

import db_connection
import myqueryclauses
import  myhelper
import myparser
import myqueryconstructor

db = db_connection.Db_connection()

def costOptimizer(mainQuery, selectDistinctQuery):
    
    overheadFactor = 30
    
    print "----------------------------Original query input:-------------------------------------------------------"
    print "%s\n" %(mainQuery)
    mainQueryCost = int(db.total_cost(mainQuery)) * overheadFactor
    print "Cost: %s"%mainQueryCost
#    print "--------------------------------------------------------------------------------------------------------"
    
    print "----------------------------Cost of finding distinct GROUP BY values:------------------------------------"
    print "%s\n" %(selectDistinctQuery)
    selectDistinctQuery = """ SELECT DISTINCT n_name, SUM(l_extendedprice * (1 - l_discount))
                              INTO tempDistinctAttributeValues
                              FROM lineitem, nation
                              GROUP BY n_name
                              ORDER BY SUM(l_extendedprice * (1 - l_discount)) DESC"""
                            
    selectDistinctQueryCost = int(db.total_cost(selectDistinctQuery)) 
    print "Cost: %s"%selectDistinctQueryCost
    print "--------------------------------------------------------------------------------------------------------"
    
#    print "----------------------------------Query Analysis:-------------------------------------------------------"
    if (mainQueryCost > selectDistinctQueryCost):
        print "Your query will benefit by enabling interrupts"
        enableInterrupts = True
    else:
        print "Your query will return faster without enabling interrupts"
        enableInterrupts = False
    print "--------------------------------------------------------------------------------------------------------"
    return enableInterrupts

