import db_connection
import myqueryclauses
import  myhelper
import myparser
import myqueryconstructor


db = db_connection.Db_connection()

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
    
    mainQuery = """ SELECT n_name, sum(l_extendedprice * (1 - l_discount)) as revenue
                    FROM customer, orders, lineitem, supplier, nation, region
                    WHERE c_custkey = o_custkey and l_orderkey = o_orderkey
                        and l_suppkey = s_suppkey and c_nationkey = s_nationkey
                        and s_nationkey = n_nationkey and n_regionkey = r_regionkey
                        and r_name = 'EUROPE' and o_orderdate >= date '1993-01-01'
                        and o_orderdate < date '1993-01-01' + interval '1' year
                   GROUP BY n_name
                   ORDER BY revenue desc
                   LIMIT ALL; """
    
    groupAttr = ""
    orderAttr = ""
    
    #Step 2: Tokenize the query give by the user
    (queryobj,groupobj,orderobj) = myparser.createUserInputObject(mainQuery, groupAttr, orderAttr)
    print "----------------------------Original query input:-------------------------------------------------------"
#    print " %s\n %s %s" %(mainQuery,groupAttr, orderAttr)
#    print "Cost: %s"%db.total_cost(mainQuery)
    print "--------------------------------------------------------------------------------------------------------"    
    
    #Step 3: Display the tokens in the user query
#    queryobj.printClauses()
    

#    
#    #Step 4: Find the ordered, distinct group by values for given query plan.
    (selectAttr, distinctValues) = myqueryconstructor.findDistinctGroupbyValues(queryobj,groupobj,orderobj)   
#    
#    #Step 5: construct the sub-selects for each distinct value in the group by/order by clauses 
#    (queryList, numRows, addBigWhere) = myqueryconstructor.constructSubSelects (queryobj, groupobj, orderobj, selectAttr, distinctValues)
#    
#    #Step 6: Create the script finally!    
#    #In the final query, need only the original select attributes.
#    orgSelect = myqueryconstructor.findStringSelectAttributes(queryobj)
#    orgSelect = myhelper.remAggregate(orgSelect)
#    myqueryconstructor.constructBigQuery(queryList, numRows, orgSelect, addBigWhere)