from sqlparse import sql
import db_connection
import myqueryclauses
import  myhelper
import myparser

db = db_connection.Db_connection()

def orderDistinctGroupbyValues(queryobj):
    print "orderDistinctGroupbyValues"


# This function is used exclusively by the findDistinctGroupbyalues
def getGroupbyDistinctList(queryTable,groupbyIdent):
    if (queryTable is not None):
        groupbyattr = [] # contains string group by attribute names
        if (myhelper.checkIfList(groupbyIdent)):
            for id in groupbyIdent:
                groupbyattr.append(str(id))
        else:
            groupbyattr.append(str(groupbyIdent))
        
        #Creating a more handle-able structure to the data
        returnList = {}
        i = 0
        for query in queryTable:
            results = db.allrows(query)    
            vals = []
            for rs in results:
                vals.append(str(rs[0]))
            returnList[groupbyattr[i]] = vals
                
            i+=1
#        print returnList
        return returnList
    return None

def findDistinctGroupbyValues(queryobj):

    groupbyIdent = queryobj.getGroupbyIdent()    
    fromIdent = queryobj.getFromIdent()
    
    queryTable =[]
    if ((groupbyIdent and fromIdent) is not None):
        if (myhelper.checkIfList(groupbyIdent)):
            for gid in groupbyIdent:
                gAlias = gid.get_parent_name()
                query = " SELECT DISTINCT " + str(gid) + " FROM "
                if (myhelper.checkIfList(fromIdent)): # have a from list and group-by list
                    for fid in fromIdent:
#                        print "fid:%s and gAlias:%s" %(fid.get_alias(),gAlias)
                        
                        if (str(fid.get_alias()) == str(gAlias)):
#                            print "got match"
#                            print "fid.get_alias():%s and gAlias(): %s" %(fid.get_alias(),gAlias)
                            if(str(fid) not in query):
                                query += str(fid) + ", "
                    query = query.strip(", ")
    
                else:
                    query += str(fromIdent)
#                print query 
                queryTable.append(query)
        else:#dont have a group-by list"
            gAlias = groupbyIdent.get_parent_name()
            if (myhelper.checkIfList(fromIdent)):
                for fid in fromIdent:
                    if (fid.get_alias() is gAlias):
                        query = " SELECT DISTINCT " + str(groupbyIdent) + " FROM " + str(fid)
            else:
                query = " SELECT DISTINCT " + str(groupbyIdent) + " FROM " + str(fromIdent)
            
            queryTable.append(query)
        resultset= None
        resultset = getGroupbyDistinctList(queryTable,groupbyIdent)
#        print resultset
        return resultset

#def findTablenameFromAlias(fromIdent,alias):    
#    if (isinstance(fromIdent, sql.Identifier)):#d.name        
#        print "I have a from identifier"
#        name = fromIdent.get_name()
#        alias = fromIdent.get_parent_name()
#        print alias
#        print name
#    else: #d.name, e.id 
#        print "I have a from by list"
#        for fid in fromIdent:
#            fidAlias = fid.get_alias()
#            if (alias == fidAlias):
#                print fid.get_real_name()
#                return fid.get_real_name()
#    return False

def findStringFromWhereClauses(queryobj):
    
    fromIdent = queryobj.getFromIdent()
    orgFromClause = ' FROM '
    if(myhelper.checkIfList(fromIdent)):
        for fid in fromIdent:
            orgFromClause += str(fid) + " , "
        orgFromClause = orgFromClause.rstrip(", ")    
    else:
        orgFromClause += str(fromIdent)
    
    orgWhereClause = None
    whereIdent = queryobj.getWhereIdent()
    if whereIdent is not None:
        orgWhereClause = " " + str(whereIdent) # Already has a WHERE keyword in it
    
    return(orgFromClause,orgWhereClause)

def constructSubSelects (queryobj, distinctGroupbyValues):
    if (queryobj, distinctGroupbyValues):
        
        #Temporary data structures required
        queryList = {}
        selectList = {}
        tempTableList = {}
        selectIntoList = {}
        whereList = {}
        addBigWhere = ""
        lastAgg = ""
        bigSelect = ""
        
        #First, check if there are any non-aggregates in the select clause
        selectIdent = queryobj.getSelectIdent()
        selectGroupbyIdent = queryobj.getSelectGroupbyIdent()
        if (selectGroupbyIdent is not None):
            selectIdentWithoutAggregates = myhelper.findSelectClauseWithoutAggregates(selectGroupbyIdent)
            newSelectIdent  = selectGroupbyIdent
            
            #Since we modify the select clause for the sub queries, we have to modify the select clause
            # in the big query as well.
            for id in selectIdent:
                if (myhelper.isAggregate(id)):
                    bigSelect+= myhelper.remAggregate(str(id)) + "_"
                else:
                    bigSelect+= myhelper.remAggregate(str(id)) + ", "
            bigSelect = bigSelect.strip("_")
            bigSelect = bigSelect.strip(", ")        
        
        else:
            # Find all non-aggregate attributes in the select clause
            #Logic: Columns in the SELECT clause which are not in the GROUP BY clause must be part of an AGGREGATE function.
            selectIdentWithoutAggregates = myhelper.findSelectClauseWithoutAggregates(selectIdent)
            newSelectIdent  = selectIdent
        
        (orgFromClause,orgWhereClause) = findStringFromWhereClauses(queryobj)
        
        #Finding the number of rows that the original group by would have.
        numRows = myhelper.findGroupbyRows(selectIdentWithoutAggregates,distinctGroupbyValues)
        
        for sid in newSelectIdent:
            iterations = 0
            containsAggregate = queryobj.getSelectContainsAggregate()
            if (containsAggregate):
                if (not myhelper.isAggregate(sid)): # selectClause has an attribute but sid is not an aggregate
                    if (lastAgg is ""): #The last seen attribute was not an aggregate
                        (selectList,tempTableList,whereList, selectIntoList) = createQueryNotAggregate(iterations,numRows,sid,
                                                                              selectList,tempTableList,whereList,
                                                                               selectIntoList,distinctGroupbyValues,containsAggregate)  
                    else: #lastAgg is not quotes"                
                        while(iterations < numRows):
                            selectClause = selectList[iterations]
                            selectClause = selectClause + str(sid) + " AS " + lastAgg+ "_"+ myhelper.remAggregate(str(sid))  + " , "
                            addBigWhere = "WHERE " +lastAgg+ "_"+ myhelper.remAggregate(str(sid)) + " IS NOT NULL AND "
                            selectList[iterations]= selectClause
                            iterations+=1

                else:#Found an aggregate
                    lastAgg = str(sid).lower()
                    while (iterations <numRows): # Max combinations possible.
                        selectClause= selectList[iterations]
                        selectClause = selectClause + str(sid) 
                        selectList[iterations]= selectClause
                        iterations+=1
            
            else: # selectClause does not have an attribute
                (selectList,tempTableList,whereList, selectIntoList) = createQueryNotAggregate(iterations,numRows,sid,
                                                                      selectList,tempTableList,whereList,
                                                                       selectIntoList,distinctGroupbyValues,containsAggregate)

        
        #Creating the final query from the sub-parts we already have.
        return createReturnValues (numRows, selectList, selectIntoList, whereList, queryList,
                                   tempTableList, orgWhereClause, orgFromClause, addBigWhere, bigSelect.lower())


# This function is used by the constructSubSelects() function to create the  
# clauses for each query when attribute (sid) is not an aggregate
def createQueryNotAggregate(iterations, numRows,sid, selectList, tempTableList, whereList, 
                            selectIntoList, distinctGroupbyValues, containsAggregate):
    
    while (iterations <numRows): # Max combinations possible.
        currLength = len(distinctGroupbyValues[str(sid)])
        numItems = currLength
        while (numItems > 0):
            if (iterations in selectList):
                selectClause = selectList[iterations]
                fromClause = tempTableList[iterations]
                whereClause = whereList[iterations]
            else:
                selectClause = " SELECT "
                fromClause = " FROM "
                selectIntoClause = ""
                whereClause = ""
                            
            attr = str(sid)
            attrValue = str(distinctGroupbyValues[str(sid)][numItems-1])
        
            selectClause+= "'"+ attrValue + "'::Text" + " AS " + myhelper.remAggregate(attr) + " , "
            fromClause += myhelper.remAggregate(attr) +"_" + myhelper.remAggregate(attrValue) + "_"
            selectIntoClause = fromClause.lstrip(" FROM ")
            if (containsAggregate):         
                whereClause = whereClause + " AND " + attr + " = '" + attrValue + "'"
            else:
                whereClause += " AND " + attr + " = '" + attrValue + "'"

            selectList[iterations] = selectClause
            tempTableList[iterations] = fromClause
            selectIntoList[iterations] = " INTO " +  selectIntoClause
            whereList[iterations] = whereClause
                        
            numItems-=1
            iterations+=1  

    return (selectList,tempTableList,whereList, selectIntoList)

# This function is used by the constructSubSelects() function to create the  
# return values for the function constructBigQuery()
def createReturnValues(numRows, selectList, selectIntoList, whereList, queryList, tempTableList,
                       orgWhereClause, orgFromClause, addBigWhere, bigSelect):
    queryTableMap = {} # to map the query executed and the new table created.
    iterations = 0        
    while(iterations <numRows):
        selectList[iterations] = selectList[iterations].rstrip(" , ")            
        selectIntoList[iterations] = selectIntoList[iterations].rstrip(" _ ")
        if orgWhereClause is None:
            whereList[iterations] = whereList[iterations].lstrip(" AND ")
            whereList[iterations] = " WHERE " + whereList[iterations]
        else:
            whereList[iterations] = orgWhereClause + whereList[iterations]
            
        queryList[iterations] = selectList[iterations] + selectIntoList[iterations] + orgFromClause +  whereList[iterations]
                
        tempTableList[iterations] = (tempTableList[iterations].rstrip("_")).lstrip(" FROM ")
        tempTable = tempTableList[iterations]
        subquery = queryList[iterations]
        queryTableMap[tempTable] = subquery
            
        iterations+=1
        
    retVal = []
    retVal.append(queryTableMap)
    retVal.append(queryList)
    retVal.append(addBigWhere.rstrip(" AND "))
    retVal.append (bigSelect)      
    return retVal
        
def constructBigQuery(subSelects):
    if (subSelects):
        queryTableMap = subSelects [0]
        queryList = subSelects[1]
        addBigWhere = subSelects [2]
        bigSelect = subSelects[3]
        
        if (bigSelect is ""):
            selectClause = " SELECT * FROM "
        else:
            selectClause = "SELECT " + bigSelect + " FROM "
        
        bigQuery = ""
        union = " UNION "
        for item in queryTableMap.iteritems():
            if (addBigWhere is not None):
                bigQuery += selectClause + str(item[0]) + " " + addBigWhere + union
            else:
                bigQuery += selectClause + str(item[0]) + " " + union

        bigQuery = bigQuery[:-6]
        writeToFile (queryList, bigQuery, queryTableMap)

#Purpose: Write the parameters passed in to the method in to a python script file called scripts.py
#used by the function constructBigQuery()
def writeToFile (queryList,bigQuery, queryTableMap):
    triplequote = (""" "" """).strip() + (""" " """).strip()

    filename = "scripts.py"
    FILE = open(filename,"w")
    if (not queryList or len (bigQuery) == 0):
        FILE.write('Unknown problem with queries')
    else:   
        FILE.write('import db_connection\n\n')
        FILE.write('db = db_connection.Db_connection()\n\n')
        FILE.write('db.clear_database() # reset the database on each run.\n\n')
        
        # Drop if exists the temp tables we are about to create
        FILE.write('# If the temp tables we are about to create exist, drop them!\n')
        for tempTable,query in queryTableMap.iteritems():
            drop = """db.make_query(""" + triplequote + "drop table if exists " + str(tempTable) + " cascade;" +triplequote + """)\n"""
            FILE.write(drop)        
        
        FILE.write('\n# Make a db call and run the sub queries that will collectively evaluate to the main query result\n')
        for key,query in queryList.iteritems():
            query = """db.make_query(""" + triplequote + str(query) + triplequote + """)\n"""
            FILE.write(query)        
        
        FILE.write('\n# The query that combines the results of small queries\n')
        FILE.write('bigQuery = '+triplequote + bigQuery+ triplequote + '\n\n')
        FILE.write('db.make_pquery(bigQuery)\n')
    FILE.close()

if __name__ == "__main__":
    
    userInput = ("SELECT e.id, e.dept_id, MAX(e.salary) "
                 " FROM employee e, department d "
                 " GROUP BY e.id, e.dept_id "
                 " ORDER BY e.dept_id DESC")
    
    userInput = ("SELECT e.dept_id, MAX(e.salary) "
                 " FROM employee e, department d "
                 " GROUP BY e.dept_id "
                 " ORDER BY e.dept_id DESC")
    
    (mytok, mytoklen) = myparser.tokenizeUserInput (userInput)
#    displayTokens(mytok,mytoklen)
    queryobj = myparser.myParser(mytok, mytoklen)
#    queryclauses.dispay()
    distinctGroupbyValues = findDistinctGroupbyValues(queryobj)
    print distinctGroupbyValues

    # dictionary having the temp table as key and the query for that table as value 
#    subSelects = constructSubSelects (queryobj, distinctGroupbyValues)
###    print "\n\n\n\n\n\n\n"
#    constructBigQuery(subSelects)    