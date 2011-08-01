from sqlparse import sql
import db_connection
import myqueryclauses
import  myhelper
import myparser

db = db_connection.Db_connection()

def getResultSetAsList(query):
    if (query is not None):
        results = db.allrows(query)    
        return results
    return None

def checkIfList(ident):
    if (isinstance(ident, sql.Identifier)):#d.name
            return False
    return True

def getGroupbyDistinctList(queryTable,groupbyIdent):
    if (queryTable is not None):
        groupbyattr = [] # contains string group by attribute names
        if (checkIfList(groupbyIdent)):
            for id in groupbyIdent:
                groupbyattr.append(str(id))
        else:
            groupbyattr.append(str(groupbyIdent))
        
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
    
#    print fromIdent
    queryTable =[]
    if ((groupbyIdent and fromIdent) is not None):
        if (checkIfList(groupbyIdent)):
            for gid in groupbyIdent:
                gAlias = gid.get_parent_name()
                query = " SELECT DISTINCT " + str(gid) + " FROM "
                if (checkIfList(fromIdent)): # have a from list and group-by list
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
            if (checkIfList(fromIdent)):
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
    

def findTablenameFromAlias(fromIdent,alias):    
    if (isinstance(fromIdent, sql.Identifier)):#d.name        
        print "I have a from identifier"
        name = fromIdent.get_name()
        alias = fromIdent.get_parent_name()
        print alias
        print name
    else: #d.name, e.id 
        print "I have a from by list"
        for fid in fromIdent:
            fidAlias = fid.get_alias()
            if (alias == fidAlias):
                print fid.get_real_name()
                return fid.get_real_name()
    return False

def constructSubSelects (queryobj, distinctGroupbyValues):
    if (queryobj, distinctGroupbyValues):
        
        print " Constructing sub selects"
        groupbyIdent = queryobj.getGroupbyIdent()    
        fromIdent = queryobj.getFromIdent()
        selectIdent = queryobj.getSelectIdent()
        whereIdent = queryobj.getWhereIdent()
        
        fromClause = ' FROM '
        orgWhereClause = " " + str(whereIdent)
        
        for fid in fromIdent:
            fromClause += str(fid) + " , "
        fromClause = fromClause.rstrip(", ")
        
        groupbyattr = [] # contains string group by attribute names
        if (checkIfList(groupbyIdent)):
            for id in groupbyIdent:
                groupbyattr.append(str(id))
        else:
            groupbyattr.append(str(groupbyIdent))
        numGroupbyAttr = len(groupbyattr) # Number of group by attributes
        
        #Logic: Columns in the SELECT clause which are not in the GROUP BY clause must be part of an AGGREGATE function.
        queryTableMap = {} # to map the query executed and the new table created.
        queryList = {}
        selectList = {}
        fromList = {}
        selectIntoList = {}
        whereList = {}
        
        
        selectIdentWithoutAggregates = myhelper.findSelectClauseWithoutAggregates(selectIdent)
        
#        print distinctGroupbyValues
        
        numRows = myhelper.findGroupbyRows(selectIdentWithoutAggregates,distinctGroupbyValues)
        print "Entering the main loop for numRows: %d"%numRows

        lastAgg = ""
        for sid in selectIdent:
            i=0
            iterations = 0
            if (queryobj.getSelectContainsAggregate()):
                if (not myhelper.isAggregate(sid)): #not an aggregate
                    print "sid: %s - not an aggregate and lastAgg: %s" %(sid,lastAgg)
                    if (lastAgg is ""):
                        while (iterations <numRows): # Max combinations possible.
                            currLength = len(distinctGroupbyValues[str(sid)])
                            numItems = currLength
                            while (numItems > 0):
                                if (iterations in selectList):
                                    selectClause = selectList[iterations]
                                    fromClause = fromList[iterations]
                                    
                                else:
                                    selectClause = " SELECT "
                                    fromClause = " FROM "
                                    selectIntoClause = ""
                                    
                                attr = str(sid)
                                attrValue = str(distinctGroupbyValues[str(sid)][numItems-1])
                
                                selectClause+= "'"+ attrValue + "'::Text" + " AS " + myhelper.remAggregate(attr) + " , "
                                fromClause += myhelper.remAggregate(attr) +"_" + str(attrValue) + "_"
                                selectIntoClause = fromClause.lstrip(" FROM ")
                                
                                selectList[iterations] = selectClause
                                fromList[iterations] = fromClause
                                selectIntoList[iterations] = selectIntoClause
                                
                                numItems-=1
                                iterations+=1  
                    else:
                        print "lastAgg is not quotes"
                        while(iterations < numRows):
                            selectClause = selectList[iterations]
                            selectClause = selectClause + str(sid) + " AS " + lastAgg+ "_"+ myhelper.remAggregate(str(sid))  + " , "
                            selectList[iterations]= selectClause
                            iterations+=1
                    i+=1
                else:
                    lastAgg = str(sid).lower()
                    print "sid: %s - is an aggregate and lastAgg: %s" %(sid,lastAgg)
                    while (iterations <numRows): # Max combinations possible.
                        selectClause= selectList[iterations]
                        selectClause = selectClause + str(sid) 
                        selectList[iterations]= selectClause
                        iterations+=1
            else:
                print "does not aggregate"
                while (iterations <numRows): # Max combinations possible.
                    currLength = len(distinctGroupbyValues[str(sid)])
                    numItems = currLength
                    while (numItems > 0):
                        if (iterations in selectList):
                            selectClause = selectList[iterations]
                            fromClause = fromList[iterations]
                            
                        else:
                            selectClause = " SELECT "
                            fromClause = " FROM "
                            selectIntoClause = ""
                            
                        attr = str(sid)
                        attrValue = str(distinctGroupbyValues[str(sid)][numItems-1])
        
                        selectClause+= "'"+ attrValue + "'::Text" + " AS " + myhelper.remAggregate(attr) + " , "
                        fromClause += myhelper.remAggregate(attr) +"_" + str(attrValue) + "_"
                        selectIntoClause = fromClause.lstrip(" FROM ")
                        
                        selectList[iterations] = selectClause
                        fromList[iterations] = fromClause
                        selectIntoList[iterations] = selectIntoClause
                        
                        numItems-=1
                        iterations+=1  
    #                    print selectClause.rstrip(" , ")
    #                    print fromClause.rstrip("_")    
    #                    print selectIntoClause.rstrip("_")
                        print selectIntoClause   
                i+=1
            
        print "Outof loope"

        for k,v in selectList.iteritems():
            print v
#        tempTable = myhelper.remAggregate(str(attr)) +"_" + str(distinctGroupbyValues[i])
#        selectInto = " INTO " + tempTable
        
        
        print "returning"
        retVal = []
#        retVal.append(queryTableMap)
#        retVal.append(queryList)
#        retVal.append(addBigWhere)      
        return retVal



def constructBigQuery(subSelects):
    if (subSelects):
        queryTableMap = subSelects [0]
        addBigWhere = subSelects [2]
        queryList = subSelects[1]
        bigQuery = ""
        union = " UNION "
        
        for item in queryTableMap.iteritems():
            bigQuery += "SELECT * FROM "+ str(item[0]) + addBigWhere + union
        bigQuery = bigQuery[:-6]
        writeToFile (queryList, bigQuery, queryTableMap)

#Purpose: Write the parameters passed in to the method in to a python script file called scripts.py
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
        for query in queryList:
            query = """db.make_query(""" + triplequote + query + triplequote + """)\n"""
            FILE.write(query)        
        
        FILE.write('\n# The query that combines the results of small queries\n')
        FILE.write('bigQuery = '+triplequote + bigQuery+ triplequote + '\n\n')
        FILE.write('db.make_pquery(bigQuery)\n')
    FILE.close()

if __name__ == "__main__":
    userInput = (" SELECT d.id, MAX (e.salary), AVG(es.skill)"
              " FROM department d, employee_skill es, employee e "
              " WHERE e.dept_id = d.id "
              " GROUP BY d.id ")
    
    (mytok, mytoklen) = myparser.tokenizeUserInput (userInput)
#    displayTokens(mytok,mytoklen)
    queryobj = myparser.myParser(mytok, mytoklen)
#    queryclauses.dispay()
    distinctGroupbyValues = findDistinctGroupbyValues(queryobj)
    
    # dictionary having the temp table as key and the query for that table as value 
    subSelects = constructSubSelects (queryobj, distinctGroupbyValues)
#    print "\n\n\n\n\n\n\n"
#    constructBigQuery(subSelects)
#    