from sqlparse import sql
import db_connection
import myqueryclauses
import  myhelper
import myparser

db = db_connection.Db_connection()

def getResultSetAsList(query):
    if (query is not None):
        #results = ['COSI','MATH','HIST']
        resultList = []
        results = db.allrows(query)
        for r in results:
            resultList.append(r[0])
        return resultList
    return None

def checkIfList(ident):
    if (isinstance(ident, sql.Identifier)):#d.name
            return False
    return True   

def findDistinctGroupbyValues(queryobj):
    groupbyIdent = queryobj.getGroupbyIdent()    
    fromIdent = queryobj.getFromIdent()
    
    if ((groupbyIdent and fromIdent) is not None):        
        if (checkIfList(groupbyIdent)):
            query = " SELECT DISTINCT " 
            for gid in groupbyIdent:
                gAlias = gid.get_parent_name()
                query += str(gid) + ", "
            query = query.strip(", ")
            query+= " FROM "
            if (checkIfList(fromIdent)): # have a from list and group-by list
                for fid in fromIdent:
                    for gid in groupbyIdent:
                        gAlias = gid.get_parent_name()
                        if (fid.get_alias() is gAlias):
                            query += str(fid) + ", "
                query = query.strip(", ")
            else:
                query += str(fromIdent)
            
        else: #dont have a group-by list"
            gAlias = groupbyIdent.get_parent_name()
            if (checkIfList(fromIdent)):
                for fid in fromIdent:
                    if (fid.get_alias() is gAlias):
                        query = " SELECT DISTINCT " + str(groupbyIdent) + " FROM " + str(fid)
            else:
                query = " SELECT DISTINCT " + str(groupbyIdent) + " FROM " + str(fromIdent)
        
        result = getResultSetAsList(query)
#        print query
#        print result
        return result
    

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
        
        
        #Logic: Columns in the SELECT clause which are not in the GROUP BY clause must be part of an AGGREGATE function.
        
        queryTableMap = {} # to map the query executed and the new table created.
        queryList = []
        distinctValues = len(distinctGroupbyValues)
        i = 0
        while (i < distinctValues):
            selectClause = ' SELECT '
            addBigWhere = " WHERE "
            tempTable =""
            if (queryobj.getSelectContainsAggregate()):
                lastAgg = ""
                for attr in selectIdent:
                    if(not myhelper.isAggregate(attr)):
                        if(lastAgg is not ""):
                            selectClause = selectClause + str(attr) + " AS " + lastAgg+ "_"+ myhelper.remAggregate(str(attr))  + " , "
                            addBigWhere += lastAgg+ "_"+ myhelper.remAggregate(str(attr)) + " IS NOT NULL AND "
                            lastAgg = ""
                        else:
                            selectClause = selectClause + "'"+ str(distinctGroupbyValues[i]) + "'::Text" +  " AS " + myhelper.remAggregate(str(attr))  + " , "
                            whereClause = orgWhereClause + " AND " + str(attr) + " = '"+ str(distinctGroupbyValues[i]) + "'"
                            tempTable = myhelper.remAggregate(str(attr)) +"_" + str(distinctGroupbyValues[i])
                            selectInto = " INTO " + tempTable
                            
                    else:
                        lastAgg = str(attr).lower()
                        selectClause = selectClause + str(attr)

                selectClause = selectClause.rstrip(", ")
                addBigWhere = addBigWhere.rstrip(' AND')
                
                subquery = selectClause + selectInto + fromClause + whereClause
                print "subquery: %s" %subquery
                queryList.append(subquery)
                queryTableMap[tempTable] = subquery
            else:    
                for attr in selectIdent:
                    selectClause = ' SELECT '
                    selectClause+= "'"+ str(distinctGroupbyValues[i]) + "'::Text"  + " AS " + myhelper.remAggregate(str(attr))  + " , "
                
                selectClause = selectClause.rstrip(", ")
                tempTable = myhelper.remAggregate(str(attr)) +"_" + str(distinctGroupbyValues[i])
                selectInto = " INTO " + tempTable
                
                subquery = selectClause + selectInto + fromClause + orgWhereClause
                print "subquery: %s" %subquery
                queryList.append(subquery)
                queryTableMap[tempTable] = subquery
            i+=1
        
        retVal = []
        retVal.append(queryTableMap)
        retVal.append(queryList)
        retVal.append(addBigWhere)      
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
    userInput = (" SELECT d.name, AVG(e.salary)"
              " FROM employee e, department d "
              " WHERE e.dept_id = d.id"
              " GROUP BY d.name")
    
    (mytok, mytoklen) = myparser.tokenizeUserInput (userInput)
#    displayTokens(mytok,mytoklen)
    queryobj = myparser.myParser(mytok, mytoklen)
#    queryclauses.dispay()
    distinctGroupbyValues = findDistinctGroupbyValues(queryobj)
    
    # dictionary having the temp table as key and the query for that table as value 
    subSelects = constructSubSelects (queryobj, distinctGroupbyValues)
    print "\n\n\n\n\n\n\n"
    constructBigQuery(subSelects)
    