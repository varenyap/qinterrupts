#from sqlparse import sql
import db_connection
import myqueryclauses
import  myhelper
import myparser

db = db_connection.Db_connection()

#Returns the Attributes as a string. No clause keywords included
def findStringOrderbyAttributes(queryobj):
    orderbyAttr = ""
    orderbyIdent = queryobj.getOrderbyIdent()
    if (orderbyIdent is  not None):
        for oid in orderbyIdent:
#            print oid
            if (myhelper.isAggregate(oid)):
#                print"agg"
                orderbyAttr+= str(oid)
            elif (myhelper.isOrderbyOperator(oid)):
#                print "ops"
                orderbyAttr = orderbyAttr.strip(", ")
                orderbyAttr+= " " + myhelper.remAggregate(str(oid)) + ", "
            elif(myhelper.isMathOperator(str(oid))):
#                print "myops %s" %oid
                orderbyAttr+=str(oid)
            else:
#                print "else"
                orderbyAttr+= str(oid) + ", "

    orderbyAttr = orderbyAttr.strip("_")
    orderbyAttr = orderbyAttr.strip(", ")
    
    print "orderbyAttr: %s" %orderbyAttr
    return orderbyAttr

#Returns the Attributes as a string. No clause keywords included
def findStringFromAttributes(queryobj):
    fromAttr = ""
    fromIdent = queryobj.getFromIdent()
    if (fromIdent is  not None):
        if (myhelper.checkIfList(fromIdent)):
            for fid in fromIdent:
                fromAttr+= str(fid) + ", "
        else:
            fromAttr+= str(fromIdent)
    
    fromAttr = fromAttr.strip(", ")
    
    return fromAttr

#Returns the Attributes as a string. No clause keywords included
def findStringSelectAttributes(queryobj):
    selectAttr = ""
    selectIdent = queryobj.getSelectIdent()
    if (selectIdent is  not None):
        if (myhelper.checkIfList(selectIdent)):
            for sid in selectIdent:
                if (myhelper.isAggregate(sid)):
                    selectAttr+=str(sid)
                else:
                    selectAttr+= str(sid) + ", "
        else:
            selectAttr+= str(selectIdent)
    selectAttr = selectAttr.strip(", ")
    return selectAttr

#Returns the Attributes as a string. No clause keywords included
def findStringGroupbyAttributes(queryobj):
    groupbyAttr = ""
    groupbyIdent = queryobj.getGroupbyIdent()
    if (groupbyIdent is  not None):
        if (myhelper.checkIfList(groupbyIdent)):
            for gid in groupbyIdent:
                groupbyAttr+= str(gid) + ", "
        else:
            groupbyAttr+= str(groupbyIdent)
    groupbyAttr = groupbyAttr.strip(", ")
    return groupbyAttr

def findOrderbyAlias(orderobj):
    orderbyString = findStringOrderbyAttributes(orderobj)
    orderbyList = orderbyString.split(",")
    orderbyAliasList = {}
    
    for item in orderbyList:
        idx = item.find("AS")
        attr = item
        if (idx is not -1):
            attr = item[:idx]
            alias = item[idx+2:]
            orderbyAliasList[attr] = alias
        else:
            orderbyAliasList[attr] = ""
    
#    print orderbyAliasList
    return orderbyAliasList

def findFromAlias(queryobj):
    #Find unique tables in the original from query.
    
    fromIdent = queryobj.getFromIdent()
    print fromIdent
    tableAliasList = {}
    for table in fromIdent:
        tableAliasList[table.get_real_name()] = table.get_alias() 
        
    return tableAliasList

# This function is used exclusively by the findDistinctGroupbyalues
def getGroupbyDistinctList(query, dropQuery):
    
    if (query and dropQuery):
        db.make_query(dropQuery)
        print query
        db.make_query(query)
        
        results = db.allrows("select * from tempDistinctAttributeValues")
        
        displayGroupbyValues(query, results)
        return results
    
    print " Error in executing distinct groupby queries\n"   
    return None

def displayGroupbyValues(query, results):
    print "\n---------------------Displaying distinct GroupbyValues:--------------------------------"
    print query
    for rs in results:
        print rs
    print "----------------------------------------------------------------------------------------"

#Creates the queries required to execute against the database
def findDistinctGroupbyValues(queryobj,groupobj,orderobj):
        
    selectClause = " SELECT DISTINCT "
    selectClause+= findStringGroupbyAttributes(groupobj) 
    selectClause+= ", "
    
    orderbyString = findStringOrderbyAttributes(orderobj)
    orderbyList = orderbyString.split(",")
    orderbyClause = " ORDER BY " #+ orderbyString
    
    for item in orderbyList:
        orderbyClause+= item + ","
        item = item.replace("ASC","")
        item = item.replace("DESC","")
        item= item.strip()
        
        if (item not in selectClause):
            selectClause+= item + ", "
            idx = item.find(" ")
            if (idx is not -1):
                item = item[:idx]
    
    selectClause = selectClause.replace("ASC","")
    selectClause = selectClause.replace("DESC", "")
    orderbyClause = orderbyClause.rstrip(", ")
    
    #Now add in all the attributes that are in the original select like aggregates
    orgSelect = findStringSelectAttributes(queryobj)
    orgSelectList = orgSelect.split(",")
    for item in orgSelectList:
        if item not in selectClause:
            selectClause+=item + ", "
    
    selectClause = selectClause.strip(", ")
    
    selectIntoClause = " INTO tempDistinctAttributeValues "
    groupbyClause = " GROUP BY " + findStringGroupbyAttributes(groupobj) + " "
    
    #Find unique tables in the original from query.
    orgFromAttr = findStringFromAttributes(queryobj)
#    orgFromAttrList = orgFromAttr.split(",")
    
#    uniqueTables = ""
#    for item in orgFromAttrList:
#        idx = item.find(" ")
#        if (idx is not -1):
#            item = item[:idx]
#            if (item.strip() not in uniqueTables):
#                uniqueTables+= item + ", "
#        else:
#            uniqueTables
    
    fromClause = " FROM " + orgFromAttr
    query = selectClause + selectIntoClause + fromClause + groupbyClause + orderbyClause
    print groupbyClause +  orderbyClause
    dropQuery = "drop table if exists tempDistinctAttributeValues;"
    
    
    distinctValues = getGroupbyDistinctList(query, dropQuery)
    # Contains only the attribute names/no keywords like DESC, yes to aggregates
    selectAttr = selectClause.lstrip(" SELECT DISTINCT ") 
    return (selectAttr, distinctValues)

def constructSubSelects (queryobj, groupobj, orderobj, selectAttr, distinctValues):
    if (queryobj and selectAttr and distinctValues):
        print"constructSubSelects"
        print selectAttr
        #Temporary data structures required
        queryList = {}
        selectList = {}
        tempTableList = {}
        selectIntoList = {}
        whereList = {}
        addBigWhere = ""
        lastAgg = ""
        bigSelect = ""
        insertClause = " INSERT INTO finalOutputTable (" + selectAttr + ")"        
        
        selectAttrList = selectAttr.split(",")#contains list of attributes in the select clause of sub-queries
        containsAggregate =  queryobj.getSelectContainsAggregate()
        orgWhere = queryobj.getWhereIdent()
#        if (orgWhere is None): # the original query has no where clause
#            orgWhere = ""
        numAttr = len(selectAttrList)
        numRows = len(distinctValues)
        fromClause = " FROM " + findStringFromAttributes(queryobj)
        
        for attr in selectAttrList:
            if("(" in attr):
                addBigWhere+= myhelper.remAggregate(attr) + " IS NOT NULL AND "
                insertClause+= myhelper.remAggregate(attr)
            else:
                insertClause+=str(attr) + ", "
        insertClause = insertClause.rstrip(", ")
        insertClause+=")"
        addBigWhere = addBigWhere.rstrip(" AND ")
        
        addBigWhere = " WHERE " + addBigWhere
        
        #SELECT dept_id, locale INTO temp1 FROM company WHERE cmp_id = 2 AND dept_id = 1 AND locale = 'Boston';
        #    SELECT '4'::Text as dept_id, 'Columbus'::Text as locale, MAX(salary) INTO temp0 FROM company 
        #    WHERE cmp_id = 2 AND dept_id = '4' AND locale = 'Columbus';
        
        iterations = 0
        for tuple in distinctValues:
            selectList[iterations] = " SELECT "
            if(orgWhere is None):
                whereList[iterations] = " WHERE "
            else:
                whereList [iterations] = " " + str(orgWhere) + " AND "
            num = 0
            while (num < numAttr):
                attrName = str(selectAttrList[num])
                if ("(" not in attrName):
                    attrValue = str(tuple[num])
                
                if (containsAggregate):
                    if ("(" not in attrName):
                        selectList[iterations]+= "'" + attrValue + "'::Text AS " + attrName + ", "
                        whereList[iterations]+= attrName + " = '" + attrValue + "' AND " 
                    else:
                        
                        selectList[iterations]+= attrName + " AS " + myhelper.remAggregate(attrName) + ", "
                else:#select contains no aggregate
                    selectList[iterations]+= attrName + ", "
                    if("(" not in attrName):
                        whereList[iterations]+= attrName + " = '" + attrValue + "' AND "
                num+=1

            selectList[iterations] = selectList[iterations].rstrip(", ")
            whereList[iterations] = whereList[iterations].rstrip(" AND ")
            selectIntoList[iterations] = " INTO temp"+ str(iterations) + " "
        
            queryList[iterations] = selectList[iterations] + selectIntoList[iterations] + fromClause + whereList[iterations]
#            print queryList[iterations]     
            iterations+=1
        return (queryList, numRows, addBigWhere)
    print "Error in creating subqueries"
    return None


def constructBigQuery(queryList, numRows, selectAttr,addBigWhere):
    if(queryList and numRows):
        print "constructbigquery"
        writeToFile (queryList, numRows, selectAttr, addBigWhere)
        
#Purpose: Write the parameters passed in to the method in to a python script file called scripts.py
#used by the function constructBigQuery()
def writeToFile (queryList, numRows, selectAttr, addBigWhere):
    triplequote = (""" "" """).strip() + (""" " """).strip()
    
    if addBigWhere == " WHERE ":
        addBigWhere = ""

    filename = "scripts.py"
    FILE = open(filename,"w")
    if (not queryList or numRows == 0):
        FILE.write('Unknown problem with queries')
    else:   
        FILE.write('import db_connection\n\n')
        FILE.write('db = db_connection.Db_connection()\n\n')
        FILE.write('db.clear_database() # reset the database on each run.\n\n')
        
        # Drop if exists the temp tables we are about to create
        FILE.write('# If the temp tables we are about to create exist, drop them!\n')
        FILE.write("db.make_query(" + triplequote + "drop table if exists finalOutputTable cascade" + triplequote + ")\n")
        iterations = 0
        while (iterations < numRows):
            drop = "db.make_query(" + triplequote + "drop table if exists temp" + str(iterations) + " cascade;" +triplequote + """)\n"""
            FILE.write(drop)
            iterations+=1        
        
        FILE.write('\n\n# Make a db call and run the sub queries that will collectively evaluate to the main query result\n')
        iterations = 0
        while(iterations <numRows):
            query = """db.make_query(""" + triplequote + queryList[iterations] + triplequote + """)\n"""
            FILE.write(query)
            iterations+=1
        
        FILE.write("\n#To find the union of the result sets\n")
        #first iterations is a SELECT INTO to create the table. subsequent are INSERT's
        iterations = 0
        while(iterations<numRows):
            if(iterations == 0):
                query = "db.make_query(" + triplequote + " SELECT * INTO finaloutputTable FROM temp" + str(iterations) + addBigWhere + triplequote + ")\n"
            else:
                query = "db.make_query(" + triplequote + " INSERT INTO finaloutputTable  SELECT * FROM temp" + str(iterations) + addBigWhere + triplequote + ")\n"
            FILE.write(query)    
            iterations+=1
        
        FILE.write("\n#Final query\n")
        FILE.write("db.make_pquery(" + triplequote + "SELECT * from finalOutputTable " + triplequote + ")\n")
    FILE.close()
    print "Script created!"


if __name__ == "__main__":
#    query = "SELECT dept_id, locale FROM company WHERE cmp_id = 2 "
#    groupAttr = "dept_id, locale "
#    orderAttr = "dept_id"
#
#    query = "SELECT dept_id, locale, salary FROM company WHERE cmp_id = 2 "
#    groupAttr = "dept_id, locale, salary "
#    orderAttr = " dept_id DESC, locale"
#    
#    #query = " SELECT q1.sym, q1.day, q1.price - q2.price FROM quotes as q1, quotes as q2 WHERE q1.sym = q2.sym and q1.day = q2.day -1 "
#    #groupAttr = " q1.sym "
#    #orderAttr = " MAX(q1.price) - MIN(q1.price) AS maxJump"
#    
#    query = " SELECT dept_id, MAX(salary) FROM company GROUP BY dept_id " 
#    groupAttr = " dept_id "
#    orderAttr = " dept_id "
    
    query = " SELECT dept_id, MAX(salary) FROM company GROUP BY dept_id " 
    groupAttr = " dept_id "
    orderAttr = " MAX(salary) "

    #doesnt work    
    query = " SELECT dept_id FROM company GROUP BY dept_id " 
    groupAttr = " dept_id "
    orderAttr = " MAX(salary) "


    (queryobj,groupobj,orderobj) = myparser.createUserInputObject(query, groupAttr, orderAttr)
    print "----------------------------Original query input:-------------------------------------------------------"
    print " %s\n %s\n %s" %(query,groupAttr, orderAttr)
    print "--------------------------------------------------------------------------------------------------------"
    

    (selectAttr, distinctValues) = findDistinctGroupbyValues(queryobj,groupobj,orderobj)    
    
    (queryList, numRows, addBigWhere) = constructSubSelects (queryobj, groupobj, orderobj, selectAttr, distinctValues)
    constructBigQuery(queryList, numRows, selectAttr, addBigWhere)

