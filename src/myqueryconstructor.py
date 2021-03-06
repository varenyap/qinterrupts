#from sqlparse import sql
import myparser
#import myqueryclauses
import db_connection
import  myhelper
import writetofile
db = db_connection.Db_connection()

#===============================================================================
# Returns the attributes of the order by clause as a string minus the 'ORDER BY'
# keyword
#===============================================================================
def findStringOrderbyAttributes(queryobj):
    orderbyAttr = ""
    if(queryobj is not None):
        orderbyIdent = queryobj.getOrderbyIdent()
        if (orderbyIdent is  not None):
            for oid in orderbyIdent:
#                print oid
                if (myhelper.isAggregate(oid)):
#                    print"agg"
                    orderbyAttr+= str(oid)
                elif (myhelper.isOrderbyOperator(oid)):
#                    print "ops"
                    orderbyAttr = orderbyAttr.strip(", ")
                    orderbyAttr+= " " + myhelper.remAggregate(str(oid)) + ", "
                elif(myhelper.isMathOperator(str(oid))):
#                    print "myops %s" %oid
                    orderbyAttr+=str(oid)
                else:
#                    print "else"
                    orderbyAttr+= str(oid) + ", "
    
        orderbyAttr = orderbyAttr.strip("_")
        orderbyAttr = orderbyAttr.strip(", ")
    
#    print "orderbyAttr: %s" %orderbyAttr
    return orderbyAttr

#===============================================================================
# Returns the attributes of the from clause as a string minus the 'FROM'
# keyword
#===============================================================================
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

#===============================================================================
# Returns the attributes of the select clause as a string minus the 'SELECT'
# keyword
#===============================================================================
def findStringSelectAttributes(queryobj):
    selectAttr = ""
    selectIdent = queryobj.getSelectIdent()
    
    if (selectIdent is  not None):
        if (myhelper.checkIfList(selectIdent)):
            for sid in selectIdent:
                if (myhelper.isAggregate(sid)):
                    selectAttr+=str(sid)
                    selectAttr= selectAttr.strip()
                elif(myhelper.isMathOperator(sid)):
                    selectAttr = selectAttr.rstrip(", ")
                    selectAttr+=str(sid)
                    selectAttr= selectAttr.strip()
                else:
                    selectAttr+= str(sid) + ", "
                    selectAttr= selectAttr.strip()
        else:
            selectAttr+= str(selectIdent)
            selectAttr= selectAttr.strip()
    selectAttr = selectAttr.strip(", ")
    
    
    return selectAttr

#===============================================================================
# Returns the attributes of the group by clause as a string minus the 'GROUP BY'
# keyword
#===============================================================================
def findStringGroupbyAttributes(queryobj):
    groupbyAttr = ""
    if (queryobj is not None):
        groupbyIdent = queryobj.getGroupbyIdent()
        if (groupbyIdent is not None):
            if (myhelper.checkIfList(groupbyIdent)):
                for gid in groupbyIdent:
                    groupbyAttr+= str(gid) + ", "
            else:
                groupbyAttr+= str(groupbyIdent)
        groupbyAttr = groupbyAttr.strip(", ")
    return groupbyAttr

#def findOrderbyAlias(orderobj):
#    orderbyString = findStringOrderbyAttributes(orderobj)
#    orderbyList = orderbyString.split(",")
#    orderbyAliasList = {}
#    
#    for item in orderbyList:
#        idx = item.find("AS")
#        attr = item
#        if (idx is not -1):
#            attr = item[:idx]
#            alias = item[idx+2:]
#            orderbyAliasList[attr] = alias
#        else:
#            orderbyAliasList[attr] = ""
#    
##    print orderbyAliasList
#    return orderbyAliasList

#def findFromAlias(queryobj):
#    #Find unique tables in the original from query.
#    fromIdent = queryobj.getFromIdent()
#    print fromIdent
#    tableAliasList = {}
#    for table in fromIdent:
#        tableAliasList[table.get_real_name()] = table.get_alias() 
#        
#    return tableAliasList

# This function is used exclusively by the findDistinctGroupbyalues
def getGroupbyDistinctList(query, dropQuery):
    
    if (query and dropQuery):
#        print "Resetting the database: "
#        db.clear_database() # reset the database on each run.
#        print "finished resetting"
#        db.make_query(dropQuery)
#        print query
        
#        db.make_query(query)
        
#        results = db.allrows("select * from tempDistinctAttributeValues")
        
        results = db.allrows("select * from tempDistinctAttributeValues5")
        return results
    
    print " Error in executing distinct groupby queries\n"   
    return None

# Creates the SELECT DISTINCT query to find ordered group by attribute values
def createSelectDistinctQuery(queryobj,groupobj,orderobj):
    selectClause = " SELECT DISTINCT "
    selectClause+= findStringGroupbyAttributes(groupobj)
    
    #because it is possible that there were no attributes in the additional group by clause 
    if (selectClause is not " SELECT DISTINCT "):  
        selectClause+= ", "
    
    #First, add attributes from the additional orderby and then add from the mainQuery
    orderbyString = findStringOrderbyAttributes(orderobj)
    orderbyList = orderbyString.split(",")
    orderbyClause = " ORDER BY " #+ orderbyString
    
    for item in orderbyList:
        if (myhelper.hasSelectOperator(item)):
            orderbyClause = orderbyClause.rstrip(", ")
        if ("(" in item):
            orderbyClause = orderbyClause.rstrip(", ")  
        orderbyClause+= " " + item + ", "
        item = item.replace("ASC","")
        item = item.replace("DESC","")
        item= item.rstrip()
        
        if (item not in selectClause):
#            if (myhelper.hasSelectOperator(item)):
#                selectClause = selectClause.rstrip(", ")

            selectClause+= item + ", "
            idx = item.find(" ")
            if (idx is not -1):
                item = item[:idx]
    
    
    selectClause = selectClause.replace("ASC","")
    selectClause = selectClause.replace("DESC", "")
    #selectClause = selectClause.rstrip(", ")
    orderbyClause = orderbyClause.rstrip(", ")
    
    #Now add in all the attributes that are in the original select -> like aggregates
    orgSelect = findStringSelectAttributes(queryobj)

    orgSelectList = orgSelect.split(",")
    for item in orgSelectList:
        
        if item not in selectClause:
            selectClause+=item + ", "
    
    selectClause = selectClause.rstrip(", ")
    selectIntoClause = " INTO tempDistinctAttributeValues "
    
    #Now, add in all the group by attributes in the main query with that of the user entered groupby
    orgGroupby = findStringGroupbyAttributes(queryobj)
    groupbyClause = " GROUP BY " + findStringGroupbyAttributes(groupobj)
    
    #because it is possible that there were no attributes in the additional group by clause
    if (groupbyClause is not " GROUP BY "):
        groupbyClause+= ", "
        
    if (orgGroupby is not None):
        orgGroupbyList = orgGroupby.split(",")
        
        for item in orgGroupbyList:
            if (str(item) not in groupbyClause):
                groupbyClause+= str(item) + ", "
    
    groupbyClause = groupbyClause.rstrip(", ")    
    
    #Find unique tables in the original from query.
    orgFromAttr = findStringFromAttributes(queryobj)
    
    fromClause = " FROM " + orgFromAttr
    query = selectClause + selectIntoClause + fromClause + groupbyClause + orderbyClause
        
    # Contains only the attribute names/no keywords like DESC, yes to aggregates
    selectAttr = selectClause.lstrip(" SELECT DISTINCT ") 
    return (selectAttr, query)
    
def findDistinctGroupbyValues(query):
    if (query):
        dropQuery = "drop table if exists tempDistinctAttributeValues;"
        #Now, get all the distinct values
        distinctValues = getGroupbyDistinctList(query, dropQuery)
        return distinctValues
    
    print "Error in finding the distinct ordered values"
    

#def findDistinctGroupbyValues(queryobj,groupobj,orderobj):
#    selectClause = " SELECT DISTINCT "
#    selectClause+= findStringGroupbyAttributes(groupobj)
#    
#    #because it is possible that there were no attributes in the additional group by clause 
#    if (selectClause is not " SELECT DISTINCT "):  
#        selectClause+= ", "
#    
#    #First, add attributes from the additional orderby and then add from the mainQuery
#    orderbyString = findStringOrderbyAttributes(orderobj)
#    orderbyList = orderbyString.split(",")
#    orderbyClause = " ORDER BY " #+ orderbyString
#    
#    for item in orderbyList:
#        if (myhelper.hasSelectOperator(item)):
#            orderbyClause = orderbyClause.rstrip(", ")
#        if ("(" in item):
#            orderbyClause = orderbyClause.rstrip(", ")  
#        orderbyClause+= " " + item + ", "
#        item = item.replace("ASC","")
#        item = item.replace("DESC","")
#        item= item.rstrip()
#        
#        if (item not in selectClause):
##            if (myhelper.hasSelectOperator(item)):
##                selectClause = selectClause.rstrip(", ")
#
#            selectClause+= item + ", "
#            idx = item.find(" ")
#            if (idx is not -1):
#                item = item[:idx]
#    
#    
#    selectClause = selectClause.replace("ASC","")
#    selectClause = selectClause.replace("DESC", "")
#    #selectClause = selectClause.rstrip(", ")
#    orderbyClause = orderbyClause.rstrip(", ")
#    
#    #Now add in all the attributes that are in the original select -> like aggregates
#    orgSelect = findStringSelectAttributes(queryobj)
#
#    orgSelectList = orgSelect.split(",")
#    for item in orgSelectList:
#        
#        if item not in selectClause:
#            selectClause+=item + ", "
#    
#    selectClause = selectClause.rstrip(", ")
#    selectIntoClause = " INTO tempDistinctAttributeValues "
#    
#    #Now, add in all the group by attributes in the main query with that of the user entered groupby
#    orgGroupby = findStringGroupbyAttributes(queryobj)
#    groupbyClause = " GROUP BY " + findStringGroupbyAttributes(groupobj)
#    
#    #because it is possible that there were no attributes in the additional group by clause
#    if (groupbyClause is not " GROUP BY "):
#        groupbyClause+= ", "
#        
#    if (orgGroupby is not None):
#        orgGroupbyList = orgGroupby.split(",")
#        
#        for item in orgGroupbyList:
#            if (str(item) not in groupbyClause):
#                groupbyClause+= str(item) + ", "
#    
#    groupbyClause = groupbyClause.rstrip(", ")    
#    
#    #Find unique tables in the original from query.
#    orgFromAttr = findStringFromAttributes(queryobj)
#    
#    fromClause = " FROM " + orgFromAttr
#    query = selectClause + selectIntoClause + fromClause + groupbyClause + orderbyClause
##    print groupbyClause +  orderbyClause
#    dropQuery = "drop table if exists tempDistinctAttributeValues;"
#    
#    #Now, get all the distinct values
##    print query
#    distinctValues = getGroupbyDistinctList(query, dropQuery)
#    
#    # Contains only the attribute names/no keywords like DESC, yes to aggregates
#    selectAttr = selectClause.lstrip(" SELECT DISTINCT ") 
#    return (selectAttr, distinctValues)

def constructSubSelects (queryobj, groupobj, orderobj, selectAttr, distinctValues):
    if (queryobj and selectAttr and distinctValues):
        print "I Have valid input for construct subselects"

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
        containsAggregate =  queryobj.getSelectContainsAggregate() or orderobj.getOrderbyContainsAggregate()
        orgWhere = queryobj.getWhereIdent()
#        if (orgWhere is None): # the original query has no where clause
#            orgWhere = ""
        numAttr = len(selectAttrList)
        numRows = len(distinctValues)
        fromClause = " FROM " + findStringFromAttributes(queryobj)
        
        #Tobe used when order by has an aggregate in it
        groupbyClause = " "
        
        for attr in selectAttrList:
            if("(" in attr):
                addBigWhere+= myhelper.remAggregate(attr) + " IS NOT NULL AND "
                insertClause+= myhelper.remAggregate(attr)
            else:
                groupbyClause+= str(attr) + ", "
                insertClause+=str(attr) + ", "
        insertClause = insertClause.rstrip(", ")
        insertClause+=")"
        addBigWhere = addBigWhere.rstrip(" AND ")
        addBigWhere = " WHERE " + addBigWhere
        
        #if the user entered an orderby attribute with an aggregate then you have a group by
        if (orderobj.getOrderbyContainsAggregate()):
            groupbyClause = " GROUP BY " + groupbyClause
            groupbyClause = groupbyClause.rstrip(", ")
        else:
            groupbyClause = " "
        
        
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
                        selectList[iterations]+= "'" + attrValue + "'::Text AS " + myhelper.remAggregate(attrName) + ", "
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
        
            queryList[iterations] = selectList[iterations] + selectIntoList[iterations] + fromClause + whereList[iterations] + groupbyClause
#            print queryList[iterations]     
            iterations+=1
        print "Subselects constructed!"
        return (queryList, numRows, addBigWhere)
    print "Error in creating subqueries"
    return None


def constructBigQuery(queryList, numRows, selectAttr,addBigWhere):
    if(queryList and numRows):
        writetofile.createScript(queryList, numRows, selectAttr, addBigWhere)
        writetofile.createScriptWithCosts(queryList, numRows, selectAttr, addBigWhere)
        

if __name__ == "__main__":

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
    

    #Step 4: Find the ordered, distinct group by values for given query plan.
#    (selectAttr, distinctValues) = findDistinctGroupbyValues(queryobj,groupobj,orderobj)
    
    #Step 5: construct the sub-selects for each distinct value in the group by/order by clauses 
#    (queryList, numRows, addBigWhere) = constructSubSelects (queryobj, groupobj, orderobj, selectAttr, distinctValues)
#    
    #Step 6: Create the script finally!    
    #In the final query, need only the original select attributes.
#    orgSelect = findStringSelectAttributes(queryobj)
#    orgSelect = myhelper.remAggregate(orgSelect)
#    constructBigQuery(queryList, numRows, orgSelect, addBigWhere)