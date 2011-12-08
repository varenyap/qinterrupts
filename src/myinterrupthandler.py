# Step 3: The sub queries generated are sent to the Interrupt Handler one by one.
#         The interrupt handler keeps track of which query is being executed at all times,
#         and  any temporary tables that are created.
#
# Step 3a: The user decides to interrupt while a sub query is begin executed.
#          This is the point where it is important for the system to keep track of which sub query
#          is being executed. So when the user decides to interrupt, then the system is able to perform
#          any clean up operations that are required. For instance, on an interrupt, the sub query could
#          finish execution or be canceled midway.

# Step 3b: If the user doesnt interrupt, then execution continues as normal. All the sub queries are
#          executed against the database sequentially. Results are returned to the user in the order
#          they are computed.


import db_connection
import myqueryclauses
import  myhelper
import myparser
import myqueryconstructor

db = db_connection.Db_connection()

def interruptHandler(queryobj, groupobj, orderobj, selectAttr, distinctValues):
    if (queryobj and groupobj and orderobj and selectAttr and distinctValues):
        constructSubSelects(queryobj, groupobj, orderobj, selectAttr, distinctValues)
    else:
        print "Error: Cannot enable interrupts"


def constructSubSelects (queryobj, groupobj, orderobj, selectAttr, distinctValues):
    if (queryobj and selectAttr and distinctValues):
        
        print "I Have valid input for construct subselects"
        
        #Temporary data structures required
        queryList = {}
        selectList = {}
#        selectIntoList = {}
        whereList = {}
        addBigWhere = ""
        insertClause = " INSERT INTO finalOutputTable (" + selectAttr + ")"        
            
        selectAttrList = selectAttr.split(",")#contains list of attributes in the select clause of sub-queries
        containsAggregate = queryobj.getSelectContainsAggregate() or orderobj.getOrderbyContainsAggregate()
        orgWhere = queryobj.getWhereIdent()
    #        if (orgWhere is None): # the original query has no where clause
    #            orgWhere = ""
        numAttr = len(selectAttrList)
        numRows = len(distinctValues)
        fromClause = " FROM " + myqueryconstructor.findStringFromAttributes(queryobj)
            
            #To be used when order by has an aggregate in it
        groupbyClause = " "
            
        for attr in selectAttrList:
            if("(" in attr):
                addBigWhere += myhelper.remAggregate(attr) + " IS NOT NULL AND "
                insertClause += myhelper.remAggregate(attr)
            else:
                groupbyClause += str(attr) + ", "
                insertClause += str(attr) + ", "
        insertClause = insertClause.rstrip(", ")
        insertClause += ")"
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
                        selectList[iterations] += "'" + attrValue + "'::Text AS " + myhelper.remAggregate(attrName) + ", "
                        whereList[iterations] += attrName + " = '" + attrValue + "' AND " 
                    else:
                        selectList[iterations] += attrName + " AS " + myhelper.remAggregate(attrName) + ", "
                else:#select contains no aggregate
                    selectList[iterations] += attrName + ", "
                    if("(" not in attrName):
                        whereList[iterations] += attrName + " = '" + attrValue + "' AND "
                num += 1
    
            selectList[iterations] = selectList[iterations].rstrip(", ")
            whereList[iterations] = whereList[iterations].rstrip(" AND ")
    
            queryList[iterations] = selectList[iterations] + fromClause + whereList[iterations] + groupbyClause
            print "Executing Sub-query: %s" % str(iterations + 1)
            db.make_pquery(queryList[iterations])
#            print queryList[iterations]
            iterations += 1

#        return (queryList, numRows, addBigWhere)
    print "Error in creating subqueries"
    return None











