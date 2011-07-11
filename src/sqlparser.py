import sqlparse
import db_connection
import re

db = db_connection.Db_connection()

def parse_sql_as_list (sql):
    if (sql is None or len (sql) == 0):
        return None
    sql = sqlparse.format(sql, reindent=True, keyword_case='upper')

    parsedQuery = sqlparse.lexer.tokenize (sql)
    # Convert the parsed query to a list
    parsedList = []
    for parsed in parsedQuery:
        parsedList.append(parsed)
    return parsedList

def cleanValue (val):
    if (val is None):
        return None
    val = str(val.strip())
    if (len(val)>0):
        return val
    else:
        return None

def isAggregate (attr):
    aggregateKeywords = ["SUM","MIN","MAX","AVG"]
    if (attr is not None):
        attr = cleanValue(str(attr))
        attr = attr.upper()
        for keyWord in aggregateKeywords:
            if (keyWord in attr):
                return True
    return False

def isWhereClauseOperator(attr):
    whereClauseOperators = ["=", "<>", "!=", ">", "<", ">=", "<=", "BETWEEN", "LIKE", "IN", "IS NULL"]
    if (attr is not None):
        attr = cleanValue(str(attr))
        attr = attr.upper()
        for keyWord in whereClauseOperators:
            if (keyWord in attr):
                return True
    return False

def isLogicalOperator(attr):
    logicalOperators = ["NOT", "OR", "AND"]
    if (attr is not None):
        attr = cleanValue(str(attr))
        attr = attr.upper()
        for keyWord in logicalOperators:
            if (keyWord in attr):
                return True
    return False

def find_where_clause(parsedList):
    if (parsedList is None):
        return False
            
    i = 0
    attr_list = ""
    foundWhere = False
    foundAttr = False
    length = len(parsedList)
    
    for parsed in parsedList:
        i += 1
        if (str(parsed[0]) == 'Token.Keyword'):
            if (str(parsed[1]) == 'WHERE'):
                foundWhere = True
            
        if (foundWhere): #found the Where part of the query
            while (i< length and foundAttr is False):
                if(str(parsedList[i][0]) =='Token.Keyword'): 
                    str_parsedList = str(parsedList[i][1])
                    #Make sure valid where clause operators/keywords are included
                    if (isWhereClauseOperator(str_parsedList) or isLogicalOperator(str_parsedList)):
                        attr_list = attr_list + str_parsedList
                    else:
                        foundAttr = True
                    i+=1
                else:
                    attr_list = attr_list + str(parsedList[i][1])
                    i+=1
            break;
        if (foundAttr): #No need for loop to continue
            break;
            
    #Set return values
    if (foundWhere):
        return cleanValue(attr_list)
    else:
        print "Error: Couldn't find where clause in query"
        return False

def split_logical_operators(clause):
    if (clause is None):
        return False
    clause = clause.upper()
    #WHERE e.dept_id = d.id and e.dept_od = f.piece or d.dept_id = 89
    attr_list = []
    foundOr = -1
    foundNot = -1
    foundAnd = -1
    
    numAnd = clause.count(" AND ")
    numOr = clause.count(" OR ")
    numNot = clause.count(" NOT ")
    
    and_split = clause.split(' AND ')
    print "---------------My and_split is :"
    print and_split
    print "End of and_split----------------"
    
    if (len(and_split) > 1):
        foundAnd = 1
    
    if (len(and_split) > 0):
        for asplit in and_split:
            asplit = asplit.strip()
            print "#### Asplit: %s ####" % asplit
            foundOr = asplit.find(" OR ")
            foundNot = asplit.find(" NOT ")
            
            if (foundOr is -1 and foundNot is - 1):
                attr_list.append(asplit)
            else:            
                while(foundOr is not -1 or foundNot is not - 1):#while((foundOr or foundNot) is not - 1):
                    if (foundOr is not - 1):
                        print "I found an OR in the asplit"
                        or_split = asplit.split(" OR ")
                        if (len(or_split) > 0):
                            for osplit in or_split:
                                osplit = osplit.strip()
                                print "#### Osplit: %s ####" % osplit
                                foundNot = osplit.find(" NOT ")
                                foundOr = osplit.find(" OR ")
                                
                                if (foundNot is - 1):
                                    print "I didnt find a not"
                                    attr_list.append(osplit)
                                else:
                                    print "I found a not"
                                    while (foundNot is not - 1):
                                        not_split = osplit.split(" NOT ")
                                        
                                        for nsplit in not_split:
                                            nsplit = nsplit.strip()
                                            foundNot = nsplit.find(" NOT ")
                                            foundOr = nsplit.find(" OR ")
                                            
                                            attr_list.append(nsplit)
                                            if(numNot > 0):
                                                attr_list.append(" NOT ")
                                                numNot-=1
                                if (numOr > 0):    
                                    attr_list.append(" OR ")
                                    numOr-=1
                    else:
                        print "I have a not"
                        not_split = asplit.split(" NOT ")
                        for nsplit in not_split:
                            nsplit = nsplit.strip()
                            foundNot = nsplit.find(" NOT ")
                            foundOr = nsplit.find(" OR ")
                            
                            attr_list.append(nsplit)
                            if (numNot>0):
                                attr_list.append(" NOT ")
                                numNot-=1
            if (numAnd > 0):                        
                attr_list.append(" AND ")
                numAnd-=1
        
    print attr_list
 

    
    
    

def find_where_attr(clause):
    
    
    if (clause is None):
        return False
    clause = cleanValue(clause)
#    print "I am in the function --%s--" %clause
    length = len(clause)
#    print "Length = %d" %length
    attr_list = []
    print clause
    split_logical_operators(clause)
    
#    if (clause):
#        clause = clause.strip()
#        attr_list = []
#        splits = clause.split(delim) # Split the attributes based on the delimiter
#
#        for s in splits:
#            #s = s.strip()
#            s = cleanValue(s)
#            i = 0
#            table = ""
#            attr = ""
#            if (s.find(".") != -1):
#                while(i<len(s) and s[i] != '.'):
#                    table = table + s[i]
#                    i+=1
#                i+=1 # Go past the dot.
#            while (i<len(s)):
#                attr = attr + s[i]
#                i+=1
#            tup = (table, attr)
#            attr_list.append(tup)
##        print "line 115: Attr list --%s--" %attr_list    
#        return attr_list
#    else:
#        return False
    
    
    
    
    
    

def find_groupby_clause(parsedList):
    if (parsedList is None):
        return False
    
    i = 0
    attr_list = ""
    foundGroup = False
    foundAttr = False
    length = len(parsedList)
    
    for parsed in parsedList:
        i += 1
        if (str(parsed[0]) == 'Token.Keyword'):
            if (str(parsed[1]) == 'GROUP'):
                foundGroup = True

        if (foundGroup): #found the group part of the clause
            if (str(parsedList[i+1][0]) == 'Token.Keyword' and str(parsedList [i+1][1]) == 'BY'):
                i+=2 #At this point, found GROUP BY tokens so moving past them
                while (i < length and foundAttr is False):
                    if (str(parsedList [i][0]) != 'Token.Keyword'):
                        attr_list = attr_list + str(parsedList[i][1])
                        i+=1
                    else:
                        foundAttr = True
            break;
        if (foundAttr): #No need for loop to continue
            break;
    
    #Set return values
    if (foundGroup):
        return attr_list
    else:
        print "Error: Couldn't find group by clause in query"
        return False

#Used for the group by clause and the select clause
def find_attr_clause(clause,delim):
#    print "line 94: Clause: --%s--"%clause
    if (clause):
        clause = clause.strip()
        attr_list = []
        splits = clause.split(delim) # Split the attributes based on the delimiter

        for s in splits:
            #s = s.strip()
            s = cleanValue(s)
            i = 0
            table = ""
            attr = ""
            if (s.find(".") != -1):
                while(i<len(s) and s[i] != '.'):
                    table = table + s[i]
                    i+=1
                i+=1 # Go past the dot.
            while (i<len(s)):
                attr = attr + s[i]
                i+=1
            tup = (table, attr)
            attr_list.append(tup)
#        print "line 115: Attr list --%s--" %attr_list    
        return attr_list
    else:
        return False

def find_tables (parsedList):
    if (parsedList is None):
        return False
    
    i = 0
    foundFrom = False
    foundAttr = False
    length = len(parsedList)
    attr_list = []
    for parsed in parsedList:
        i += 1
        if (str(parsed[0]) == 'Token.Keyword'):
            if (str(parsed[1]) == 'FROM'):
                foundFrom = True

        if (foundFrom): #found the from part of the sql
            i+=1 #At this point, found FROM clause so moving past them
            while (i < length and foundAttr is False):
                if (str(parsedList [i][0]) != 'Token.Keyword'):
                    val = cleanValue (str(parsedList[i][1]))
                    if (val is not None):
                        attr_list.append(val)
                    i+=1
                elif (str(parsedList [i][0]) == 'Token.Keyword' and str(parsedList [i][1]) == 'JOIN'): # Straight forward joins
                    val = cleanValue(str(parsedList[i+1][1]))
                    if (val is not None):
                        attr_list.append(val)
                    i+=1
                elif (str(parsedList [i][0]) == 'Token.Keyword' and str(parsedList [i+2][0]) == 'Token.Keyword' and str(parsedList [i+2][1]) == 'JOIN'): # joins like left, right, natural etc
                    val = cleanValue(str(parsedList[i+3][1]))
                    if (val is not None):
                        attr_list.append(val)
                    i+=1
                else:
                    foundAttr = True
            break;
        if (foundAttr): #No need for loop to continue
            break;
    
    #Set return values
    if (foundFrom and attr_list):
        i = 0
        for attr in attr_list:
            if (attr == ','):
                del (attr_list[i])
            i += 1
        tableMap = {}
        i = 0
        for attr in attr_list:
            if (len (attr_list) > i+1):
                tableMap [attr_list[i+1]] = attr_list[i]
            i +=2

        return tableMap
    else:
        print "Error: Couldn't parse the from clause in query"
        return False

def find_distinct_group_by_values (queryTables, grpByCols):
    if (queryTables and grpByCols):
        distinctResults = {}
        for col in grpByCols:
            column = col [1]
            alias = col [0]
            table = getTableNameForAlias (queryTables, alias)
            if (table is None):
                continue
            tblCol = str(alias)+'.'+str(column)
            query = 'SELECT DISTINCT '+ column + ' FROM '+ str(table)
            results = get_query_result_as_list (query)
            distinctResults [tblCol] = results
        return distinctResults

def find_select_columns (parsedList):
    if (parsedList is None):
        return False
    
    i = 0
    attr_list = ""
    foundSelect = False
    foundAttr = False
    length = len(parsedList)
    
    for parsed in parsedList:
        i += 1
        if (str(parsed[0]) == 'Token.Keyword.DML'):
            if (str(parsed[1]) == 'SELECT'):
                foundSelect = True

        if (foundSelect): #found the group part of the clause
            while (i < length and foundAttr is False):
                if (str(parsedList [i][0]) != 'Token.Keyword'):
                    attr_list = attr_list + str(parsedList[i][1])
                    i+=1
                elif (str(parsedList [i][0]) == 'Token.Keyword' and isAggregate(str(parsedList [i][1]))):
                    attr_list = attr_list + str(parsedList [i][1])+str(parsedList[i+1][1])
                    i+=1
                else:
                    foundAttr = True
            break;
        if (foundAttr): #No need for loop to continue
            break;
    
    #Set return values
    if (foundSelect):
        return attr_list
    else:
        return False

def get_query_result_as_list(query):
    if (query is not None):
        #results = ['COSI','MATH','HIST']
        print "Line 209: %s" %query
        results = db.allrows(query)
        return results
    else:
        return None

def getTableNameForAlias (queryTables, tblAlias):
    if (tblAlias is None) :
        return None
    tblAlias = tblAlias.strip()
    if (queryTables and len(tblAlias) > 0):
        for alias in queryTables.iterkeys():
            if (alias == str(tblAlias)):
                return str(queryTables[alias])
    return None

def constructSubSelects (selAttributes, distinctGrouupVals, tblsInQry):
    if (selAttributes and distinctGrouupVals and tblsInQry):
        queryTemptblMap = {}
        selColumns = ''
        containAggregate = False
        
        for selAtt in selAttributes:
            if (isAggregate(selAtt)) :
                containAggregate = True
                selColumns = selColumns + str(selAtt[0])+'.'+str(selAtt[1])+','
        
        if (not containAggregate):
            for selAtt in selAttributes:
                selColumns = selColumns + str(selAtt[0])+'.'+str(selAtt[1])+','
        
        selColumns = selColumns.rstrip(",")
        selectClause = 'SELECT '+ str(selColumns)
        fromClause = ' FROM '
        
#        print "line 253 - select clause -  %s" %selectClause
        for alias in tblsInQry.iterkeys():
            table = tblsInQry[alias]
            fromClause = fromClause + table+ ' '+ alias + ','
        fromClause = fromClause.rstrip(",")
        
        # Colletct queries with in to part
        qlist = []
        for val in distinctGrouupVals.iterkeys():
            tempDistVals = distinctGrouupVals[val]
            for distVal in tempDistVals:
                whereClaus = ' WHERE '+ str(val)+ '=' + "'"+str(distVal[0])+"'"
                tempTblName = str(val)+"_"+str(distVal[0])
                tempTblName = tempTblName.replace('.','_')
                
                # SELECT AVG  (e.salary),'COSI' INTO d_name_COSI FROM employee e,department d WHERE d.name='COSI'
                
                if (containAggregate):
                    query = selectClause+ ",'"+ str(distVal[0]) +"'"+' INTO '+ str(tempTblName) + str(fromClause) + str(whereClaus)
                else:
                    query = selectClause+' INTO '+ str(tempTblName) + str(fromClause) + str(whereClaus)
                
                qlist.append(query)
                print "line 274: %s" %query
                
                db.make_query(query)# make a db call and run the insert the query
                if (containAggregate):
                    #ALTER TABLE name_cosi ALTER COLUMN "?column?" TYPE varchar(20);
                    alterQuery = "ALTER TABLE " + str(tempTblName) + """ ALTER COLUMN "?column?" TYPE VARCHAR(20); """
                    db.make_query(alterQuery)
                    qlist.append(alterQuery)
                
                # construct the query without in to part to persist which temp table contains which query result
                if (containAggregate):
                    query = selectClause + ",'"+str(distVal[0])+"'" + fromClause + whereClaus
                    queryTemptblMap [tempTblName] = query
                else:
                    query = selectClause + fromClause + whereClaus
                    queryTemptblMap [tempTblName] = query
        
        retVal = []
        retVal.append(queryTemptblMap)
        retVal.append(qlist)        
        return retVal

def constructBigQueryResult (subSelects):
    if (subSelects):
        dictSS = subSelects [0]
        if (dictSS):
            bigQuery = ''
            union = " UNION "
            for subSelect in dictSS.iterkeys():
                bigQuery += "SELECT * FROM "+ subSelect + union
                
            bigQuery = bigQuery[:-6]
            result = db.allrows(bigQuery)
            print "line 306: %s" %bigQuery
            writeToFile (subSelects[1],bigQuery)
            return result

#Purpose: Write the parameters passed in to the method in to a python script file called scripts.py
def writeToFile (subSelects,bigQuery):
    filename = "scripts.py"
    FILE = open(filename,"w")
    if (not subSelects or len (bigQuery) == 0):
        FILE.write('Unknown problem with queries')
    else:   
        FILE.write('import db_connection\n\n')
        FILE.write('db = db_connection.Db_connection()\n\n')
        FILE.write('db.clear_database() # reset the database on each run.\n\n')
        FILE.write('# Make a db call and run the sub queries that will collectively evaluate to the main query result\n')
        for subSelect in subSelects:
            FILE.write('db.make_query('+subSelect+')\n')
        
        
        FILE.write('\n# The query that combines the results of small queries\n')
        FILE.write('bigQuery = '+bigQuery+'\n\n')
        FILE.write('db.make_pquery(bigQuery)\n')
    FILE.close()
                
def main():
    
    db.clear_database() # reset the database on each run. 
    
    query1 = ("SELECT d.name, AVG (e.salary) "
              " FROM employee e, department d "
              " WHERE e.dept_id = d.id and e.dept_od = f.piece or d.dept_id = 89 "
              " GROUP BY d.name")
#    query1 = ("SELECT e.name "
#              "FROM employee e"
#              "GROUP BY e.name ")
    
    parsed_list = parse_sql_as_list(query1) #Parse sql into a walk-able list     
    where_clause = find_where_clause(parsed_list)
    
    find_where_attr(where_clause)
    
    
    
#
#    # 1. find the columns in group by clause
#    grpByCols = find_groupby_clause(parsed_list)
##    print "--%s--" %grpByCols
#    
#    # 2. split the group by attributes to table alias and column name
#    attributes = find_attr_clause(grpByCols,",") # List of tuples t[0] = table alias t[1] = column name
#    
#    # 3. find all the tables in the query
#    tblsInQry = find_tables(parsed_list) # A dictionary of table names and aliases - key alias, value table name
#    print tblsInQry
#    
#    # 4. construct the list of distinct values for the attributes in group by clause
#    distinctGrouupVals = find_distinct_group_by_values (tblsInQry,attributes) # dictionary with tableAlias.colums as key and a list of distinct values for that column
#    
#    # 5. find the columns in the select clause
#    selectCols = find_select_columns (parsed_list)
#    
#    # 6. split the select attributes to table alias and column name
#    selAttributes = find_attr_clause(selectCols,",") # List of tuples t[0] = table alias t[1] = column name
#    
#    # 7. construct the sub selects for each distinct value represented in the group by clause
#    subSelects = constructSubSelects (selAttributes, distinctGrouupVals, tblsInQry) # dictionary having the temp table as key and the query for that table as value
#    
#    #8. Union the small queries to evaluate the big query
#    queryResults = constructBigQueryResult(subSelects)
#    print" Results: \n\n%s" %queryResults
        
#    db.display_schema()
    
if __name__ == "__main__":
    main()