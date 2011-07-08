import sqlparse
import os
import db_connection

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
    
def find_groupby_clause(sql):
    attr_list = ""
    
    parsedList = parse_sql_as_list(sql)
    
    if (parsedList is None):
        return False
    
    i = 0
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
        return False

def find_attr_clause(clause,delim):
    if (clause):
        clause = clause.strip()
        attr_list = []
        splits = clause.split(delim) # Split the attributes based on the delimiter

        for s in splits:
            s = s.strip()
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
            
        return attr_list
    else:
        return False

def cleanValue (val):
    if (val is None):
        return None
    val = str(val.strip())
    if (len(val)>0):
        return val
    else:
        return None
        

def find_tables (sql):
    parsedList = parse_sql_as_list(sql)
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


def find_select_columns (sql):
    attr_list = ""
    
    parsedList = parse_sql_as_list(sql)
    aggregateKeywords = ["SUM","MIN","MAX","AVG"]
    
    if (parsedList is None):
        return False
    
    i = 0
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
                elif (str(parsedList [i][0]) == 'Token.Keyword' and str(parsedList [i][1]) in aggregateKeywords):
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
    

# Todo - implement the db call
def get_query_result_as_list(query):
    if (query is not None):
        #results = ['COSI','MATH','HIST']
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

def isAggregate (attr):
    aggregateKeywords = ["SUM","MIN","MAX","AVG"]
    if (attr is not None):
        attr = str(attr).strip()
        for keyWord in aggregateKeywords:
            if (keyWord in attr):
                return True
    return False

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

                if (containAggregate):
                    query = selectClause+ ",'"+ str(distVal[0]) +"'"+' INTO '+ str(tempTblName) + str(fromClause) + str(whereClaus)
                else:
                    query = selectClause+' INTO '+ str(tempTblName) + str(fromClause) + str(whereClaus)
                
                qlist.append(query)
                
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
              bigQuery += "SELECT * FROM "+ subSelect+ union
            bigQuery = bigQuery[:-5]
            writeToFile (subSelects[1],bigQuery)

#Function: writeToFile
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
        FILE.write('db.allrows(bigQuery)\n')
    FILE.close()
                
def main():
    
    #db.clear_database() # reset the database on each run. 
    
    query1 = ("SELECT d.name, AVG (e.salary) "
              " FROM employee e, department d "
              " WHERE e.dept_id = d. id "
              " GROUP BY d.name")

    # 1. find the columns in group by clause
    grpByCols = find_groupby_clause(query1)
    
    # 2. split the group by attributes to table alias and column name
    attributes = find_attr_clause(grpByCols,",") # List of tuples t[0] = table alias t[1] = column name
    
    # 3. find all the tables in the query
    tblsInQry = find_tables (query1) # A dictionary of table names and aliases - key alias, value table name
    
    # 4. construct the list of distinct values for the attributes in group by clause
    distinctGrouupVals = find_distinct_group_by_values (tblsInQry,attributes) # dictionary with tableAlias.colums as key and a list of distinct values for that column
    
    # 5. find the columns in the select clause
    selectCols = find_select_columns (query1)
    
    # 6. split the select attributes to table alias and column name
    selAttributes = find_attr_clause(selectCols,",") # List of tuples t[0] = table alias t[1] = column name
    
    # 7. construct the sub selects for each distinct value represented in the group by clause
    subSelects = constructSubSelects (selAttributes, distinctGrouupVals, tblsInQry) # dictionary having the temp table as key and the query for that table as value
    
    # 8. Union the small queries to evaluate the big query
    queryResults = constructBigQueryResult(subSelects)
        
    db.display_schema()
    
if __name__ == "__main__":
    main()