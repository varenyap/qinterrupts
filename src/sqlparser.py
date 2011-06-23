import sqlparse
import db_connection

db = db_connection.Db_connection()

def parse_sql_as_list (sql):
    if (sql is None or len (sql) == 0):
        return None
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
                tableMap [attr_list[i]] =  attr_list[i+1]
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
            tblCol = str(table)+'.'+str(column)
            query = 'SELECT DISTINCT '+ column + ' FROM '+ str(table)
            results = get_query_result_as_list (query)
            distinctResults [tblCol] = results
        return  distinctResults


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
                    attr_list = attr_list + str(parsedList[i+1][1])
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
        results = []        
        results = db.allrows(query) 
        #make the db call, populate the results list. Return None if there are no results or some sillyness happens
        return results
    else:
        return None

def getTableNameForAlias (queryTables, tblAlias):
    if (tblAlias is None) :
        return None
    tblAlias = tblAlias.strip()
    if (queryTables and len(tblAlias) > 0):
        for table in queryTables.iterkeys():
            if (str(queryTables[table]) == str(tblAlias)):
                return str(table)
    return None
    

#def constructSubSelects (selAttributes, distinctQueris, tblsInQry):
#    if (selAttributes and distinctQueris and tblsInQry):
        
                
def main():
    db.clear_database() # reset the database on each run.
    
    query1 = ("SELECT d.name, AVG (e.salary) "
              " FROM employee e, department d "
              " WHERE e.dept_id = d. id "
              " GROUP BY d.name")

    # 1. find the columns in group by clause
    grpByCols = find_groupby_clause(query1)
    
    # 2. split the group by attributes to table alias and column name
    attributes = find_attr_clause(grpByCols,",") # List of tuples t[0] = table alias t[1] = column name 
    
    # 3. find all the tables in the query
    tblsInQry = find_tables (query1) # A dictionary of table names and aliases - key table name, value alias
    
    # 4. construct the list of distinct values for the attributes in group by clause
    distinctQueris = find_distinct_group_by_values (tblsInQry,attributes) # dictionary with tableAlias.colums as key and a list of distinct values for that column
    
    # 5. find the columns in the select clause
    selectCols = find_select_columns (query1)
    
    # 6. split the select attributes to table alias and column name
    selAttributes = find_attr_clause(selectCols,",") # List of tuples t[0] = table alias t[1] = column name 
    
    print selAttributes
    
    
    
    
if __name__ == "__main__":
    main()