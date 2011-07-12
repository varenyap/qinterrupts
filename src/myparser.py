import sqlparse
import db_connection
import myhelper

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

#def get_query_result_as_list(query):
#    if (query is not None):
#        #results = ['COSI','MATH','HIST']
#        print "Line 209: %s" %query
#        results = db.allrows(query)
#        return results
#    else:
#        return None

#Used for the group by clause and the select clause
def find_attr_clause(clause,delim):
#    print "line 94: Clause: --%s--"%clause
    if (clause):
        clause = myhelper.cleanValue(clause)
        attr_list = []
        splits = clause.split(delim) # Split the attributes based on the delimiter

        for s in splits:
            s = myhelper.cleanValue(s)
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
                    if (myhelper.isWhereClauseOperator(str_parsedList) or myhelper.isLogicalOperator(str_parsedList)):
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
        return myhelper.cleanValue(attr_list)
    else:
        print "Error: Couldn't find where clause in query"
        return False

def find_where_attr(clause):    
    if (clause is None):
        return False
    clause = myhelper.cleanValue(clause)
#    print "I am in the function --%s--" %clause
#    length = len(clause)
#    print "Length = %d" %length
#    attr_list = []
    print clause
    myhelper.split_logical_operators(clause)    

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
            results = db.allrows(query)
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
                elif (str(parsedList [i][0]) == 'Token.Keyword' and myhelper.isAggregate(str(parsedList [i][1]))):
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

def getTableNameForAlias (queryTables, tblAlias):
    if (tblAlias is None) :
        return None
    tblAlias = myhelper.cleanValue(tblAlias)
    if (queryTables and len(tblAlias) > 0):
        for alias in queryTables.iterkeys():
            if (alias == str(tblAlias)):
                return str(queryTables[alias])
    return None

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
                    val = myhelper.cleanValue (str(parsedList[i][1]))
                    if (val is not None):
                        attr_list.append(val)
                    i+=1
                elif (str(parsedList [i][0]) == 'Token.Keyword' and str(parsedList [i][1]) == 'JOIN'): # Straight forward joins
                    val = myhelper.cleanValue(str(parsedList[i+1][1]))
                    if (val is not None):
                        attr_list.append(val)
                    i+=1
                elif (str(parsedList [i][0]) == 'Token.Keyword' and str(parsedList [i+2][0]) == 'Token.Keyword' and str(parsedList [i+2][1]) == 'JOIN'): # joins like left, right, natural etc
                    val = myhelper.cleanValue(str(parsedList[i+3][1]))
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




