#===============================================================================
# myhelper.py
# Contains all 'helper' functions required to parse a SQL query
#===============================================================================

from sqlparse.tokens import *#Used for match function
from sqlparse import sql

def cleanValue (val):
    if (val is None):
        return None
    val = str(val.strip())
    if (len(val)>0):
        return val
    else:
        return None

#Function checks if the token passed in is an aggregate type
def isAggregate (tok):
    aggregateKeywords = ["SUM","MIN","MAX","AVG", "COUNT", "FIRST", "LAST"]
    if (tok is not None):
        for key in aggregateKeywords:
            if(tok.match(Keyword, key)):
                return True
    return False

def remAggregate (input):
    input = input.strip()
    input = input.replace("(",'')
    input = input.replace(")",'')
    input = input.replace(".",'_')
    input = input.replace(" ", "_")
    
    selectOperators = ["NOT", "OR", "AND", "+", "-", "*", "/", "=", "<>", "!=", ">", "<", ">=", "<="]
    if (input is not None):
        for key in selectOperators:
            if (key in str(input)):
                input = input.replace(key, "")
    return input

def isWhereClauseOperator(tok):
    whereClauseOperators = ["=", "<>", "!=", ">", "<", ">=", "<=", "BETWEEN", "LIKE", "IN", "IS NULL"]
    if (tok is not None):
        for key in whereClauseOperators:
            if(tok.match(Keyword, key)):
                return True
    return False

def isMathOperator(input):
    mathOperators = ["+", "-", "*", "/", "=", "<>", "!=", ">", "<", ">=", "<="]
    if (input is not None):
        if str(input) in mathOperators:
            return True
    return False

def isLogicalOperator(tok):
    logicalOperators = ["NOT", "OR", "AND"]
    if (tok is not None):
        for key in logicalOperators:
            if (tok.match(Keyword, key)):
                return True
    return False

def isOrderbyOperator(tok):
    orderbyOperators = ["ASC", "DESC"]
    if (tok is not None):
        for key in orderbyOperators:
            if (tok.match(Keyword, key)):
                return True
    return False

def hasSelectOperator(input):
    selectOperators = ["NOT", "OR", "AND", "+", "-", "*", "/", "=", "<>", "!=", ">", "<", ">=", "<="]
    if (input is not None):
        for key in selectOperators:
            if (key in str(input)):
                return True
    return False
def removeListDuplicates(seq): 
    # order preserving
    noDupes = []
    [noDupes.append(i) for i in seq if not noDupes.count(i)]
    return noDupes

def findSelectClauseWithoutAggregates(selectIdent):
    if (selectIdent):
        selectWithoutAggregates = []
        for sid in selectIdent:
            if (not isAggregate(sid)):
                if (not "(" in str(sid)):
                    selectWithoutAggregates.append(sid)
    #    print selectWithoutAggregates
        return selectWithoutAggregates
    return None

def findGroupbyRows(selectIdentWithoutAggregates,distinctGroupbyValues):

    max = 1
    iterations = 0
    lenSeen = []
    numRows = 1
    if selectIdentWithoutAggregates is None:
#        print "select has only aggregates"
        print"Select has no attributes so how to find group by rows"
            
    else:
        for item in selectIdentWithoutAggregates:
            if (iterations == 0):
                numRows = len(distinctGroupbyValues[str(item)])
                iterations+=1
            else:
                currLength = len(distinctGroupbyValues[str(item)])
                if (currLength not in lenSeen):
                    lenSeen.append(currLength)
                    max = max * currLength
        numRows = max * numRows
#        print numRows
    return numRows

def checkIfList(ident):
    if (isinstance(ident, sql.Identifier)):#d.name
            return False
    return True

    
    
#Unused
#def getResultSetAsList(query):
#    if (query is not None):
#        results = db.allrows(query)    
#        return results
#    return None