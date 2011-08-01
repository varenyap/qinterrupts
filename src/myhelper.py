#===============================================================================
# myhelper.py
# Contains all 'helper' functions required to parse a SQL query
#===============================================================================

from sqlparse.tokens import *#Used for match function

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
    aggregateKeywords = ["SUM","MIN","MAX","AVG"]
    if (tok is not None):
        for key in aggregateKeywords:
            if(tok.match(Keyword, key)):
                return True
    return False

def remAggregate (input):
    input = input.replace("(",'')
    input = input.replace(")",'')
    input = input.replace(".",'_')
    return input

def isWhereClauseOperator(tok):
    whereClauseOperators = ["=", "<>", "!=", ">", "<", ">=", "<=", "BETWEEN", "LIKE", "IN", "IS NULL"]
    if (tok is not None):
        for key in whereClauseOperators:
            if(tok.match(Keyword, key)):
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

def removeListDuplicates(seq): 
    # order preserving
    noDupes = []
    [noDupes.append(i) for i in seq if not noDupes.count(i)]
    return noDupes

def findSelectClauseWithoutAggregates(selectIdent):
    
    print "select clause without aggregates: %s" %selectIdent
    selectWithoutAggregates = []
    for sid in selectIdent:
        if (not isAggregate(sid)):
#            print "not an aggregate: %s " %sid
            if (not "(" in str(sid)):
                selectWithoutAggregates.append(sid)
#        else:
#            print "found aggregate: %s" %sid
            
#    print selectWithoutAggregates
    return selectWithoutAggregates

def findGroupbyRows(selectIdentWithoutAggregates,distinctGroupbyValues):
#    print "find groupby Rows"
#    print distinctGroupbyValues
#    print selectIdentWithoutAggregates
    max = 1
    iterations = 0
    lenSeen = []
    numRows = 1
    if selectIdentWithoutAggregates is None:
        print "select has only aggregates"
            
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
    #    print max
        numRows = max * numRows
        print numRows
    return numRows
    
    
    
    
    
    