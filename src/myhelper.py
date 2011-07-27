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


