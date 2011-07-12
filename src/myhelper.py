


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

#Comment: How to handle 'BETWEEN pred AND pred'?
#Given a clause, it returns a list of attributes and logical operators in order of appearance.
def split_logical_operators(clause):
    if (clause is None):
        return False
    clause = clause.upper()

    attr_list = []
    foundOr = -1
    foundNot = -1
    numAnd = clause.count(" AND ")
    numOr = clause.count(" OR ")
    numNot = clause.count(" NOT ")
    
    and_split = clause.split(' AND ')
    if (len(and_split) > 0):
        for asplit in and_split: # Go through each of the attribute split over the AND operator
            asplit = cleanValue(asplit)

            #Update loop variables to see if logical operators exist in the split portion
            foundOr = asplit.find(" OR ")
            foundNot = asplit.find(" NOT ")
            
            if (foundOr is -1 and foundNot is - 1):#The attribute has no logical operators
                attr_list.append(asplit)
            else:            
                while(foundOr is not -1 or foundNot is not - 1):#while((foundOr or foundNot) is not - 1):
                    if (foundOr is not - 1): #Has an OR logical operator
                        or_split = asplit.split(" OR ")
                        if (len(or_split) > 0):
                            for osplit in or_split:
                                osplit = cleanValue(osplit)

                                #Update loop variables
                                foundNot = osplit.find(" NOT ")
                                foundOr = osplit.find(" OR ")
                                
                                if (foundNot is - 1): # No more NOT logical operators
                                    attr_list.append(osplit)
                                else:
                                    while (foundNot is not - 1):
                                        (foundNot, foundOr, numNot,attr_list) = find_not_operator(osplit,numNot,attr_list)
                                if (numOr > 0):    
                                    attr_list.append(" OR ")
                                    numOr-=1
                    else:
                        (foundNot, foundOr, numNot,attr_list) = find_not_operator(asplit,numNot,attr_list)
            if (numAnd > 0):                        
                attr_list.append(" AND ")
                numAnd-=1
        
    return attr_list

#This function is a helper for the function: split_logical_operators
#For the given sub_attr, the function goes through and returns a list of the 
# NOT attributes it has. Also updates the needed loop variables.    
def find_not_operator(sub_attr,numNot, attr_list):
    not_split = sub_attr.split(" NOT ")
    for nsplit in not_split:
        nsplit = cleanValue(nsplit)
        foundNot = nsplit.find(" NOT ")
        foundOr = nsplit.find(" OR ")
        
        attr_list.append(nsplit)
        if(numNot>0):
            attr_list.append(" NOT ")
            numNot-=1
        
    return (foundNot, foundOr, numNot, attr_list)

