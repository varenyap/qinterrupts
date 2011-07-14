
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

def remAggregate (attr):
    aggregateKeywords = ["SUM","MIN","MAX","AVG"]
    if (attr is not None):
        attr = cleanValue(str(attr))
        attr = attr.upper()
        for keyWord in aggregateKeywords:
            if (keyWord in attr):
                attr = attr.replace("(",'')
                attr = attr.replace(")",'')
                return attr.replace(keyWord,'').lower()
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

def isparenthesis(str):
    if (str is not None):
        types_parenthesis = ["(", ")"]
        if (cleanValue(str) in types_parenthesis):
            return True
    return False

def split_parenthesis(clause_list):
    attr_list = []
    foundParen = False
    
    for item in clause_list:
        index= 0
        parenValue = ""
#        print item
        for str in item:
            if (isparenthesis(str)):
#                print "I found a paren at %d in %s " %(index,item)
                foundParen = True
                parenValue = str
                break
            index+=1
        if (foundParen):
            attr_list.append(parenValue)
            index+=1
            temp = item[index:]
#            print "temp: %s"%temp
            if (temp is not ""):
                attr_list.append(temp)
            foundParen = False
        else:
            if (item is not ""):
                attr_list.append(item)
    
    return attr_list

# Input is of the following example format: ['SALARY BETWEEN 90000', ' AND ', '459891']
# returns dictionary {attribute, max, min} i.e {'attribute':salary,'max':459891, 'min':90000}
# If there is no between, returns False
# Todo: dealing with parenthesis in the having clause
def between_operator_attr(attrList):
    if (attrList is None):
        return False

    for attr in attrList:
        attr = cleanValue(str(attr))
        attr = attr.upper()
        bindex = isBetweenOperator(attr) 

        if (bindex is not -1):
            length = len(attr)
            havingattribute = ""
            min = ""
            i = 0
            while(i < bindex): #Grab the attribute
                havingattribute+=str(attr[i])
                i+=1
            while(i <length and attr[i] is not " "): #past the between
                i+=1
            while (i <length): #Grab the minimum
                min+= attr[i]
                i+=1

            max = attrList[2] #Grab max, needs to be fixed to include parenthesis
            return {'attribute': cleanValue(havingattribute), 'max':cleanValue(max), 'min':cleanValue(min) }
    
    return False

#function determines if the input i.e attr is 'Between'
def isBetweenOperator(attr):    
    if (attr is not None):
        attr = cleanValue(str(attr))
        attr = attr.upper()
        index = attr.find("BETWEEN")
        return index    



def main():
    clause = "e.dept_id = d.id and (e.dept_od = f.piece or d.dept_id = 89) "
    list = split_logical_operators(clause)
    print split_parenthesis(list)
    
    

    
if __name__ == "__main__":
    main()