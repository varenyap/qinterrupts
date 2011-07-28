#===============================================================================
# myparser.py
# Given a SQL query, the file is used to parse the query into its various
# components such that the attributes for each clause are returned in a
# read-able format.
# For each clause, returns an identifier, or an identifier list.
# Handles the following keywords: SELECT, FROM, WHERE, GROUP BY, ORDER BY and
# all aggregates
# Need the Having clause
#===============================================================================

import sqlparse
from sqlparse import sql
from sqlparse.tokens import *
import myhelper
import myqueryclauses

#queryclauses = myqueryclauses.myqueryclauses()
import myqueryconstructor

def getUserInput():    
    userInput = ""
    entry = raw_input("Enter query, 'done' on its own line to quit: \n")
    while entry.lower() != "done":
        userInput+=str(entry)
        userInput+=" "
        entry = raw_input("")
    return userInput

def tokenizeUserInput(userInput):
#    query1 = ("SELECT d.name, AVG (e.salary) "
#              " FROM employee e, department d"
#              " WHERE e.dept_id = d.id"
#              " GROUP BY d.name")
#    
#    userInput = query1
    
    fromattedquery = sqlparse.format(userInput,keyword_case = 'upper', identifier_case = 'lower', strip_comments = True)
    mystmt = sqlparse.parse(fromattedquery)[0]
#    print "Statement: %s" %mystmt
    mytok = mystmt.tokens
    mytoklen = len(mytok)
    return (mytok, mytoklen)

def displayTokens(mytok,mytoklen):
    idx=0
    while (idx <mytoklen):
        print "%d: ~~%s~~ Type is: '%s' Value is: ~%s~" %(idx, mytok[idx], mytok[idx].ttype, mytok[idx].value)
        idx+=1
    print " ----------------------------Finished printing the 'tokenized' query-----------------------"

def myParser(mytok, mytoklen):
    
    queryobj = myqueryclauses.myqueryclauses()
        
    i=0
    while(i <mytoklen):

        if (mytok[i].is_whitespace()):      
            i+=1
        else:    
            
            if (str(mytok[i].ttype) == 'Token.Keyword.DML' and str(mytok[i].value) == "SELECT"):
                i = incrementIfWhitespace(mytok[i+1], i)
                
                selectIdent = findIdentifierList(mytok[i+1])
                if (selectIdent is None):
                    mytoklist = []
                    foundAttr = False
                    foundAggregate = False
                    
                    while (i <mytoklen and foundAttr is False):
                        (foundAttr, mytoklist,foundAggregate) = findIdentifierListWithKeywords(mytok[i],mytoklist)
                        if (foundAggregate):
                            queryobj.setSelectContainsAggregate(foundAggregate)
                        if (foundAttr):
                            i-=2
                            break;
                        i+=1# From where loop at line 53
                    selectIdent = (sqlparse.sql.IdentifierList(mytoklist)).get_identifiers()
                
                
                queryobj.setSelectIdent(selectIdent)
#                queryobj.setSelectIdent(sqlparse.sql.IdentifierList(mytoklist))
#                queryclauses.getSelectIdent()
            
            elif (str(mytok[i].ttype) == 'Token.Keyword' and str(mytok[i].value) == "FROM"):
                fromIdent =  findIdentifierList(mytok[i+2])
                if (fromIdent is None): # Not found identifier list, have one table in from
                    fromIdent = mytok[i+2]
                else:
                    i=i+2
                queryobj.setFromIdent(fromIdent)
#                queryclauses.getFromIdent()
            
            elif (isinstance(mytok[i], sql.Where)): # Found the where clause
                queryobj.setWhereIdent((mytok[i]))
#                queryclauses.getWhereIdent()
            
            elif (str(mytok[i].ttype) == 'Token.Keyword' and str(mytok[i].value) == "GROUP"):
                if (str(mytok[i+2].ttype) == 'Token.Keyword' and str(mytok[i+2].value) == "BY"):
                    i=i+2
                    i = incrementIfWhitespace(mytok[i+1],i)
                    
                    groupbyIdent = findIdentifierList(mytok[i+1])
#                    print groupbyIdent
                    if (groupbyIdent is None): # Not found identifier list, have one group by attribute
                        groupbyIdent = mytok[i+1]
                    
                    queryobj.setGroupbyIdent(groupbyIdent)
                i+=1
            
            elif (str(mytok[i].ttype) == 'Token.Keyword' and str(mytok[i].value) == "ORDER"):
                if (str(mytok[i+2].ttype) == 'Token.Keyword' and str(mytok[i+2].value) == "BY"):
                    i=i+3#Go past order by
                    mytoklist = []
                    foundAttr = False
                    while (i <mytoklen and foundAttr is False):
                        (foundAttr, mytoklist) = findIdentifierListWithKeywords(mytok[i],mytoklist)
                        if (foundAttr):
                            i-=2
                            break;
                        i+=1# this is the where loop at line 94
                    queryobj.setOrderbyIdent(sqlparse.sql.IdentifierList(mytoklist))
#                    queryclauses.getOrderbyIdent()
                i+=1
                
            elif (str(mytok[i].ttype) == 'Token.Keyword' and str(mytok[i].value) == "HAVING"):
                i = incrementIfWhitespace(mytok[i+1],i)
                mytoklist = []
                foundAttr = False
                while (i <mytoklen and foundAttr is False):
                    (foundAttr, mytoklist) = findIdentifierListWithKeywords(mytok[i],mytoklist)
                    if (foundAttr):
                        i-=2
                        break;
                    i+=1# Closing where loop at line 218    
                queryobj.setHavingIdent(sqlparse.sql.IdentifierList(mytoklist))
#                queryclauses.getHavingIdent()
            i+=1
    return queryobj
#    print " ----------------------------Finished parsing the user query--------------------------"

def incrementIfWhitespace(tok, idx):
    if ((tok).is_whitespace()):
        return (idx+1)
    return idx            

# Checks to see if the token is an identifier List. If yes, it returns the identifiers else returns None
def findIdentifierList(token):
    if (isinstance(token,sql.IdentifierList)):
        mytoklist = []
        myidentlist = token
        ident =  sql.IdentifierList.get_identifiers(myidentlist)
#        for id in ident:
#            mytoklist.append(id)
#        print"TOkkkie"
#        print mytoklist
        return ident
    else:
        return None

# To account for aggregates, logical/comparison operators and other such keywords.
# Used for Select and Order by Clauses 
def findIdentifierListWithKeywords(token,mytoklist):
    foundAttr = False
    curr = token
    foundAggregate = False
#    print "TOken: %s" %token
    if (curr.ttype is None):
#        print "Im on the None: %s"%curr
        mytoklist.append(curr)
    elif (curr.ttype is Token.Keyword):
        if (myhelper.isAggregate(curr)):
            mytoklist.append(curr)
            foundAggregate = True
        elif (myhelper.isLogicalOperator(curr)):
            mytoklist.append(curr)
        elif (myhelper.isOrderbyOperator(curr)):
            mytoklist.append(curr)
        else:
            foundAttr = True
    elif (curr.ttype is Token.Punctuation):
        mytoklist.append(curr)
    else:
#        print "Im on the else: %s"%curr
        mytoklist.append(curr)
    return (foundAttr, mytoklist, foundAggregate)
    
def parseIdentifierList(attributes):
    print attributes
    print attributes[0].get_real_name()
    print attributes[0].get_alias()

if __name__ == "__main__":
    
    #Step 1: Get query from user
#    userInput = getUserInput()
    userInput = ("SELECT d.name, AVG (e.salary) "
              " FROM employee e, department d "
              " WHERE e.dept_id = d.id "
              " GROUP BY d.name, e.id ")
    
    userInput = ("SELECT d.name ")
    #Step 2: Tokenize the query give by the user
    (mytok, mytoklen) = tokenizeUserInput (userInput)
    
    #Step 3: Display the tokens in the user query
    displayTokens(mytok,mytoklen)
    
    #Step 4: Parse the user query using the tokens created
    queryclauses = myParser(mytok, mytoklen)
    
    #Step 5: Display the clauses in the user query
    queryclauses.dispay()   
    selectid = queryclauses.getSelectIdent() 
#    print myqueryconstructor.checkIfList(selectid)
    print queryclauses.getSelectContainsAggregate()
#    
##    ----------------------------------------------------------------------------------------------------------
#    
#    #Step 1: find the columns in group by clause
#    groupbyIdent = queryclauses.getGroupbyIdent()
#    
##    if (isinstance(groupbyIdent, sql.Identifier)):#d.name
##        print "I have a group by identifier"
##    else: #d.name, e.id 
##        print "I have a group by list"
#
#    #Step 2: find all the tables in the from clause
#    fromIdent = queryclauses.getFromIdent()
##    if (isinstance(fromIdent, sql.Identifier)):#d.name
##        print "I have a from identifier"
##    else: #d.name, e.id 
##        print "I have a from list"
#    
#    # Step 3: Find the distinct values of the group-by attribute
