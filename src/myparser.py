#===============================================================================
# myparser.py
# Given a SQL query, the file is used to parse the query into its various
# components such that the attributes for each clause are returned in a
# read-able format.
# For each clause, returns an identifier, or an identifier list (see the
# sql-parse documentation for details)
# Handles the following keywords: SELECT, FROM, WHERE, GROUP BY, ORDER BY,
# HAVING and all aggregates.

#===============================================================================

import sqlparse
from sqlparse import sql
from sqlparse.tokens import *
import myhelper
import myqueryclauses

# The function is used to create token out of the three parts of the query plan.
# It returns tokens
def createUserInputObject(query, groupby, orderby):
    
    (mytok, mytoklen) = tokenizeUserInput (query)
#    displayTokens(mytok,mytoklen)
    queryobj = myParser(mytok, mytoklen)
    
    groupobj = None
    if (groupby):
        (mytok, mytoklen) = tokenizeUserInput (" GROUP BY " + groupby)
        #    displayTokens(mytok,mytoklen)
        groupobj = myParser(mytok, mytoklen)
    
    orderobj = None
    if (orderby):
        (mytok, mytoklen) = tokenizeUserInput (" ORDER BY " + orderby)
    #    displayTokens(mytok,mytoklen)
        orderobj = myParser(mytok, mytoklen)
    
    return (queryobj,groupobj,orderobj)

def tokenizeUserInput(userInput):
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

#===============================================================================
# The main parser function. This takes in tokens, number of tokens and iterates
# through the list to find SQL clauses and attributes. Returns an object of
# type: myqueryclauses.
#===============================================================================
def myParser(mytok, mytoklen):
    queryobj = myqueryclauses.myqueryclauses()
    i=0
    while(i <mytoklen):

        if (mytok[i].is_whitespace()):      
            i+=1
        else:    
            
            if (str(mytok[i].ttype) == 'Token.Keyword.DML' and str(mytok[i].value) == "SELECT"):
                i = incrementIfWhitespace(mytok[i+1], i)
                
#                selectIdent = findIdentifierList(mytok[i+1])
                selectIdent = None
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
                #For some obscure reason, this is the only clause that sq-parse seems to 
                #capture on its own.
                queryobj.setWhereIdent((mytok[i]))
#                queryclauses.getWhereIdent()
            
            elif (str(mytok[i].ttype) == 'Token.Keyword' and str(mytok[i].value) == "GROUP"):
                if (str(mytok[i+2].ttype) == 'Token.Keyword' and str(mytok[i+2].value) == "BY"):
                    i=i+2
                    
                    groupbyIdent = findIdentifierList(mytok[i+2])
                    if (groupbyIdent is None): # Not found identifier list, have one group by attribute
                        groupbyIdent = mytok[i+2]
                    
                    queryobj.setGroupbyIdent(groupbyIdent)
                i+=1
            
            elif (str(mytok[i].ttype) == 'Token.Keyword' and str(mytok[i].value) == "ORDER"):
                if (str(mytok[i+2].ttype) == 'Token.Keyword' and str(mytok[i+2].value) == "BY"):
                    i=i+2#Go past order by
                    i = incrementIfWhitespace(mytok[i+1],i)
                    i+=1
                    
                    mytoklist = []
                    foundAttr = False
                    while (i <mytoklen and foundAttr is False):
                        (foundAttr, mytoklist,foundAggregate) = findIdentifierListWithKeywords(mytok[i],mytoklist)
                        if (foundAggregate):
                            queryobj.setOrderbyContainsAggregate(foundAggregate)

                        if (foundAttr):
                            i-=2
                            break;
                        i+=1# From where loop at line 53
                    orderbyIdent = (sqlparse.sql.IdentifierList(mytoklist)).get_identifiers()
                    queryobj.setOrderbyIdent(orderbyIdent)
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
    
    #Find new Identity
    selectid = queryobj.getSelectIdent()
    groupbyid = queryobj.getGroupbyIdent()
    newIdent = findNewSelectIdent(selectid, groupbyid)
    queryobj.setNewSelectIdent(newIdent)
    
    return queryobj

def incrementIfWhitespace(tok, idx):
    if ((tok).is_whitespace()):
        return (idx+1)
    return idx            

# Checks to see if the token is an identifier List. If yes, it returns the identifiers else returns None
def findIdentifierList(token):
#    print"find identifierLIst"
    if (isinstance(token,sql.IdentifierList)):
#        mytoklist = []
        myidentlist = token
        ident =  sql.IdentifierList.get_identifiers(myidentlist)
#        print ident
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
#    print "I got into findidentifierListWIthKeywords: %s" %token
    foundAttr = False
    curr = token
    foundAggregate = False
#    print "TOken: %s" %token
    if (curr.ttype is None):
        if ("," in str(curr)):
            #After an aggregate is found, the clause is no longer an identifierList.
            commalist = str(curr).split(",")
#            commalist = str(curr).split()
            for  i in commalist:
                i = str(i).strip()
                itemtok = sql.Token(None,i)
                mytoklist.append(itemtok)
#            print commalist
        else:
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

## Used for the case where the select clause contains only aggregates.
##Parameters are of type: Identifier or IdentifierList
#def addSelectGroupbyAttributesIdents(selectid, groupbyid):
#    if (selectid and groupbyid):
#        newIdent = " SELECT "#sqlparse.sql.IdentifierList
#        if (myhelper.checkIfList(groupbyid)):
#            for gid in groupbyid:
#                newIdent+=str(gid) + ", "
#        else:
#            newIdent+=str(groupbyid) + ", "
#        
#        if(myhelper.checkIfList(selectid)):
#            for sid in selectid:
#                if(myhelper.isAggregate(sid)):
#                    newIdent+=str(sid) + " "
#                else:
#                    newIdent+=str(sid) + ", "
#        else:
#            newIdent+=str(selectid) + " "
#    
#        newIdent = newIdent.rstrip(", ")     
#    
#        (mytok, mytoklen) = tokenizeUserInput (newIdent)
#        myobj = myParser(mytok,mytoklen)
#        return myobj.getSelectIdent()
#    return None


# Used for the case where the select clause contains only aggregates.
#Parameters are of type: Identifier or IdentifierList
def findNewSelectIdent(selectid, groupbyid):
    
    if (selectid and groupbyid):
        newIdent = " SELECT "#sqlparse.sql.IdentifierList

        if(myhelper.checkIfList(selectid)):
            for sid in selectid:
                if(myhelper.isAggregate(sid)):
                    newIdent+=str(sid) + " "
                elif ("(" in str(sid)):
                    newIdent+=str(sid) + ", "
                elif ("AS" in str(sid)):
                    newIdent+=str(sid) + ", "
                else:
                    newIdent+=str(sid) + ", "
        else:
            newIdent+=str(selectid) + " "
        
        if (myhelper.checkIfList(groupbyid)):
            for gid in groupbyid:
                if (str(gid) not in newIdent):
                    newIdent+=str(gid) + ", "
        else:
            if (str(groupbyid) not in newIdent):
                newIdent+=str(groupbyid) + ", "
            
        newIdent = newIdent.rstrip(", ")     
        
        (mytok, mytoklen) = tokenizeUserInput (newIdent)
        myobj = myParser(mytok,mytoklen)
        return myobj.getSelectIdent()
    return None


#def parseIdentifierList(attributes):
#    print attributes
#    print attributes[0].get_real_name()
#    print attributes[0].get_alias()


if __name__ == "__main__":
    
#    userInput = ("SELECT MAX(e.id) "
#                 " FROM department d, employee e "
#                 " WHERE e.dept_id = d.id "
#                 " GROUP BY e.salary, e.dept_id "
#                 " ORDER BY e.dept_id ")
    
#    userInput = ("SELECT d.name, e.name, AVG(e.salary) "
#                 " FROM employee e, department d, employee_skill es "
#                 " WHERE e.dept_id = d.id and e.id = es.emp_id "
#                 " GROUP BY d.name,es.skill,e.name ")
    userInput = (" SELECT q1.sym, q1.days, q1.price - q2.price "
                 " FROM quotes as q1, quotes as q2 "
                 " WHERE q1.sym = q2.sym and q1.day = q2.day -1 ")
    
#    userInput = (" SELECT s.salary as sal, e.dept_id"
#                 " FROM department d, employee e "
#                 " WHERE e.dept_id = d.id "
#                 " GROUP BY e.id ")

    print"------------------------------------------------------------------"
    print userInput
    print"------------------------------------------------------------------"
    #Step 2: Tokenize the query give by the user
    (mytok, mytoklen) = tokenizeUserInput (userInput)
#    displayTokens(mytok, mytoklen)
    #Step 4: Parse the user query using the tokens created
    queryclauses = myParser(mytok, mytoklen)
    
    #Step 5: Display the clauses in the user query
    queryclauses.dispay()   
