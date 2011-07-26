import sqlparse
#import sqlparse.sql
#from sqlparse.sql import IdentifierList, Identifier
from sqlparse import sql
#from sqlparse import tokens as T
from sqlparse.tokens import *


def tester1():
    sql = 'foo(c1, c2)'    
    p = sqlparse.parse(sql)
    print p
    p = sqlparse.parse(sql)[0]
    print "p is: %s" %p
    mytok = p.tokens
    print "p.tokens is %s" %mytok
    for mt in mytok:
        print mt
    print "()()()()()()()()()()()()()()()()"
    query = "Select * from tablefoo"
    mytok = sqlparse.lexer.tokenize(sql)
    myidlist = sqlparse.sql.TokenList(query)
    
    tokenslist = myidlist.value
    print tokenslist
    print "()()()()()()()()()()()()()()()()"
    
    
    
#    print myidlist.token_first()
    
    
#    print mytok.get_identifiers()
    
#    col1 = p.tokens[0].tokens[1].tokens[1].tokens[0]
#    print "col1: %s" %col1.flatten()
#    
#    print "\n\n\n\n\n testing"
#    print p.tokens[0]
#    print p.tokens[0].tokens[1]
#    print p.tokens[0].tokens[1].tokens[1]
#    print p.tokens[0].tokens[1].tokens[1].tokens[0]
#    print col1.within(sqlparse.sql.Function)
#    print col1.within(sqlparse.sql.Identifier)

def tester2():
    sql = """Select * from tablefoobar"""
    
#    mytree = sqlparse.parse(sql) 
#    myt = sqlparse.parse(sql)[0]
#    print "--%s--" %myt.tokens[0]
#    print myt.tokens[0].within(sqlparse.sql.Identifier)
    mytokenlist = sqlparse.sql.TokenList(sqlparse.parse(sql)) # Creates a token list i think
    gen = mytokenlist.flatten()
    print gen
    
#    lgen = list(gen)
#    for g in lgen:
#        print g
   
    mysublist =  mytokenlist.get_sublists()
    print mytokenlist.token_first(ignore_whitespace=True)

def tester3():
    query = "select * from tablefoo"
    st=  sqlparse.parse(query)
    
    stmt = st[0]
    print stmt
    tok=  stmt.tokens
    i = 0
    for t in tok:
        print"%d %s: ~%s~" %(i,t.ttype,t.value)
        if (t.is_whitespace()):
            print "ALOHA"
        i+=1
    toklist = sqlparse.sql.TokenList(tok)
    print toklist.token_first(ignore_whitespace = True)
    print toklist.token_index(toklist.token_first())
    print toklist.token_next(0,skip_ws = True)

def tester4():
    query = "Select * from departments"
    st = sqlparse.parse(query)
    stmt =  st[0]
    print stmt.get_type()
    print stmt.tokens[4]
    query = "(col1, col2)"
    query = "(Select * from departments where id >2)"
    
    p = sqlparse.parse(query)[0]
    print "----------------------------------------------"
    if (p.tokens[0].tokens[1].is_child_of(p.tokens[0])):
        print p.tokens[0]
        print p.tokens[0].tokens[0]
        print p.tokens[0].tokens[1].ttype
        print p.tokens[0].tokens[2]
        print p.tokens[0].tokens[3]
        print p.tokens[0].tokens[4]
        print p.tokens[0].tokens[5].ttype
        print "YOU"
        

def tester5():
    query = "select foo from departments where id >4"
    mytokens = sqlparse.parse(query)[0]
    toknum = len(mytokens.tokens) # Number of tokens in the query
    
    i= 0
    while (i <toknum):
        print mytokens.tokens[i]
        i+=1

    print "-------------------------------------"
    if (not mytokens.tokens[2].is_child_of(mytokens.tokens[0])):
        print "%s is not a child of %s " %(mytokens.tokens[2],mytokens.tokens[0])
    if (mytokens.tokens[2].is_child_of(mytokens)):
        print "%s is a child of %s " %(mytokens.tokens[2],mytokens)
    
def tester6():
    query = "select * from foo where bar = 1 order by id desc"
    query = 'select x from (select y from foo where bar = 1) z'

    parsed = sqlparse.parse(query)[0]
    print parsed
    mytok = parsed.tokens
    print mytok
    mylen = len(mytok)
    print "-------------------------------------------"
    if (isinstance(mytok[-3].tokens[-2], sql.Where)):
        print mytok[-7]
        print mytok[-3]
        print mytok[-3].tokens
        print mytok[-3].tokens[-2]
        print "Well thats true"
        
    p = sqlparse.parse("select *, null, 1, 'foo', bar from mytable, x")[0]
    if(isinstance(p.tokens[2], sql.IdentifierList)):
        print "IS an identifier list"
        print p.tokens[2]
#        l = p.tokens[2]
#        self.assertEqual(len(l.tokens), 13)

def tester7():
    print "This is my main tester" # HAVING CLAUSE?
    query = (""" Select e.id, d.id """
             """ from department as d, employee e """
             """ where d.dept_id>4 and (laugh = False or food=True)"""
             """ group by d.id, e.id """
             """ order by d.id DESC, e.id""")
    
#    query = (""" order by d.id DESC, e.id """
#             """ group by d.id, e.id """)
    
    myquery = sqlparse.format(query,keyword_case = 'upper', identifier_case = 'lower', strip_comments = True)
    mystmt = sqlparse.parse(myquery)[0]
#    print "Statement: %s" %mystmt
    mytok = mystmt.tokens
    mytoklen = len(mytok)
    
    i= 0
    while (i <mytoklen):
        print "%d: ~~%s~~ Type is: '%s' Value is: ~%s~" %(i, mytok[i], mytok[i].ttype, mytok[i].value)
        i+=1
    print " ----------------------------Finished printing the 'tokenized' query-----------------------"
    selectIdent = None
    fromIdent = None
    whereIdent = None
    groupbyIdent = None
    orderbyIdent = None
    
    i=0
    while(i <mytoklen):

        if (mytok[i].is_whitespace()):      
            i+=1
        else:    
            if (str(mytok[i].ttype) == 'Token.Keyword.DML' and str(mytok[i].value) == "SELECT"):
                if (mytok[i+1].is_whitespace()):
                    i+=1
                
                selectIdent = findIdentifierList(mytok[i+1]) #Check for identifier List
#                selectIdent[0].get_real_name()
                if (selectIdent is None): # Not found identifier list
                    if (str(mytok[i+1].value) is "*"): # Found a wildcard
                        selectIdent = mytok[i+1]
                    else: # Have one select attribute
                        selectIdent = mytok[i+1]
                i+=1
                print "SELECT IDENTIFIERS:^^^^^^%s^^^^^"%selectIdent
            elif (str(mytok[i].ttype) == 'Token.Keyword' and str(mytok[i].value) == "FROM"):
                print "I found a from"
                fromIdent =  findIdentifierList(mytok[i+2])
                i=i+2
                print "FROM IDENTIFIERS:^^^^^^%s^^^^^"%fromIdent
            elif (isinstance(mytok[i], sql.Where)): # Found the where clause
                print'found a where'
                whereIdent = mytok[i]
                print "WHERE IDENTIFIERS:^^^^^^%s^^^^^"%whereIdent
            elif (str(mytok[i].ttype) == 'Token.Keyword' and str(mytok[i].value) == "GROUP"):
                if (str(mytok[i+2].ttype) == 'Token.Keyword' and str(mytok[i+2].value) == "BY"):
                    i=i+2
                    print "found GROUP BY"
                    if (mytok[i+1].is_whitespace()):
                        i+=1
                    groupbyIdent = findIdentifierList(mytok[i+1])
                    if (groupbyIdent is None): # Not found identifier list, have one group by attribute
                        groupbyIdent = mytok[i+1]
                    print "GROUP BY IDENTIFIERS:^^^^^^%s^^^^^"%groupbyIdent
                i+=1
            elif (str(mytok[i].ttype) == 'Token.Keyword' and str(mytok[i].value) == "ORDER"):
                if (str(mytok[i+2].ttype) == 'Token.Keyword' and str(mytok[i+2].value) == "BY"):
                    i=i+3#Go past order by
                    print "found ORDER BY"

                    mytoklist = []
                    foundAttr = False
                    while (i <mytoklen and foundAttr is False):
                        curr = mytok[i]
                        if (curr.ttype is None):
                            mytoklist.append(curr)
                        elif (mytok[i].ttype is Token.Keyword):
                            curr = mytok[i]
                            if((curr.match(Keyword, 'DESC')) or curr.match(Keyword,'ASC')):
                                mytoklist.append(curr)
                            else:
                                foundAttr = True
                        elif (mytok[i].ttype is Token.Punctuation):
                            mytoklist.append(curr)
                        else:
                            mytoklist.append(curr)
                        if (foundAttr):
                            i-=2
                            break;
                        i+=1# From where loop at line 218    
                    orderbyIdent = sqlparse.sql.IdentifierList(mytoklist)
                    print "ORDER BY IDENTIFIERS:^^^^^^%s^^^^^"%orderbyIdent
                i+=1 #FRom the order by order by elif
            i+=1

# Checks to see if the token is an identifier List. If yes, it returns the identifiers else returns None
def findIdentifierList(token):
#    print "TOKEN %s" %token
    if (isinstance(token,sql.IdentifierList)):
            myidentlist = token
            ident =  sql.IdentifierList.get_identifiers(myidentlist)
            myidentlen = len(ident)
#            print " ~~%s~~ Len is: %d" %(myidentlist,myidentlen) 
            return ident
    else:
        return None

def parseIdentifierList(attributes):
    print "Im in function parseIdentifierList"
    print attributes
    print attributes[0].get_real_name()
    print attributes[0].get_alias()

def tester8():
    
    query = " order by id "
    p = sqlparse.parse(query)[0]
    tok = p.tokens
    
    t1 = sql.Token(Keyword, 'foo')
    t2 = sql.Token(Punctuation, ',')
    x = sql.TokenList([t1, t2])
    
    print x.token_matching(0, [lambda t: t.ttype is Keyword]),t1
    desc_token = sql.Token(Keyword,'DESC')
    
    print "----------------------------------------------"
    
    for t in tok:
        print "%s: %s" %(t.ttype,t.value)
        print t
        
#        if ((tokens.token_next_match(0,[lambda t: t.ttype is Keyword],[lambda t: t.value is 'DESC']),desc_token)):
#            print "DESC TOKEN"
        
    
    
#    if (p.tokens[0].tokens[1].is_child_of(p.tokens[0])):
#        print p.tokens[0]
#        print p.tokens[0].tokens[0]
#        print "YOU"
    

if __name__ == "__main__":
#    tester1()
#    tester2()
#    tester3() #INTERESTING
#    tester4() #INTERESTING
#    tester5() #INTERESTING
#    tester6() #REALLY INTERESTING
    tester7()
#    tester8()