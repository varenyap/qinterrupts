import sqlparse

def find_groupby_clause(sql):
    attr_list = ""
    
    parsedQuery = sqlparse.lexer.tokenize (sql)    
    # Convert the parsed query to a list
    parsedList = []
    for parsed in parsedQuery:
        parsedList.append(parsed)
    
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

def find_attr_clause(clause):
    if (clause):
        clause = clause.strip()
        attr_list = []
        splits = clause.split(",") # Split the attributes based on the comma

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
                
def main():
    query1 = ("SELECT q.sym, MAX(q.price) - MIN(q.price) as maxjump"
              " INTO q_max_overall"
              " FROM quotes q" 
              " GROUP BY q.sym, a.bb"
              " ORDER BY maxjump DESC;")    
    query2 = ("SELECT q.sym, GROUP BY q.sym, a.bb")    
    query3 = ("SELECT q.sym")    
    query4 = ("GROUP BY q.sym, a.bb")    
    query5 = (" GROUP BY q.sym, a.bb INTO q_max_overall")
    
    
    query6 = ("GROUP BY bb")
    query7 = ("SELECT Customer,OrderDate,SUM(OrderPrice) FROM Orders"
              " GROUP BY Customer, OrderDate")
    query8 = ("GROUP BY aa, q.stock")
    query9 = ("GROUP BY q.stock, aa")
    
    value = find_groupby_clause(query1)
    print "Printing group by clause:"
    print value
    attributes = find_attr_clause(value)
    print "Printing (table.attributes)"    
    
    if (attributes):
        for atr in attributes:
            print "%s.%s" %(atr[0],atr[1])
    else:
        print "No attributes"
    
    
if __name__ == "__main__":
    main()