import sqlparse

def find_groupby_clause(sql):
    attr_list = []
    
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
                # At this point, found GROUP BY
                i+=2 # Now past the tokens for 'GROUP BY'
                while (i < length and foundAttr is False):
                    if (str(parsedList [i][0]) != 'Token.Keyword'):
                        attr_list.append(parsedList[i][1])
                        i+=1
                    else:
                        foundAttr = True                    
            break;
        if (foundAttr):
            break;
               
    if (foundGroup):
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
    
    
    value = find_groupby_clause(query3)
    if (value is False):
        print  "No group clause"
    else:
        print value
    
    
if __name__ == "__main__":
    main()
