import sqlparse
sql = ("SELECT q.sym, MAX(q.price) - MIN(q.price) as maxjump"
              " INTO q_max_overall"
              " FROM quotes q" 
              " GROUP BY q.sym, a.bb"
              " ORDER BY maxjump DESC;")


parsedQuery = sqlparse.lexer.tokenize (sql)

# Convert the parsed query to a list
parsedList = []
for parsed in parsedQuery:
	parsedList.append(parsed)	

i = 0
foundGroup = False
for parsed in parsedList:
	i += 1
	if (str(parsed[0]) == 'Token.Keyword'):
		if (str(parsed[1]) == 'GROUP'):
			foundGroup = True
	if (foundGroup):
		i += 1
		while (True):
			if (str(parsedList [i+1][0]) == 'Token.Keyword' and str(parsedList [i][1]) != 'BY'):
				foundGroup = False
				break
			if (str(parsedList [i][0]) != 'Token.Keyword'):
				print (parsedList [i][1])
			i += 1