import db_connection
import myqueryclauses
import  myhelper
import myparser
import myqueryconstructor

if __name__ == "__main__":
    
    #Step 1: Get query from user
#    userInput = getUserInput()
    userInput = ("SELECT d.name, AVG (e.salary) "
              " FROM employee e, department d"
              " WHERE e.dept_id = d.id"
              " GROUP BY d.name, e.id")
    
    #Step 2: Tokenize the query give by the user
    (mytok, mytoklen) = myparser.tokenizeUserInput (userInput)

    #Step 3: Display the tokens in the user query
#    displayTokens(mytok,mytoklen)
    
    #Step 4: Parse the user query using the tokens created
    queryclauses = myparser.myParser(mytok, mytoklen)
    
    #Step 5: Display the clauses in the user query
    queryclauses.dispay()
    #############################################################################################    

    #Step 1: find the columns in group by clause
    groupbyIdent = queryclauses.getGroupbyIdent()
    
    #Step 2: find all the tables in the from clause
    fromIdent = queryclauses.getFromIdent()

    # Step 3: Find the distinct values of the group-by attribute
    distinctGroupbyValues = myqueryconstructor.findDistinctGroupbyValues(groupbyIdent, fromIdent)
    
    #Step 4: 
    # dictionary having the temp table as key and the query for that table as value
    subSelects = myqueryconstructor.constructSubSelects (selAttributes, distinctGrouupVals, tblsInQry,where_attr_list)
    

