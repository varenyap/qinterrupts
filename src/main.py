import db_connection
import myqueryclauses
import  myhelper
import myparser
import myqueryconstructor

if __name__ == "__main__":
    
    #Step 1: Get query from user
#    userInput = getUserInput()
    
    userInput = ("SELECT e.dept_id, MAX(e.salary) "
                 " FROM employee e, department d "
                 " GROUP BY e.dept_id "
                 " ORDER BY e.dept_id DESC")
    
    userInput = ("SELECT e.id, e.dept_id, MAX(e.salary) "
                 " FROM employee e, department d "
                 " GROUP BY e.id, e.dept_id "
                 " ORDER BY e.dept_id DESC")
    
    #Step 2: Tokenize the query give by the user
    (mytok, mytoklen) = myparser.tokenizeUserInput (userInput)

    #Step 3: Display the tokens in the user query
#    displayTokens(mytok,mytoklen)
    
    #Step 4: Parse the user query using the tokens created
    queryclausesobj = myparser.myParser(mytok, mytoklen)
    
    #Step 5: Display the clauses in the user query
#    queryclausesobj.dispay()
    
    #############################################################################################    
    print "------------------------------------------------------------------------------------"
    print userInput
    print "------------------------------------------------------------------------------------"

    #Step 1: Find the distinct values of the group-by attribute
    distinctGroupbyValues = myqueryconstructor.findDistinctGroupbyValues(queryclausesobj)
    print distinctGroupbyValues
    
    #Step 2: construct the sub selects for each distinct value represented in the group by clause
    # dictionary having the temp table as key and the query for that table as value 
    subSelects = myqueryconstructor.constructSubSelects (queryclausesobj, distinctGroupbyValues)

    # Step 3: Union the small queries to evaluate the big query
    myqueryconstructor.constructBigQuery(subSelects)
    
    print "Script created!"



    
    
