import db_connection
import myqueryclauses
import  myhelper
import myparser
import myqueryconstructor

if __name__ == "__main__":
    
    #Step 1: Get query from user
#    userInput = getUserInput()
#    userInput = (" SELECT d.id, MAX (e.salary)"
#              " FROM department d, employee e "
#              " WHERE e.dept_id = d.id "
#              " GROUP BY d.id ")
    
#    userInput = ("SELECT e.name "
#              " FROM department d, employee e "
#              " GROUP BY e.name ")   
    
    userInput = (" SELECT e.id, MAX(e.salary) "
                 " FROM employee e "
                 " group by e.id ") 
#    
#    userInput = ("SELECT d.name, e.name "
#              " FROM employee e, department d "
#              " WHERE e.dept_id = d.id"
#              " GROUP BY d.name, e.name")    
#    
#    #does not work
#    userInput = (" SELECT AVG(e.salary) "
#                 " FROM department d, employee e "
#                 " GROUP BY e.dept_id " )

#    userInput = ("SELECT d.name, e.name, MAX(e.salary) "
#              " FROM employee e, department d "
#              " WHERE e.dept_id = d.id"
#              " GROUP BY d.name, e.name")
#    
#    userInput = ("SELECT d.name, e.name, AVG(e.salary) "
#              " FROM employee e, department d, employee_skill es"
#              " WHERE e.dept_id = d.id and e.id = es.emp_id"
#              " GROUP BY d.name,es.skill,e.name")
#
#    userInput = (" SELECT d.name, es.skill, MAX (e.salary) " 
#              " FROM employee e, department d, employee_skill es "
#              " WHERE e.dept_id = d.id and es.emp_id = e.id "
#              " GROUP BY d.name, es.skill " )
    
    #Step 2: Tokenize the query give by the user
    (mytok, mytoklen) = myparser.tokenizeUserInput (userInput)

    #Step 3: Display the tokens in the user query
#    displayTokens(mytok,mytoklen)
    
    #Step 4: Parse the user query using the tokens created
    queryclausesobj = myparser.myParser(mytok, mytoklen)
    
    #Step 5: Display the clauses in the user query
    queryclausesobj.dispay()
    #############################################################################################    

    #Step 1: Find the distinct values of the group-by attribute
    distinctGroupbyValues = myqueryconstructor.findDistinctGroupbyValues(queryclausesobj)
    
    #Step 2: construct the sub selects for each distinct value represented in the group by clause
    # dictionary having the temp table as key and the query for that table as value 
    subSelects = myqueryconstructor.constructSubSelects (queryclausesobj, distinctGroupbyValues)

    # Step 3: Union the small queries to evaluate the big query
    myqueryconstructor.constructBigQuery(subSelects)
    
    print "Script created!"



    
    

