import myparser
import myqueryconstructor

def getUserInput():    
    userInput = ""
    query = ""
    entry = raw_input("Enter query, 'done' on its own line to quit: \n")
    while entry.lower() != "done":
        query+=str(entry)
        query+=" "
        entry = raw_input("")
    
    groupby = ""
    entry = raw_input("Enter GROUP BY clause, 'done' on its own line to quit: \n")
    while entry.lower() != "done":
        groupby+=str(entry)
        groupby+=" "
        entry = raw_input("")
    
    orderby = ""
    entry = raw_input("Enter ORDER BY clause, 'done' on its own line to quit: \n")
    while entry.lower() != "done":
        orderby+=str(entry)
        orderby+=" "
        entry = raw_input("")
    
    userInput = query + ' ' + groupby + ' ' + orderby 
    return userInput


if __name__ == "__main__":
    
    #Step 1: Get query from user
#    userInput = getUserInput()
#    print userInput
    
#    userInput = ("SELECT e.dept_id, MAX(e.salary) "
#                 " FROM employee e, department d "
#                 " GROUP BY e.dept_id "
#                 " ORDER BY e.dept_id")
    
#    userInput = ("SELECT e.id, e.dept_id "
#                 " FROM employee e "
#                 " GROUP BY e.id, e.dept_id "
#                 " ORDER BY e.dept_id DESC")

#    userInput = (" SELECT e.dept_id, d.id, MAX(e.salary) "
#                 " FROM employee e, department d "
#                 " GROUP BY e.dept_id, d.id "
#                 " ORDER BY MAX(e.salary) ")
    
#    userInput = (" SELECT e.dept_id, MAX(e.salary) "
#                 " FROM employee e, department d "
#                 " GROUP BY e.dept_id " 
#                 " ORDER BY MAX(e.salary) DESC")
    
    userInput = ("SELECT d.name, e.name, AVG(e.salary) "
                 " FROM employee e, department d, employee_skill es "
                 " WHERE e.dept_id = d.id and e.id = es.emp_id "
                 " GROUP BY d.name,es.skill,e.name "
                 " ORDER BY es.skill ")
#    
#    userInput = ("SELECT d.name, e.name, AVG(e.salary) "
#                 " FROM employee e, department d, employee_skill es "
#                 " WHERE e.dept_id = d.id and e.id = es.emp_id "
#                 " GROUP BY d.name,es.skill,e.name "
#                 " ORDER BY e.name DESC")

#    userInput = ("SELECT e.id, e.dept_id, MAX(e.salary) "
#                 " FROM employee e, department d "
#                 " GROUP BY e.id, e.dept_id "
#                 " ORDER BY e.dept_id DESC, e.id ASC")

#    userInput = ("SELECT e.id, e.dept_id, MAX(e.salary) "
#                 " FROM employee e, department d, employee_skill es "
#                 " GROUP BY e.id, e.dept_id, es.skill "
#                 " ORDER BY e.dept_id DESC, e.id ASC, es.skill, MAX(e.salary) " )
    
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
#    print distinctGroupbyValues
    
    #Step 2: construct the sub selects for each distinct value represented in the group by clause
    # dictionary having the temp table as key and the query for that table as value 
    subSelects = myqueryconstructor.constructSubSelects (queryclausesobj, distinctGroupbyValues)

    # Step 3: Union the small queries to evaluate the big query
    myqueryconstructor.constructBigQuery(subSelects)
    
    print "Script created!"



    
    
