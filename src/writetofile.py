#===============================================================================
# writetofile.py
# The function listed here write to file on disk.
#===============================================================================

#Creates a python script file called scripts.py comprising of the sub-queries
def createScript (queryList, numRows, selectAttr, addBigWhere):
    triplequote = (""" "" """).strip() + (""" " """).strip()
    
    if addBigWhere == " WHERE ":
        addBigWhere = ""

    filename = "scripts.py"
    FILE = open(filename,"w")
    if (not queryList or numRows == 0):
        FILE.write('Unknown problem with queries')
    else:   
        FILE.write("from datetime import datetime\n")
        FILE.write('import db_connection\n\n')
        FILE.write('db = db_connection.Db_connection()\n\n')
        FILE.write('db.clear_database() # reset the database on each run.\n\n')
        
        # Drop if exists the temp tables we are about to create
        FILE.write('# If the temp tables we are about to create exist, drop them!\n')
        FILE.write("db.make_query(" + triplequote + "drop table if exists finalOutputTable cascade" + triplequote + ")\n")
        iterations = 0
        while (iterations < numRows):
            drop = "db.make_query(" + triplequote + "drop table if exists temp" + str(iterations) + " cascade;" +triplequote + """)\n"""
            FILE.write(drop)
            iterations+=1        
        
        FILE.write('\n\n# Make a db call and run the sub queries that will collectively evaluate to the main query result\n')
        iterations = 0
        while(iterations <numRows):
            query = """db.make_query(""" + triplequote + queryList[iterations] + triplequote + """)\n"""
            FILE.write(query)
            iterations+=1
        
        FILE.write("\n#To find the union of the result sets\n")
        #first iterations is a SELECT INTO to create the table. subsequent are INSERT's
        iterations = 0
        while(iterations<numRows):
            if(iterations == 0):
                query = "db.make_query(" + triplequote + " SELECT * INTO finaloutputTable FROM temp" + str(iterations) + addBigWhere + triplequote + ")\n"
            else:
                query = "db.make_query(" + triplequote + " INSERT INTO finaloutputTable  SELECT * FROM temp" + str(iterations) + addBigWhere + triplequote + ")\n"
            FILE.write(query)    
            iterations+=1
        
        FILE.write("\n#Final query\n")
        FILE.write("db.make_pquery(" + triplequote + "SELECT " + selectAttr + " FROM finalOutputTable " + triplequote + ")\n")
    FILE.close()
    print "scripts created!"



#Creates a python script file called scriptsCost.py comprising of the sub-queries
def createScriptWithCosts (queryList, numRows, selectAttr, addBigWhere):
    triplequote = (""" "" """).strip() + (""" " """).strip()
    
    if addBigWhere == " WHERE ":
        addBigWhere = ""

    filename = "scriptsCost.py"
    FILE = open(filename,"w")
    if (not queryList or numRows == 0):
        FILE.write('Unknown problem with queries')
    else:
        FILE.write('import db_connection\n\n')
        FILE.write('totalCost = 0\n')
        FILE.write('db = db_connection.Db_connection()\n\n')
        FILE.write('db.clear_database() # reset the database on each run.\n\n')
        
        # Drop if exists the temp tables we are about to create
        FILE.write('# If the temp tables we are about to create exist, drop them!\n')
        FILE.write("db.make_query(" + triplequote + "drop table if exists finalOutputTable cascade" + triplequote + ")\n")
        iterations = 0
        while (iterations < numRows):
            drop = "db.make_query(" + triplequote + "drop table if exists temp" + str(iterations) + " cascade;" +triplequote + """)\n"""
            FILE.write(drop)
            iterations+=1        
        
        FILE.write('\n\n# Make a db call and run the sub queries that will collectively evaluate to the main query result\n')
        iterations = 0
        while(iterations <numRows):
            query = triplequote + queryList[iterations] + triplequote
            FILE.write("""db.make_query(""" + query + """)\n""" )
            FILE.write("totalCost+= int(db.total_cost(" + query + "))\n\n")
            iterations+=1
        
        FILE.write("\n#To find the union of the result sets\n")
        #first iteration is a SELECT INTO to create the table. subsequent iterations are INSERT's
        iterations = 0
        while(iterations<numRows):
            if(iterations == 0):
                query = triplequote + " SELECT * INTO finaloutputTable FROM temp" + str(iterations) + addBigWhere + triplequote
            else:
                query = triplequote + " INSERT INTO finaloutputTable  SELECT * FROM temp" + str(iterations) + addBigWhere + triplequote
            FILE.write("db.make_query(" + query + ")\n")    
            FILE.write("totalCost+= int(db.total_cost("+ query + "))\n\n")
            iterations+=1
        
        FILE.write("\n#Final query\n")
        query = triplequote + "SELECT " + selectAttr +" FROM finalOutputTable " + triplequote
        FILE.write("db.make_pquery(" + query + ")\n")
        FILE.write("totalCost+= int(db.total_cost("+ query + "))\n")
        FILE.write("print 'Cost to execute script is: %s' %(totalCost)\n")
    FILE.close()
    print "scriptsCost created!"
