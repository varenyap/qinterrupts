import db_connection
import myhelper
import myparser

db = db_connection.Db_connection()

def constructSubSelects (selAttributes, distinctGrouupVals, tblsInQry):
    if (selAttributes and distinctGrouupVals and tblsInQry):
        queryTemptblMap = {}
        selColumns = ''
        containAggregate = False
        
        for selAtt in selAttributes:
            if (myhelper.isAggregate(selAtt)) :
                containAggregate = True
                selColumns = selColumns + str(selAtt[0])+'.'+str(selAtt[1])+','
        
        if (not containAggregate):
            for selAtt in selAttributes:
                selColumns = selColumns + str(selAtt[0])+'.'+str(selAtt[1])+','
        
        selColumns = selColumns.rstrip(",")
        selectClause = 'SELECT '+ str(selColumns)
        fromClause = ' FROM '
        
#        print "line 253 - select clause -  %s" %selectClause
        for alias in tblsInQry.iterkeys():
            table = tblsInQry[alias]
            fromClause = fromClause + table+ ' '+ alias + ','
        fromClause = fromClause.rstrip(",")
        
        # Colletct queries with in to part
        qlist = []
        for val in distinctGrouupVals.iterkeys():
            tempDistVals = distinctGrouupVals[val]
            for distVal in tempDistVals:
                whereClaus = ' WHERE '+ str(val)+ '=' + "'"+str(distVal[0])+"'"
                tempTblName = str(val)+"_"+str(distVal[0])
                tempTblName = tempTblName.replace('.','_')
                
                # SELECT AVG  (e.salary),'COSI' INTO d_name_COSI FROM employee e,department d WHERE d.name='COSI'
                
                if (containAggregate):
                    query = selectClause+ ",'"+ str(distVal[0]) +"'"+' INTO '+ str(tempTblName) + str(fromClause) + str(whereClaus)
                else:
                    query = selectClause+' INTO '+ str(tempTblName) + str(fromClause) + str(whereClaus)
                
                qlist.append(query)
                print "line 274: %s" %query
                
                #To delete
                db.make_query(query)# make a db call and run the insert the query
                
                if (containAggregate):
                    #ALTER TABLE name_cosi ALTER COLUMN "?column?" TYPE varchar(20);
                    alterQuery = "ALTER TABLE " + str(tempTblName) + """ ALTER COLUMN "?column?" TYPE VARCHAR(20); """
                    db.make_query(alterQuery)
                    qlist.append(alterQuery)
                
                # construct the query without in to part to persist which temp table contains which query result
                if (containAggregate):
                    query = selectClause + ",'"+str(distVal[0])+"'" + fromClause + whereClaus
                    queryTemptblMap [tempTblName] = query
                else:
                    query = selectClause + fromClause + whereClaus
                    queryTemptblMap [tempTblName] = query
        
        retVal = []
        retVal.append(queryTemptblMap)
        retVal.append(qlist)        
        return retVal

def constructBigQueryResult (subSelects):
    if (subSelects):
        dictSS = subSelects [0]
        if (dictSS):
            bigQuery = ''
            union = " UNION "
            for subSelect in dictSS.iterkeys():
                bigQuery += "SELECT * FROM "+ subSelect + union
                
            bigQuery = bigQuery[:-6]
            writeToFile (subSelects[1],bigQuery)
            
            #To delete
            result = db.allrows(bigQuery)
            print "line 306: %s" %bigQuery
            return result

#Purpose: Write the parameters passed in to the method in to a python script file called scripts.py
def writeToFile (subSelects,bigQuery):
    triplequote = (""" "" """).strip() + (""" " """).strip()
    
    filename = "scripts.py"
    FILE = open(filename,"w")
    if (not subSelects or len (bigQuery) == 0):
        FILE.write('Unknown problem with queries')
    else:   
        FILE.write('import db_connection\n\n')
        FILE.write('db = db_connection.Db_connection()\n\n')
        FILE.write('db.clear_database() # reset the database on each run.\n\n')
        FILE.write('# Make a db call and run the sub queries that will collectively evaluate to the main query result\n')
        for subSelect in subSelects:
            query = """db.make_query(""" + triplequote + subSelect + triplequote + """)\n"""
            FILE.write(query)        
        
        FILE.write('\n# The query that combines the results of small queries\n')
        FILE.write('bigQuery = '+triplequote + bigQuery+ triplequote + '\n\n')
        FILE.write('db.make_pquery(bigQuery)\n')
    FILE.close()
                
#def main():
#    
#    db.clear_database() # reset the database on each run. 
#    
#    query1 = ("SELECT d.name, AVG (e.salary) "
#              " FROM employee e, department d "
#              " WHERE e.dept_id = d.id and e.dept_od = f.piece or d.dept_id = 89 "
#              " GROUP BY d.name")
#    
#    parsed_list = myparser.parse_sql_as_list(query1) #Parse sql into a walk-able list         
#
#    # 1. find the columns in group by clause
#    grpByCols = myparser.find_groupby_clause(parsed_list)
##    print "--%s--" %grpByCols
#    
#    # 2. split the group by attributes to table alias and column name
#    attributes = myparser.find_attr_clause(grpByCols,",") # List of tuples t[0] = table alias t[1] = column name
#    
#    # 3. find all the tables in the query
#    tblsInQry = myparser.find_tables(parsed_list) # A dictionary of table names and aliases - key alias, value table name
#    
#    # 4. construct the list of distinct values for the attributes in group by clause
#    distinctGrouupVals = myparser.find_distinct_group_by_values (tblsInQry,attributes) # dictionary with tableAlias.colums as key and a list of distinct values for that column
#    
#    # 5. find the columns in the select clause
#    selectCols = myparser.find_select_columns (parsed_list)
#    
#    # 6. split the select attributes to table alias and column name
#    selAttributes = myparser.find_attr_clause(selectCols,",") # List of tuples t[0] = table alias t[1] = column name
#    
#    # 7. construct the sub selects for each distinct value represented in the group by clause
#    subSelects = constructSubSelects (selAttributes, distinctGrouupVals, tblsInQry) # dictionary having the temp table as key and the query for that table as value
#    
#    #8. Union the small queries to evaluate the big query
#    queryResults = constructBigQueryResult(subSelects)
#    print" Results: \n\n%s" %queryResults
#        
##    db.display_schema()
#    
#if __name__ == "__main__":
#    main()