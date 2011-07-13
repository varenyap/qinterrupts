import db_connection
import myhelper
import myparser

db = db_connection.Db_connection()

def constructSubSelects (selAttributes, distinctGrouupVals, tblsInQry,whereAttrList):
    if (selAttributes and distinctGrouupVals and tblsInQry and whereAttrList):
        queryTemptblMap = {} # to map the query executed and the new table created. 
        selColumns = ''
        containAggregate = False
        
        aggSelects = ''
        for selAtt in selAttributes:
            if (myhelper.isAggregate(selAtt)) :
                containAggregate = True
                aggSelects = aggSelects + str(selAtt[0])+'.'+str(selAtt[1])+','
        
        if (not containAggregate):
            for selAtt in selAttributes:
                selColumns = selColumns + str(selAtt[0])+'.'+str(selAtt[1])+' AS ' +str(selAtt[0])+'_'+str(selAtt[1])+','
        
        aggSelects = aggSelects.rstrip(",")
        selColumns = selColumns.rstrip(",")
        selectClause = 'SELECT '+ str(selColumns)
        fromClause = ' FROM '
        
#        print "line 253 - select clause -  %s" %selectClause
        for alias in tblsInQry.iterkeys():
            table = tblsInQry[alias]
            fromClause = fromClause + table+ ' '+ alias + ','
        fromClause = fromClause.rstrip(",")
        
        orgWhereClause = " WHERE "
        orgWhereAtt = ''
        for item in whereAttrList:
            orgWhereClause += str(item)
        orgWhereClause = orgWhereClause+ ' AND '
        
        # Collect queries with in to part
        qlist = []
        
        # Declare up to 4 lists to hold 3 distinct group by values
        distList1 = None
        distList2 = None
        distList3 = None
        distList4 = None
        
        # Declare up to 4 string variables to hold group by column names
        gByAtt1 = None
        gByAtt2 = None
        gByAtt3 = None
        aByAtt4 = None
        
        # Iterate the dictionary containing the group by column names and dist values and assign them to above variables
        for att in distinctGrouupVals.iterkeys():
            if (att is not None and len (att) >0 ):
                if (gByAtt1 is None and distList1 is None): # first iteration
                    gByAtt1 = att
                    distList1 = distinctGrouupVals[att]
                elif (gByAtt2 is None and distList2 is None): # second iteration
                    gByAtt2 = att
                    distList2 = distinctGrouupVals[att]
                elif (gByAtt3 is None and distList3 is None): # third iteration
                    gByAtt3 = att
                    distList3 = distinctGrouupVals[att]    
                elif (gByAtt4 is None and distList4 is None): # fourth iteration
                    gByAtt4 = att
                    distList4 = distinctGrouupVals[att]
        
        qlist = []
        if (distList1):
            for val1 in distList1 :
                whereClaus =  str(gByAtt1) + " = '" + str(val1[0]) + "'"
                tempTblName = str(gByAtt1)+"_"+str(val1[0])
                if (containAggregate):
                    selectClause = "SELECT '"+ str(val1[0])+"'::text as " + str(gByAtt1).replace('.','_') +"," + aggSelects
                if (distList2): #2
                    for val2 in distList2:
                        whereClaus =  str(gByAtt1) + " = '" + str(val1[0]) + "' AND " + str(gByAtt2) + " = '" + str(val2[0]) + "'"
                        tempTblName = str(gByAtt1)+"_"+str(val1[0]) + "_" + str(gByAtt2)+"_"+str(val2[0])
                        if (containAggregate):
                            selectClause = "SELECT '"+str(val1[0])+"'::text as " + str(gByAtt1).replace('.','_') +", '" +str(val2[0])+"'::text as " + str(gByAtt2).replace('.','_') +" , "+ aggSelects
                    
                        if (distList3): #3
                            for val3 in distList3:
                                whereClaus =  str(gByAtt1) + " = '" + str(val1[0]) + "'" + " AND " + str(gByAtt2) + " = '" + str(val2[0]) + "' AND " + str(gByAtt3) + " = '" + str(val3[0]) + "'"
                                tempTblName = str(gByAtt1)+"_"+str(val1[0]) + "_" + str(gByAtt2)+"_"+str(val2[0]) + "_" + str(gByAtt3)+"_"+str(val3[0])
                                if (containAggregate):
                                    selectClause = "SELECT '"+str(val1[0])+"'::text as " + str(gByAtt1).replace('.','_') +", '" +str(val2[0])+"'::text as " + str(gByAtt2).replace('.','_') + ", '"+str(val3[0])+"'::text as " + str(gByAtt3).replace('.','_') + " , "+ aggSelects
                    
                                if (distList4):  #4:
                                    for val4 in distList4:
                                        whereClaus =  str(gByAtt1) + " = '" + str(val1[0]) + "'" + " AND " + str(gByAtt2) + " = '" + str(val2[0]) + "' AND " + str(gByAtt3) + " = '" + str(val3[0]) + "' AND " + str(gByAtt4) + " = '" + str(val4[0]) + "'"
                                        tempTblName = str(gByAtt1)+"_"+str(val1[0]) + "_" + str(gByAtt2)+"_"+str(val2[0]) + "_" + str(gByAtt3)+"_"+str(val3[0]) + "_" + str(gByAtt4)+"_"+str(val4)
                                        if (containAggregate):
                                            selectClause = "SELECT '"+ str(val1[0])+"'::text as " + str(gByAtt1).replace('.','_') +", '" +str(val2[0])+"'::text as " + str(gByAtt2).replace('.','_') + ", '"+ str(val3[0])+"'::text as " + str(gByAtt3).replace('.','_') + ", '"+ str(val4[0])+"'::text as " + str(gByAtt4).replace('.','_') + " , " + aggSelects
                                        handleTheQuery (selectClause,fromClause,whereClaus,tempTblName,orgWhereClause,qlist,queryTemptblMap)
                                else:   #4
                                    handleTheQuery (selectClause,fromClause,whereClaus,tempTblName,orgWhereClause,qlist,queryTemptblMap)
                        else: #3
                            handleTheQuery (selectClause,fromClause,whereClaus,tempTblName,orgWhereClause,qlist,queryTemptblMap)    
                else: #2
                   handleTheQuery (selectClause,fromClause,whereClaus,tempTblName,orgWhereClause,qlist,queryTemptblMap)
        
        retVal = []
        retVal.append(queryTemptblMap)
        retVal.append(qlist)      
        return retVal

def handleTheQuery (selectClause,fromClause,whereClaus,tempTblName,orgWhereClause,qlist,queryTemptblMap):
    tempTblName = tempTblName.replace(' ','_')
    tempTblName = tempTblName.replace('.','_')
    query = selectClause+' INTO '+ str(tempTblName) + str(fromClause) + str(orgWhereClause+whereClaus)      
    qlist.append(query)
    query = selectClause + fromClause + orgWhereClause+whereClaus
    queryTemptblMap [tempTblName] = query

def constructBigQueryResult (subSelects):
    if (subSelects):
        dictSS = subSelects [0]
        tempTables = []
        if (dictSS):
            bigQuery = ''
            union = " UNION "
            for subSelect in dictSS.iterkeys():
                bigQuery += "SELECT * FROM "+ subSelect + union
                tempTables.append(subSelect)
                
            bigQuery = bigQuery[:-6]
            writeToFile (subSelects[1],bigQuery,tempTables)
            
            #To delete
            #result = db.allrows(bigQuery)
            #return result

#Purpose: Write the parameters passed in to the method in to a python script file called scripts.py
def writeToFile (subSelects,bigQuery,tempTables):
    triplequote = (""" "" """).strip() + (""" " """).strip()
    
    filename = "scripts.py"
    FILE = open(filename,"w")
    if (not subSelects or len (bigQuery) == 0):
        FILE.write('Unknown problem with queries')
    else:   
        FILE.write('import db_connection\n\n')
        FILE.write('db = db_connection.Db_connection()\n\n')
        FILE.write('db.clear_database() # reset the database on each run.\n\n')
        
        
        # Drop if exists the temp tables we are about to create
        FILE.write('# Drop if exists the temp tables we are about to create\n')
        for tempTble in tempTables:
            drop = """db.make_query(""" + triplequote + "drop table if exists " + tempTble + " cascade;" +triplequote + """)\n"""
            FILE.write(drop)        
        
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