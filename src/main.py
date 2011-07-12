import db_connection
import myparser
import myqueryconstructor

db = db_connection.Db_connection()

def main():
    
    db.clear_database() # reset the database on each run. 
    
    query1 = ("SELECT d.name, AVG (e.salary) "
              " FROM employee e, department d "
              " WHERE e.dept_id = d.id and e.dept_od = f.piece or d.dept_id = 89 "
              " GROUP BY d.name")
    
    parsed_list = myparser.parse_sql_as_list(query1) #Parse sql into a walk-able list         

    # 1. find the columns in group by clause
    grpByCols = myparser.find_groupby_clause(parsed_list)
#    print "--%s--" %grpByCols
    
    # 2. split the group by attributes to table alias and column name
    attributes = myparser.find_attr_clause(grpByCols,",") # List of tuples t[0] = table alias t[1] = column name
    
    # 3. find all the tables in the query
    tblsInQry = myparser.find_tables(parsed_list) # A dictionary of table names and aliases - key alias, value table name
    
    # 4. construct the list of distinct values for the attributes in group by clause
    distinctGrouupVals = myparser.find_distinct_group_by_values (tblsInQry,attributes) # dictionary with tableAlias.colums as key and a list of distinct values for that column
    
    # 5. find the columns in the select clause
    selectCols = myparser.find_select_columns (parsed_list)
    
    # 6. split the select attributes to table alias and column name
    selAttributes = myparser.find_attr_clause(selectCols,",") # List of tuples t[0] = table alias t[1] = column name
    
    # 7. construct the sub selects for each distinct value represented in the group by clause
    subSelects = myqueryconstructor.constructSubSelects (selAttributes, distinctGrouupVals, tblsInQry) # dictionary having the temp table as key and the query for that table as value
    
    #8. Union the small queries to evaluate the big query
    queryResults = myqueryconstructor.constructBigQueryResult(subSelects)
    print" Results: \n\n%s" %queryResults
        
#    db.display_schema()
    
if __name__ == "__main__":
    main()