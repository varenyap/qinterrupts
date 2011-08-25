import psycopg2
import config

init_script_path = config.init_script_path
db_name = config.db_name
db_user = config.db_user
db_password = config.db_password

class Db_connection:
    cursor =""
    conn =""

    def __init__(self):
        global db_name
        global db_password
        global db_user
        global init_script_path
        self.cursor =""
        self.conn = ""

    #Open DB connection
    def openConnection(self):
        try:
            connect_str = "dbname="+ db_name + " user=" + db_user + " password=" + db_password
            self.conn = psycopg2.connect(connect_str)
        except:
            print "unable to connect to the database"
        self.cursor = (self.conn).cursor()
        
    #Close DB connection
    def closeConnection(self,conn,cursor):
        (self.cursor).close()
        (self.conn).close()
        self.cursor = None
        self.conn = None
    
    #Clearing the database
    def reset_database(self,cursor):
        self.openConnection()
        
        for line in open(init_script_path):            
            (self.cursor).execute(line)
        
        (self.conn).commit()    
        self.closeConnection(self.conn,self.cursor)
                
    def clear_database(self):
        self.reset_database(self.cursor)

    # Use for select queries only so as to display results
    def make_pquery(self,query):
        if (query is not None):
            self.openConnection()
            row = ""
            
            (self.cursor).execute(query)        
            for row in self.cursor:
                print row
    
            value = 0
            if (len(row) > 0):
                value = 1
    
            self.closeConnection(self.conn,self.cursor)
            return value
        return None
    
    # Use for queries when the result need not be displayed to screen    
    def make_query(self,query):
        if (query is not None):
            self.openConnection()
            (self.cursor).execute(query)
            (self.conn).commit()    
    #        print query
            self.closeConnection(self.conn,self.cursor)

    # Use when the first result of the query is needed.
    def onerow(self,query):
        if (query is not None):
            self.openConnection()
            (self.cursor).execute(query)
            result = (self.cursor).fetchone()
            self.closeConnection(self.conn,self.cursor)
            if (result is not None):
                return result
        return None   
    
    #Use when you want all the results of the query
    def allrows(self,query):
        if (query is not None):
            self.openConnection()
            (self.cursor).execute(query)
            result = (self.cursor).fetchall()
            self.closeConnection(self.conn,self.cursor)
            if (result is not None):
                return result
        return None
    
    def list_tables(self):
        query = ("SELECT tablename"
             " FROM pg_tables"
             " WHERE tablename !~* 'pg_*' AND tablename !~* 'sql_*'")
        
        return self.allrows(query)
    
    def list_table_attributes(self, table_name):
        
        query = ("SELECT a.attname AS Column, t.typname AS Type"
             " FROM pg_class c, pg_attribute a, pg_type t"
             " WHERE c.relname = '%s'"
                " AND a.attnum > 0"
                " AND a.attrelid = c.oid"
                " AND a.atttypid = t.oid"
                " ORDER BY a.attnum;" %table_name)
        
        return self.allrows(query)
    
    def display_schema(self):
        print 'The Schema is:'  
        
        tables = self.list_tables()
        for table in tables:
            print"\n-----------------------------------------------------"
            print"Table: %s" %table
            
            attributes =  self.list_table_attributes(table)
            for attribute in attributes:
                print "%s (%s)" %(attribute[0],attribute[1])
        
    def total_cost(self, query):
        query = " EXPLAIN " + query
        query_plan = self.allrows(query)
        idx_dot = query_plan[0][0].find("..")
        idx_rows = query_plan[0][0].find("rows")
        
        total_cost = query_plan[0][0][idx_dot+2:idx_rows]
        return total_cost
    
def main():
    dbobj = Db_connection()
    dbobj.clear_database()
#    dbobj.display_schema()
#    query = " SELECT dept_id, MAX(salary) FROM company GROUP BY dept_id ORDER BY MAX(salary) "
#    print dbobj.total_cost(query)

    query = " SELECT * FROM quotes "
    print dbobj.total_cost(query)
    
    
    
#    print dbobj.list_tables()
#    print dbobj.list_table_attributes('quotes')
#    print dbobj.list_table_attributes('q_orcl')
    

    
if __name__ == "__main__":
    main()
        