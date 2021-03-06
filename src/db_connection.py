import psycopg2
import config
import math

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
    
    #private function to clear the database
    def reset_database(self,cursor):
        self.openConnection()
        
        for line in open(init_script_path):            
            (self.cursor).execute(line)
        
        (self.conn).commit()    
        self.closeConnection(self.conn,self.cursor)
                
    def clear_database(self):
        self.reset_database(self.cursor)

    # Use for select queries only i.e. use only to display results
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
    
    # Use for executing queries that will not return a result set back    
    def make_query(self,query):
        if (query is not None):
            self.openConnection()
            (self.cursor).execute(query)
            (self.conn).commit()    
    #        print query
            self.closeConnection(self.conn,self.cursor)

    # Use when only the first tuple of the result set is required.
    def onerow(self,query):
        if (query is not None):
            self.openConnection()
            (self.cursor).execute(query)
            result = (self.cursor).fetchone()
            self.closeConnection(self.conn,self.cursor)
            if (result is not None):
                return result
        return None   
    
    #Use when the entire result of the query is needed.
    def allrows(self,query):
        if (query is not None):
            self.openConnection()
            (self.cursor).execute(query)
            result = (self.cursor).fetchall()
            self.closeConnection(self.conn,self.cursor)
            if (result is not None):
                return result
        return None
    
    #Used to view only the relations in the schema
    def list_tables(self):
        query = ("SELECT tablename"
             " FROM pg_tables"
             " WHERE tablename !~* 'pg_*' AND tablename !~* 'sql_*'")
        
        return self.allrows(query)
    
    #Used to get the attributes of a given table
    def list_table_attributes(self, table_name):
        
        query = ("SELECT a.attname AS Column, t.typname AS Type"
             " FROM pg_class c, pg_attribute a, pg_type t"
             " WHERE c.relname = '%s'"
                " AND a.attnum > 0"
                " AND a.attrelid = c.oid"
                " AND a.atttypid = t.oid"
                " ORDER BY a.attnum;" %table_name)
        
        return self.allrows(query)
    
    #Used to display the entire schema i.e all relations and attributes/attribute type of each relations
    def display_schema(self):
        print 'The Schema is:'  
        
        tables = self.list_tables()
        for table in tables:
            print"\n-----------------------------------------------------"
            print"Table: %s" %table
            
            attributes =  self.list_table_attributes(table)
            for attribute in attributes:
                print "%s (%s)" %(attribute[0],attribute[1])
    
    #Function returns the approximate cost of the query. The decimal point is ignored
    def total_cost(self, query):
        query = " EXPLAIN " + query
        query_plan = self.allrows(query)
        idx_dot = query_plan[0][0].find("..")
        idx_rows = query_plan[0][0].find("rows")
        
        #This get the full cost including decimal as a string.
#        total_cost = query_plan[0][0][idx_dot+2:idx_rows]
        
        #Calculating approx cost (ignoring the decimal)
        idx_dot+=2
        total_cost = ""
        while (idx_dot < idx_rows):
            val = query_plan[0][0][idx_dot]
            if val.isdigit():
                total_cost+= str(query_plan[0][0][idx_dot])
            elif(val is "."): 
                break#Ignoring the decimal point if exists
            idx_dot+=1
                    
        return total_cost
    
def main():
    dbobj = Db_connection()
    dbobj.clear_database()
#    dbobj.display_schema()

#    query = " SELECT * FROM quotes "
#    cost = dbobj.total_cost(query)
#    print cost

    mainQuery = (" SELECT q1.sym "
                 " FROM quotes as q1, quotes as q2 "
                 " WHERE q1.sym = q2.sym and q1.days = q2.days -1 ")
    
    groupAttr =  " GROUP BY q1.sym "
    orderAttr = " ORDER BY MAX(q1.price) - MIN(q2.price) "
    query = mainQuery + groupAttr + orderAttr
    print dbobj.total_cost(query)


    
if __name__ == "__main__":
    main()
        