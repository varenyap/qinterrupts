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
    
    # Use for queries when the result need not be displayed to screen    
    def make_query(self,query):
        self.openConnection()
        (self.cursor).execute(query)
        (self.conn).commit()    
        self.closeConnection(self.conn,self.cursor)

    # Use when the first result of the query is needed.
    def onerow(self,query):
        self.openConnection()
        (self.cursor).execute(query)
        result = (self.cursor).fetchone()
        self.closeConnection(self.conn,self.cursor)
        return result
    
    #Use when you want all the results of the query
    def allrows(self,query):
        self.openConnection()
        (self.cursor).execute(query)
        result = (self.cursor).fetchall()
        self.closeConnection(self.conn,self.cursor)
        return result
    
def main():
    dbobj = Db_connection()
    
    dbobj.clear_database()
#    query = "SELECT * FROM quotes;"
##    dbobj.make_pquery(query)
#    
#    db_data = dbobj.allrows(query)
##    print db_data
#    
#    query2 = "SELECT Q1.sym, Q1.day, Q1.price - Q2.price as dayjump INTO Q_IBM FROM quotes as Q1, quotes as Q2 WHERE Q1.sym = 'IBM' and Q2.sym = 'IBM' and Q1.day = Q2.day -1"
##    query2 = "INSERT INTO users(uid, uname, pwd) VALUES (4,'natesaprasad', 'dodo');"
#    result = dbobj.make_pquery(query2)
    
if __name__ == "__main__":
    main()
        