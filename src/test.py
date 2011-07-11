import db_connection

def main():

    db = db_connection.Db_connection()
    
    print "Resetting database first"
    db.clear_database()
    print"-----------------------------------------------------\n"
    
    #First, rank stocks in descending order over maxjump for a two week period.
    print "Part1: Rank stocks in descending order over maxjump for the two week period"
    query1 = ("SELECT sym, MAX(price) - MIN(price) as maxjump"
              " INTO q_max_overall"
              " FROM quotes" 
              " GROUP BY sym"
              " ORDER BY maxjump DESC;")
    
    db.make_query(query1)
    weeklyjump_set = db.allrows("SELECT * from q_max_overall;")    
    print "Tables Created: q_max_overall"
    print"-----------------------------------------------------\n"
    
    #Part2 - Okay, so now need to compute the daily jump for each stock in order of max bi-weekly jump     
    print"Part2: Compute daily jump for each stock in order of max two-week period jump"
    for i in weeklyjump_set:
        stock_sym = i[0]
        
        query2 = ("SELECT Q1.sym, Q1.day, Q1.price - Q2.price as dayjump"
                  " INTO q_%s"                  
                  " FROM quotes as Q1, quotes as Q2"
                  " WHERE Q1.sym = '%s' and Q2.sym = '%s' and Q1.day = Q2.day -1;" %(stock_sym,stock_sym,stock_sym))
        db.make_query(query2)       
        
        query3 = ("INSERT INTO q_%s (sym, day, dayjump)"
                        " (SELECT * "
                        "FROM quotes "
                        "WHERE sym = '%s' and day = (SELECT MAX(day) FROM quotes));" %(stock_sym, stock_sym))
                        
        db.make_query(query3)           
    print "Tables Created: q_MSFT, q_ORCL,q_IBM"
    print"-----------------------------------------------------\n"
    
    #Part3 NOw show results of all stocks in union, but in order of computation.
    print"Part3: Show results of all stocks in union, in order of computation."
    for k in weeklyjump_set:
        stock_sym = k[0]      
        
        query4 = ("INSERT INTO output (sym, day, dayjump)"
                    " (SELECT * "
                    " FROM q_%s);" %stock_sym)
        
        db.make_query(query4)
    print "Tables created: output"
    print"-----------------------------------------------------\n"
    
    print "Done!"   
    
if __name__ == "__main__":
    main()
        