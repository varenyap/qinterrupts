import db_connection

totalCost = 0
db = db_connection.Db_connection()

db.clear_database() # reset the database on each run.

# If the temp tables we are about to create exist, drop them!
db.make_query("""drop table if exists finalOutputTable cascade""")
db.make_query("""drop table if exists temp0 cascade;""")
db.make_query("""drop table if exists temp1 cascade;""")
db.make_query("""drop table if exists temp2 cascade;""")


# Make a db call and run the sub queries that will collectively evaluate to the main query result
db.make_query(""" SELECT 'MSFT'::Text AS q1_sym,  MAX(q1.price) -MIN(q2.price) AS MAXq1_price_MINq2_price INTO temp0  FROM quotes AS q1, quotes AS q2 WHERE q1.sym = q2.sym AND q1.days = q2.days -1 AND q1.sym = 'MSFT' GROUP BY  q1.sym""")
totalCost+= int(db.total_cost(""" SELECT 'MSFT'::Text AS q1_sym,  MAX(q1.price) -MIN(q2.price) AS MAXq1_price_MINq2_price INTO temp0  FROM quotes AS q1, quotes AS q2 WHERE q1.sym = q2.sym AND q1.days = q2.days -1 AND q1.sym = 'MSFT' GROUP BY  q1.sym"""))

db.make_query(""" SELECT 'IBM'::Text AS q1_sym,  MAX(q1.price) -MIN(q2.price) AS MAXq1_price_MINq2_price INTO temp1  FROM quotes AS q1, quotes AS q2 WHERE q1.sym = q2.sym AND q1.days = q2.days -1 AND q1.sym = 'IBM' GROUP BY  q1.sym""")
totalCost+= int(db.total_cost(""" SELECT 'IBM'::Text AS q1_sym,  MAX(q1.price) -MIN(q2.price) AS MAXq1_price_MINq2_price INTO temp1  FROM quotes AS q1, quotes AS q2 WHERE q1.sym = q2.sym AND q1.days = q2.days -1 AND q1.sym = 'IBM' GROUP BY  q1.sym"""))

db.make_query(""" SELECT 'ORCL'::Text AS q1_sym,  MAX(q1.price) -MIN(q2.price) AS MAXq1_price_MINq2_price INTO temp2  FROM quotes AS q1, quotes AS q2 WHERE q1.sym = q2.sym AND q1.days = q2.days -1 AND q1.sym = 'ORCL' GROUP BY  q1.sym""")
totalCost+= int(db.total_cost(""" SELECT 'ORCL'::Text AS q1_sym,  MAX(q1.price) -MIN(q2.price) AS MAXq1_price_MINq2_price INTO temp2  FROM quotes AS q1, quotes AS q2 WHERE q1.sym = q2.sym AND q1.days = q2.days -1 AND q1.sym = 'ORCL' GROUP BY  q1.sym"""))


#To find the union of the result sets
db.make_query(""" SELECT * INTO finaloutputTable FROM temp0 WHERE MAXq1_price_MINq2_price IS NOT NULL""")
totalCost+= int(db.total_cost(""" SELECT * INTO finaloutputTable FROM temp0 WHERE MAXq1_price_MINq2_price IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp1 WHERE MAXq1_price_MINq2_price IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp1 WHERE MAXq1_price_MINq2_price IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp2 WHERE MAXq1_price_MINq2_price IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp2 WHERE MAXq1_price_MINq2_price IS NOT NULL"""))


#Final query
db.make_pquery("""SELECT q1_sym FROM finalOutputTable """)
totalCost+= int(db.total_cost("""SELECT q1_sym FROM finalOutputTable """))
print 'Cost to execute script is: %s' %(totalCost)
