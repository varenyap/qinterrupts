import db_connection

totalCost = 0
db = db_connection.Db_connection()

# If the temp tables we are about to create exist, drop them!
db.make_query("""drop table if exists finalOutputTable cascade""")
db.make_query("""drop table if exists temp0 cascade;""")
db.make_query("""drop table if exists temp1 cascade;""")
db.make_query("""drop table if exists temp2 cascade;""")
db.make_query("""drop table if exists temp3 cascade;""")
db.make_query("""drop table if exists temp4 cascade;""")
db.make_query("""drop table if exists temp5 cascade;""")
db.make_query("""drop table if exists temp6 cascade;""")
db.make_query("""drop table if exists temp7 cascade;""")
db.make_query("""drop table if exists temp8 cascade;""")
db.make_query("""drop table if exists temp9 cascade;""")
db.make_query("""drop table if exists temp10 cascade;""")
db.make_query("""drop table if exists temp11 cascade;""")
db.make_query("""drop table if exists temp12 cascade;""")
db.make_query("""drop table if exists temp13 cascade;""")
db.make_query("""drop table if exists temp14 cascade;""")
db.make_query("""drop table if exists temp15 cascade;""")
db.make_query("""drop table if exists temp16 cascade;""")
db.make_query("""drop table if exists temp17 cascade;""")
db.make_query("""drop table if exists temp18 cascade;""")
db.make_query("""drop table if exists temp19 cascade;""")
db.make_query("""drop table if exists temp20 cascade;""")
db.make_query("""drop table if exists temp21 cascade;""")
db.make_query("""drop table if exists temp22 cascade;""")
db.make_query("""drop table if exists temp23 cascade;""")
db.make_query("""drop table if exists temp24 cascade;""")


# Make a db call and run the sub queries that will collectively evaluate to the main query result
db.make_query(""" SELECT 'ALGERIA                  '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp0  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'ALGERIA                  ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'ALGERIA                  '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp0  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'ALGERIA                  ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'ARGENTINA                '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp1  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'ARGENTINA                ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'ARGENTINA                '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp1  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'ARGENTINA                ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'BRAZIL                   '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp2  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'BRAZIL                   ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'BRAZIL                   '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp2  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'BRAZIL                   ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'CANADA                   '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp3  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'CANADA                   ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'CANADA                   '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp3  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'CANADA                   ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'CHINA                    '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp4  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'CHINA                    ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'CHINA                    '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp4  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'CHINA                    ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'EGYPT                    '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp5  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'EGYPT                    ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'EGYPT                    '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp5  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'EGYPT                    ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'ETHIOPIA                 '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp6  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'ETHIOPIA                 ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'ETHIOPIA                 '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp6  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'ETHIOPIA                 ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'FRANCE                   '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp7  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'FRANCE                   ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'FRANCE                   '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp7  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'FRANCE                   ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'GERMANY                  '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp8  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'GERMANY                  ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'GERMANY                  '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp8  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'GERMANY                  ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'INDIA                    '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp9  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'INDIA                    ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'INDIA                    '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp9  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'INDIA                    ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'INDONESIA                '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp10  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'INDONESIA                ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'INDONESIA                '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp10  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'INDONESIA                ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'IRAN                     '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp11  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'IRAN                     ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'IRAN                     '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp11  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'IRAN                     ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'IRAQ                     '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp12  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'IRAQ                     ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'IRAQ                     '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp12  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'IRAQ                     ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'JAPAN                    '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp13  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'JAPAN                    ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'JAPAN                    '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp13  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'JAPAN                    ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'JORDAN                   '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp14  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'JORDAN                   ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'JORDAN                   '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp14  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'JORDAN                   ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'KENYA                    '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp15  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'KENYA                    ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'KENYA                    '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp15  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'KENYA                    ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'MOROCCO                  '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp16  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'MOROCCO                  ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'MOROCCO                  '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp16  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'MOROCCO                  ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'MOZAMBIQUE               '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp17  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'MOZAMBIQUE               ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'MOZAMBIQUE               '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp17  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'MOZAMBIQUE               ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'PERU                     '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp18  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'PERU                     ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'PERU                     '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp18  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'PERU                     ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'ROMANIA                  '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp19  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'ROMANIA                  ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'ROMANIA                  '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp19  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'ROMANIA                  ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'RUSSIA                   '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp20  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'RUSSIA                   ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'RUSSIA                   '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp20  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'RUSSIA                   ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'SAUDI ARABIA             '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp21  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'SAUDI ARABIA             ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'SAUDI ARABIA             '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp21  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'SAUDI ARABIA             ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'UNITED KINGDOM           '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp22  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'UNITED KINGDOM           ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'UNITED KINGDOM           '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp22  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'UNITED KINGDOM           ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'UNITED STATES            '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp23  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'UNITED STATES            ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'UNITED STATES            '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp23  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'UNITED STATES            ' GROUP BY  n_name"""))

db.make_query(""" SELECT 'VIETNAM                  '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp24  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'VIETNAM                  ' GROUP BY  n_name""")
totalCost+= int(db.total_cost(""" SELECT 'VIETNAM                  '::Text AS n_name,  SUM(l_extendedprice * (1 - l_discount)) AS SUMl_extendedprice__1__l_discount INTO temp24  FROM customer, orders, lineitem, supplier, nation, region WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
                        AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                        AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE' AND o_orderdate >= date '1993-01-01'
                        AND o_orderdate < date '1993-01-01' + interval '1' YEAR
                    AND n_name = 'VIETNAM                  ' GROUP BY  n_name"""))


#To find the union of the result sets
db.make_query(""" SELECT * INTO finaloutputTable FROM temp0 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" SELECT * INTO finaloutputTable FROM temp0 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp1 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp1 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp2 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp2 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp3 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp3 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp4 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp4 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp5 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp5 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp6 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp6 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp7 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp7 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp8 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp8 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp9 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp9 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp10 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp10 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp11 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp11 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp12 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp12 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp13 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp13 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp14 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp14 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp15 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp15 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp16 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp16 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp17 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp17 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp18 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp18 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp19 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp19 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp20 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp20 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp21 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp21 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp22 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp22 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp23 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp23 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp24 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp24 WHERE SUMl_extendedprice__1__l_discount IS NOT NULL"""))


#Final query
db.make_pquery("""SELECT n_name,SUMl_extendedprice__1__l_discount FROM finalOutputTable """)
totalCost+= int(db.total_cost("""SELECT n_name,SUMl_extendedprice__1__l_discount FROM finalOutputTable """))
print 'Cost to execute script is: %s' %(totalCost)
