import db_connection

totalCost = 0
db = db_connection.Db_connection()

db.clear_database() # reset the database on each run.

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
db.make_query("""drop table if exists temp25 cascade;""")
db.make_query("""drop table if exists temp26 cascade;""")
db.make_query("""drop table if exists temp27 cascade;""")
db.make_query("""drop table if exists temp28 cascade;""")
db.make_query("""drop table if exists temp29 cascade;""")
db.make_query("""drop table if exists temp30 cascade;""")
db.make_query("""drop table if exists temp31 cascade;""")
db.make_query("""drop table if exists temp32 cascade;""")
db.make_query("""drop table if exists temp33 cascade;""")
db.make_query("""drop table if exists temp34 cascade;""")
db.make_query("""drop table if exists temp35 cascade;""")
db.make_query("""drop table if exists temp36 cascade;""")
db.make_query("""drop table if exists temp37 cascade;""")
db.make_query("""drop table if exists temp38 cascade;""")
db.make_query("""drop table if exists temp39 cascade;""")
db.make_query("""drop table if exists temp40 cascade;""")
db.make_query("""drop table if exists temp41 cascade;""")
db.make_query("""drop table if exists temp42 cascade;""")
db.make_query("""drop table if exists temp43 cascade;""")
db.make_query("""drop table if exists temp44 cascade;""")
db.make_query("""drop table if exists temp45 cascade;""")
db.make_query("""drop table if exists temp46 cascade;""")
db.make_query("""drop table if exists temp47 cascade;""")
db.make_query("""drop table if exists temp48 cascade;""")
db.make_query("""drop table if exists temp49 cascade;""")
db.make_query("""drop table if exists temp50 cascade;""")
db.make_query("""drop table if exists temp51 cascade;""")
db.make_query("""drop table if exists temp52 cascade;""")
db.make_query("""drop table if exists temp53 cascade;""")
db.make_query("""drop table if exists temp54 cascade;""")
db.make_query("""drop table if exists temp55 cascade;""")
db.make_query("""drop table if exists temp56 cascade;""")
db.make_query("""drop table if exists temp57 cascade;""")
db.make_query("""drop table if exists temp58 cascade;""")
db.make_query("""drop table if exists temp59 cascade;""")
db.make_query("""drop table if exists temp60 cascade;""")
db.make_query("""drop table if exists temp61 cascade;""")
db.make_query("""drop table if exists temp62 cascade;""")
db.make_query("""drop table if exists temp63 cascade;""")
db.make_query("""drop table if exists temp64 cascade;""")
db.make_query("""drop table if exists temp65 cascade;""")
db.make_query("""drop table if exists temp66 cascade;""")
db.make_query("""drop table if exists temp67 cascade;""")
db.make_query("""drop table if exists temp68 cascade;""")
db.make_query("""drop table if exists temp69 cascade;""")
db.make_query("""drop table if exists temp70 cascade;""")
db.make_query("""drop table if exists temp71 cascade;""")
db.make_query("""drop table if exists temp72 cascade;""")
db.make_query("""drop table if exists temp73 cascade;""")
db.make_query("""drop table if exists temp74 cascade;""")
db.make_query("""drop table if exists temp75 cascade;""")
db.make_query("""drop table if exists temp76 cascade;""")
db.make_query("""drop table if exists temp77 cascade;""")
db.make_query("""drop table if exists temp78 cascade;""")
db.make_query("""drop table if exists temp79 cascade;""")
db.make_query("""drop table if exists temp80 cascade;""")
db.make_query("""drop table if exists temp81 cascade;""")
db.make_query("""drop table if exists temp82 cascade;""")
db.make_query("""drop table if exists temp83 cascade;""")
db.make_query("""drop table if exists temp84 cascade;""")
db.make_query("""drop table if exists temp85 cascade;""")
db.make_query("""drop table if exists temp86 cascade;""")
db.make_query("""drop table if exists temp87 cascade;""")
db.make_query("""drop table if exists temp88 cascade;""")
db.make_query("""drop table if exists temp89 cascade;""")
db.make_query("""drop table if exists temp90 cascade;""")


# Make a db call and run the sub queries that will collectively evaluate to the main query result
db.make_query(""" SELECT 'AA'::Text AS sym,   MAX(price) AS MAXprice INTO temp0  FROM quotes WHERE sym = 'AA' """)
totalCost+= int(db.total_cost(""" SELECT 'AA'::Text AS sym,   MAX(price) AS MAXprice INTO temp0  FROM quotes WHERE sym = 'AA' """))

db.make_query(""" SELECT 'ADBE'::Text AS sym,   MAX(price) AS MAXprice INTO temp1  FROM quotes WHERE sym = 'ADBE' """)
totalCost+= int(db.total_cost(""" SELECT 'ADBE'::Text AS sym,   MAX(price) AS MAXprice INTO temp1  FROM quotes WHERE sym = 'ADBE' """))

db.make_query(""" SELECT 'ADSK'::Text AS sym,   MAX(price) AS MAXprice INTO temp2  FROM quotes WHERE sym = 'ADSK' """)
totalCost+= int(db.total_cost(""" SELECT 'ADSK'::Text AS sym,   MAX(price) AS MAXprice INTO temp2  FROM quotes WHERE sym = 'ADSK' """))

db.make_query(""" SELECT 'AGU'::Text AS sym,   MAX(price) AS MAXprice INTO temp3  FROM quotes WHERE sym = 'AGU' """)
totalCost+= int(db.total_cost(""" SELECT 'AGU'::Text AS sym,   MAX(price) AS MAXprice INTO temp3  FROM quotes WHERE sym = 'AGU' """))

db.make_query(""" SELECT 'AKRX'::Text AS sym,   MAX(price) AS MAXprice INTO temp4  FROM quotes WHERE sym = 'AKRX' """)
totalCost+= int(db.total_cost(""" SELECT 'AKRX'::Text AS sym,   MAX(price) AS MAXprice INTO temp4  FROM quotes WHERE sym = 'AKRX' """))

db.make_query(""" SELECT 'AMZN'::Text AS sym,   MAX(price) AS MAXprice INTO temp5  FROM quotes WHERE sym = 'AMZN' """)
totalCost+= int(db.total_cost(""" SELECT 'AMZN'::Text AS sym,   MAX(price) AS MAXprice INTO temp5  FROM quotes WHERE sym = 'AMZN' """))

db.make_query(""" SELECT 'APOL'::Text AS sym,   MAX(price) AS MAXprice INTO temp6  FROM quotes WHERE sym = 'APOL' """)
totalCost+= int(db.total_cost(""" SELECT 'APOL'::Text AS sym,   MAX(price) AS MAXprice INTO temp6  FROM quotes WHERE sym = 'APOL' """))

db.make_query(""" SELECT 'AXP'::Text AS sym,   MAX(price) AS MAXprice INTO temp7  FROM quotes WHERE sym = 'AXP' """)
totalCost+= int(db.total_cost(""" SELECT 'AXP'::Text AS sym,   MAX(price) AS MAXprice INTO temp7  FROM quotes WHERE sym = 'AXP' """))

db.make_query(""" SELECT 'BA'::Text AS sym,   MAX(price) AS MAXprice INTO temp8  FROM quotes WHERE sym = 'BA' """)
totalCost+= int(db.total_cost(""" SELECT 'BA'::Text AS sym,   MAX(price) AS MAXprice INTO temp8  FROM quotes WHERE sym = 'BA' """))

db.make_query(""" SELECT 'BAC'::Text AS sym,   MAX(price) AS MAXprice INTO temp9  FROM quotes WHERE sym = 'BAC' """)
totalCost+= int(db.total_cost(""" SELECT 'BAC'::Text AS sym,   MAX(price) AS MAXprice INTO temp9  FROM quotes WHERE sym = 'BAC' """))

db.make_query(""" SELECT 'BAS'::Text AS sym,   MAX(price) AS MAXprice INTO temp10  FROM quotes WHERE sym = 'BAS' """)
totalCost+= int(db.total_cost(""" SELECT 'BAS'::Text AS sym,   MAX(price) AS MAXprice INTO temp10  FROM quotes WHERE sym = 'BAS' """))

db.make_query(""" SELECT 'BBBY'::Text AS sym,   MAX(price) AS MAXprice INTO temp11  FROM quotes WHERE sym = 'BBBY' """)
totalCost+= int(db.total_cost(""" SELECT 'BBBY'::Text AS sym,   MAX(price) AS MAXprice INTO temp11  FROM quotes WHERE sym = 'BBBY' """))

db.make_query(""" SELECT 'BCE'::Text AS sym,   MAX(price) AS MAXprice INTO temp12  FROM quotes WHERE sym = 'BCE' """)
totalCost+= int(db.total_cost(""" SELECT 'BCE'::Text AS sym,   MAX(price) AS MAXprice INTO temp12  FROM quotes WHERE sym = 'BCE' """))

db.make_query(""" SELECT 'BIDU'::Text AS sym,   MAX(price) AS MAXprice INTO temp13  FROM quotes WHERE sym = 'BIDU' """)
totalCost+= int(db.total_cost(""" SELECT 'BIDU'::Text AS sym,   MAX(price) AS MAXprice INTO temp13  FROM quotes WHERE sym = 'BIDU' """))

db.make_query(""" SELECT 'BMC'::Text AS sym,   MAX(price) AS MAXprice INTO temp14  FROM quotes WHERE sym = 'BMC' """)
totalCost+= int(db.total_cost(""" SELECT 'BMC'::Text AS sym,   MAX(price) AS MAXprice INTO temp14  FROM quotes WHERE sym = 'BMC' """))

db.make_query(""" SELECT 'BRCM'::Text AS sym,   MAX(price) AS MAXprice INTO temp15  FROM quotes WHERE sym = 'BRCM' """)
totalCost+= int(db.total_cost(""" SELECT 'BRCM'::Text AS sym,   MAX(price) AS MAXprice INTO temp15  FROM quotes WHERE sym = 'BRCM' """))

db.make_query(""" SELECT 'CAT'::Text AS sym,   MAX(price) AS MAXprice INTO temp16  FROM quotes WHERE sym = 'CAT' """)
totalCost+= int(db.total_cost(""" SELECT 'CAT'::Text AS sym,   MAX(price) AS MAXprice INTO temp16  FROM quotes WHERE sym = 'CAT' """))

db.make_query(""" SELECT 'CMCSA'::Text AS sym,   MAX(price) AS MAXprice INTO temp17  FROM quotes WHERE sym = 'CMCSA' """)
totalCost+= int(db.total_cost(""" SELECT 'CMCSA'::Text AS sym,   MAX(price) AS MAXprice INTO temp17  FROM quotes WHERE sym = 'CMCSA' """))

db.make_query(""" SELECT 'CNOOC'::Text AS sym,   MAX(price) AS MAXprice INTO temp18  FROM quotes WHERE sym = 'CNOOC' """)
totalCost+= int(db.total_cost(""" SELECT 'CNOOC'::Text AS sym,   MAX(price) AS MAXprice INTO temp18  FROM quotes WHERE sym = 'CNOOC' """))

db.make_query(""" SELECT 'CSCO'::Text AS sym,   MAX(price) AS MAXprice INTO temp19  FROM quotes WHERE sym = 'CSCO' """)
totalCost+= int(db.total_cost(""" SELECT 'CSCO'::Text AS sym,   MAX(price) AS MAXprice INTO temp19  FROM quotes WHERE sym = 'CSCO' """))

db.make_query(""" SELECT 'CTSH'::Text AS sym,   MAX(price) AS MAXprice INTO temp20  FROM quotes WHERE sym = 'CTSH' """)
totalCost+= int(db.total_cost(""" SELECT 'CTSH'::Text AS sym,   MAX(price) AS MAXprice INTO temp20  FROM quotes WHERE sym = 'CTSH' """))

db.make_query(""" SELECT 'CTXS'::Text AS sym,   MAX(price) AS MAXprice INTO temp21  FROM quotes WHERE sym = 'CTXS' """)
totalCost+= int(db.total_cost(""" SELECT 'CTXS'::Text AS sym,   MAX(price) AS MAXprice INTO temp21  FROM quotes WHERE sym = 'CTXS' """))

db.make_query(""" SELECT 'CVX'::Text AS sym,   MAX(price) AS MAXprice INTO temp22  FROM quotes WHERE sym = 'CVX' """)
totalCost+= int(db.total_cost(""" SELECT 'CVX'::Text AS sym,   MAX(price) AS MAXprice INTO temp22  FROM quotes WHERE sym = 'CVX' """))

db.make_query(""" SELECT 'DD'::Text AS sym,   MAX(price) AS MAXprice INTO temp23  FROM quotes WHERE sym = 'DD' """)
totalCost+= int(db.total_cost(""" SELECT 'DD'::Text AS sym,   MAX(price) AS MAXprice INTO temp23  FROM quotes WHERE sym = 'DD' """))

db.make_query(""" SELECT 'DELL'::Text AS sym,   MAX(price) AS MAXprice INTO temp24  FROM quotes WHERE sym = 'DELL' """)
totalCost+= int(db.total_cost(""" SELECT 'DELL'::Text AS sym,   MAX(price) AS MAXprice INTO temp24  FROM quotes WHERE sym = 'DELL' """))

db.make_query(""" SELECT 'DIS'::Text AS sym,   MAX(price) AS MAXprice INTO temp25  FROM quotes WHERE sym = 'DIS' """)
totalCost+= int(db.total_cost(""" SELECT 'DIS'::Text AS sym,   MAX(price) AS MAXprice INTO temp25  FROM quotes WHERE sym = 'DIS' """))

db.make_query(""" SELECT 'DK'::Text AS sym,   MAX(price) AS MAXprice INTO temp26  FROM quotes WHERE sym = 'DK' """)
totalCost+= int(db.total_cost(""" SELECT 'DK'::Text AS sym,   MAX(price) AS MAXprice INTO temp26  FROM quotes WHERE sym = 'DK' """))

db.make_query(""" SELECT 'EBAY'::Text AS sym,   MAX(price) AS MAXprice INTO temp27  FROM quotes WHERE sym = 'EBAY' """)
totalCost+= int(db.total_cost(""" SELECT 'EBAY'::Text AS sym,   MAX(price) AS MAXprice INTO temp27  FROM quotes WHERE sym = 'EBAY' """))

db.make_query(""" SELECT 'EDC'::Text AS sym,   MAX(price) AS MAXprice INTO temp28  FROM quotes WHERE sym = 'EDC' """)
totalCost+= int(db.total_cost(""" SELECT 'EDC'::Text AS sym,   MAX(price) AS MAXprice INTO temp28  FROM quotes WHERE sym = 'EDC' """))

db.make_query(""" SELECT 'EQIX'::Text AS sym,   MAX(price) AS MAXprice INTO temp29  FROM quotes WHERE sym = 'EQIX' """)
totalCost+= int(db.total_cost(""" SELECT 'EQIX'::Text AS sym,   MAX(price) AS MAXprice INTO temp29  FROM quotes WHERE sym = 'EQIX' """))

db.make_query(""" SELECT 'ERTS'::Text AS sym,   MAX(price) AS MAXprice INTO temp30  FROM quotes WHERE sym = 'ERTS' """)
totalCost+= int(db.total_cost(""" SELECT 'ERTS'::Text AS sym,   MAX(price) AS MAXprice INTO temp30  FROM quotes WHERE sym = 'ERTS' """))

db.make_query(""" SELECT 'EVR'::Text AS sym,   MAX(price) AS MAXprice INTO temp31  FROM quotes WHERE sym = 'EVR' """)
totalCost+= int(db.total_cost(""" SELECT 'EVR'::Text AS sym,   MAX(price) AS MAXprice INTO temp31  FROM quotes WHERE sym = 'EVR' """))

db.make_query(""" SELECT 'EWBC'::Text AS sym,   MAX(price) AS MAXprice INTO temp32  FROM quotes WHERE sym = 'EWBC' """)
totalCost+= int(db.total_cost(""" SELECT 'EWBC'::Text AS sym,   MAX(price) AS MAXprice INTO temp32  FROM quotes WHERE sym = 'EWBC' """))

db.make_query(""" SELECT 'EXPE'::Text AS sym,   MAX(price) AS MAXprice INTO temp33  FROM quotes WHERE sym = 'EXPE' """)
totalCost+= int(db.total_cost(""" SELECT 'EXPE'::Text AS sym,   MAX(price) AS MAXprice INTO temp33  FROM quotes WHERE sym = 'EXPE' """))

db.make_query(""" SELECT 'FSI'::Text AS sym,   MAX(price) AS MAXprice INTO temp34  FROM quotes WHERE sym = 'FSI' """)
totalCost+= int(db.total_cost(""" SELECT 'FSI'::Text AS sym,   MAX(price) AS MAXprice INTO temp34  FROM quotes WHERE sym = 'FSI' """))

db.make_query(""" SELECT 'GE'::Text AS sym,   MAX(price) AS MAXprice INTO temp35  FROM quotes WHERE sym = 'GE' """)
totalCost+= int(db.total_cost(""" SELECT 'GE'::Text AS sym,   MAX(price) AS MAXprice INTO temp35  FROM quotes WHERE sym = 'GE' """))

db.make_query(""" SELECT 'GENE'::Text AS sym,   MAX(price) AS MAXprice INTO temp36  FROM quotes WHERE sym = 'GENE' """)
totalCost+= int(db.total_cost(""" SELECT 'GENE'::Text AS sym,   MAX(price) AS MAXprice INTO temp36  FROM quotes WHERE sym = 'GENE' """))

db.make_query(""" SELECT 'GOOG'::Text AS sym,   MAX(price) AS MAXprice INTO temp37  FROM quotes WHERE sym = 'GOOG' """)
totalCost+= int(db.total_cost(""" SELECT 'GOOG'::Text AS sym,   MAX(price) AS MAXprice INTO temp37  FROM quotes WHERE sym = 'GOOG' """))

db.make_query(""" SELECT 'GRMN'::Text AS sym,   MAX(price) AS MAXprice INTO temp38  FROM quotes WHERE sym = 'GRMN' """)
totalCost+= int(db.total_cost(""" SELECT 'GRMN'::Text AS sym,   MAX(price) AS MAXprice INTO temp38  FROM quotes WHERE sym = 'GRMN' """))

db.make_query(""" SELECT 'HD'::Text AS sym,   MAX(price) AS MAXprice INTO temp39  FROM quotes WHERE sym = 'HD' """)
totalCost+= int(db.total_cost(""" SELECT 'HD'::Text AS sym,   MAX(price) AS MAXprice INTO temp39  FROM quotes WHERE sym = 'HD' """))

db.make_query(""" SELECT 'HPQ'::Text AS sym,   MAX(price) AS MAXprice INTO temp40  FROM quotes WHERE sym = 'HPQ' """)
totalCost+= int(db.total_cost(""" SELECT 'HPQ'::Text AS sym,   MAX(price) AS MAXprice INTO temp40  FROM quotes WHERE sym = 'HPQ' """))

db.make_query(""" SELECT 'HSTM'::Text AS sym,   MAX(price) AS MAXprice INTO temp41  FROM quotes WHERE sym = 'HSTM' """)
totalCost+= int(db.total_cost(""" SELECT 'HSTM'::Text AS sym,   MAX(price) AS MAXprice INTO temp41  FROM quotes WHERE sym = 'HSTM' """))

db.make_query(""" SELECT 'IBM'::Text AS sym,   MAX(price) AS MAXprice INTO temp42  FROM quotes WHERE sym = 'IBM' """)
totalCost+= int(db.total_cost(""" SELECT 'IBM'::Text AS sym,   MAX(price) AS MAXprice INTO temp42  FROM quotes WHERE sym = 'IBM' """))

db.make_query(""" SELECT 'ILMN'::Text AS sym,   MAX(price) AS MAXprice INTO temp43  FROM quotes WHERE sym = 'ILMN' """)
totalCost+= int(db.total_cost(""" SELECT 'ILMN'::Text AS sym,   MAX(price) AS MAXprice INTO temp43  FROM quotes WHERE sym = 'ILMN' """))

db.make_query(""" SELECT 'INFY'::Text AS sym,   MAX(price) AS MAXprice INTO temp44  FROM quotes WHERE sym = 'INFY' """)
totalCost+= int(db.total_cost(""" SELECT 'INFY'::Text AS sym,   MAX(price) AS MAXprice INTO temp44  FROM quotes WHERE sym = 'INFY' """))

db.make_query(""" SELECT 'INTC'::Text AS sym,   MAX(price) AS MAXprice INTO temp45  FROM quotes WHERE sym = 'INTC' """)
totalCost+= int(db.total_cost(""" SELECT 'INTC'::Text AS sym,   MAX(price) AS MAXprice INTO temp45  FROM quotes WHERE sym = 'INTC' """))

db.make_query(""" SELECT 'INTU'::Text AS sym,   MAX(price) AS MAXprice INTO temp46  FROM quotes WHERE sym = 'INTU' """)
totalCost+= int(db.total_cost(""" SELECT 'INTU'::Text AS sym,   MAX(price) AS MAXprice INTO temp46  FROM quotes WHERE sym = 'INTU' """))

db.make_query(""" SELECT 'JNJ'::Text AS sym,   MAX(price) AS MAXprice INTO temp47  FROM quotes WHERE sym = 'JNJ' """)
totalCost+= int(db.total_cost(""" SELECT 'JNJ'::Text AS sym,   MAX(price) AS MAXprice INTO temp47  FROM quotes WHERE sym = 'JNJ' """))

db.make_query(""" SELECT 'JOYG'::Text AS sym,   MAX(price) AS MAXprice INTO temp48  FROM quotes WHERE sym = 'JOYG' """)
totalCost+= int(db.total_cost(""" SELECT 'JOYG'::Text AS sym,   MAX(price) AS MAXprice INTO temp48  FROM quotes WHERE sym = 'JOYG' """))

db.make_query(""" SELECT 'JPM'::Text AS sym,   MAX(price) AS MAXprice INTO temp49  FROM quotes WHERE sym = 'JPM' """)
totalCost+= int(db.total_cost(""" SELECT 'JPM'::Text AS sym,   MAX(price) AS MAXprice INTO temp49  FROM quotes WHERE sym = 'JPM' """))

db.make_query(""" SELECT 'JST'::Text AS sym,   MAX(price) AS MAXprice INTO temp50  FROM quotes WHERE sym = 'JST' """)
totalCost+= int(db.total_cost(""" SELECT 'JST'::Text AS sym,   MAX(price) AS MAXprice INTO temp50  FROM quotes WHERE sym = 'JST' """))

db.make_query(""" SELECT 'KCI'::Text AS sym,   MAX(price) AS MAXprice INTO temp51  FROM quotes WHERE sym = 'KCI' """)
totalCost+= int(db.total_cost(""" SELECT 'KCI'::Text AS sym,   MAX(price) AS MAXprice INTO temp51  FROM quotes WHERE sym = 'KCI' """))

db.make_query(""" SELECT 'KEYN'::Text AS sym,   MAX(price) AS MAXprice INTO temp52  FROM quotes WHERE sym = 'KEYN' """)
totalCost+= int(db.total_cost(""" SELECT 'KEYN'::Text AS sym,   MAX(price) AS MAXprice INTO temp52  FROM quotes WHERE sym = 'KEYN' """))

db.make_query(""" SELECT 'KLAC'::Text AS sym,   MAX(price) AS MAXprice INTO temp53  FROM quotes WHERE sym = 'KLAC' """)
totalCost+= int(db.total_cost(""" SELECT 'KLAC'::Text AS sym,   MAX(price) AS MAXprice INTO temp53  FROM quotes WHERE sym = 'KLAC' """))

db.make_query(""" SELECT 'KO'::Text AS sym,   MAX(price) AS MAXprice INTO temp54  FROM quotes WHERE sym = 'KO' """)
totalCost+= int(db.total_cost(""" SELECT 'KO'::Text AS sym,   MAX(price) AS MAXprice INTO temp54  FROM quotes WHERE sym = 'KO' """))

db.make_query(""" SELECT 'LEN'::Text AS sym,   MAX(price) AS MAXprice INTO temp55  FROM quotes WHERE sym = 'LEN' """)
totalCost+= int(db.total_cost(""" SELECT 'LEN'::Text AS sym,   MAX(price) AS MAXprice INTO temp55  FROM quotes WHERE sym = 'LEN' """))

db.make_query(""" SELECT 'LRCX'::Text AS sym,   MAX(price) AS MAXprice INTO temp56  FROM quotes WHERE sym = 'LRCX' """)
totalCost+= int(db.total_cost(""" SELECT 'LRCX'::Text AS sym,   MAX(price) AS MAXprice INTO temp56  FROM quotes WHERE sym = 'LRCX' """))

db.make_query(""" SELECT 'MAT'::Text AS sym,   MAX(price) AS MAXprice INTO temp57  FROM quotes WHERE sym = 'MAT' """)
totalCost+= int(db.total_cost(""" SELECT 'MAT'::Text AS sym,   MAX(price) AS MAXprice INTO temp57  FROM quotes WHERE sym = 'MAT' """))

db.make_query(""" SELECT 'MCD'::Text AS sym,   MAX(price) AS MAXprice INTO temp58  FROM quotes WHERE sym = 'MCD' """)
totalCost+= int(db.total_cost(""" SELECT 'MCD'::Text AS sym,   MAX(price) AS MAXprice INTO temp58  FROM quotes WHERE sym = 'MCD' """))

db.make_query(""" SELECT 'MCHP'::Text AS sym,   MAX(price) AS MAXprice INTO temp59  FROM quotes WHERE sym = 'MCHP' """)
totalCost+= int(db.total_cost(""" SELECT 'MCHP'::Text AS sym,   MAX(price) AS MAXprice INTO temp59  FROM quotes WHERE sym = 'MCHP' """))

db.make_query(""" SELECT 'MMM'::Text AS sym,   MAX(price) AS MAXprice INTO temp60  FROM quotes WHERE sym = 'MMM' """)
totalCost+= int(db.total_cost(""" SELECT 'MMM'::Text AS sym,   MAX(price) AS MAXprice INTO temp60  FROM quotes WHERE sym = 'MMM' """))

db.make_query(""" SELECT 'MOS'::Text AS sym,   MAX(price) AS MAXprice INTO temp61  FROM quotes WHERE sym = 'MOS' """)
totalCost+= int(db.total_cost(""" SELECT 'MOS'::Text AS sym,   MAX(price) AS MAXprice INTO temp61  FROM quotes WHERE sym = 'MOS' """))

db.make_query(""" SELECT 'MSFT'::Text AS sym,   MAX(price) AS MAXprice INTO temp62  FROM quotes WHERE sym = 'MSFT' """)
totalCost+= int(db.total_cost(""" SELECT 'MSFT'::Text AS sym,   MAX(price) AS MAXprice INTO temp62  FROM quotes WHERE sym = 'MSFT' """))

db.make_query(""" SELECT 'MSN'::Text AS sym,   MAX(price) AS MAXprice INTO temp63  FROM quotes WHERE sym = 'MSN' """)
totalCost+= int(db.total_cost(""" SELECT 'MSN'::Text AS sym,   MAX(price) AS MAXprice INTO temp63  FROM quotes WHERE sym = 'MSN' """))

db.make_query(""" SELECT 'MSPD'::Text AS sym,   MAX(price) AS MAXprice INTO temp64  FROM quotes WHERE sym = 'MSPD' """)
totalCost+= int(db.total_cost(""" SELECT 'MSPD'::Text AS sym,   MAX(price) AS MAXprice INTO temp64  FROM quotes WHERE sym = 'MSPD' """))

db.make_query(""" SELECT 'MU'::Text AS sym,   MAX(price) AS MAXprice INTO temp65  FROM quotes WHERE sym = 'MU' """)
totalCost+= int(db.total_cost(""" SELECT 'MU'::Text AS sym,   MAX(price) AS MAXprice INTO temp65  FROM quotes WHERE sym = 'MU' """))

db.make_query(""" SELECT 'MYL'::Text AS sym,   MAX(price) AS MAXprice INTO temp66  FROM quotes WHERE sym = 'MYL' """)
totalCost+= int(db.total_cost(""" SELECT 'MYL'::Text AS sym,   MAX(price) AS MAXprice INTO temp66  FROM quotes WHERE sym = 'MYL' """))

db.make_query(""" SELECT 'NFLX'::Text AS sym,   MAX(price) AS MAXprice INTO temp67  FROM quotes WHERE sym = 'NFLX' """)
totalCost+= int(db.total_cost(""" SELECT 'NFLX'::Text AS sym,   MAX(price) AS MAXprice INTO temp67  FROM quotes WHERE sym = 'NFLX' """))

db.make_query(""" SELECT 'NGD'::Text AS sym,   MAX(price) AS MAXprice INTO temp68  FROM quotes WHERE sym = 'NGD' """)
totalCost+= int(db.total_cost(""" SELECT 'NGD'::Text AS sym,   MAX(price) AS MAXprice INTO temp68  FROM quotes WHERE sym = 'NGD' """))

db.make_query(""" SELECT 'NIHD'::Text AS sym,   MAX(price) AS MAXprice INTO temp69  FROM quotes WHERE sym = 'NIHD' """)
totalCost+= int(db.total_cost(""" SELECT 'NIHD'::Text AS sym,   MAX(price) AS MAXprice INTO temp69  FROM quotes WHERE sym = 'NIHD' """))

db.make_query(""" SELECT 'NTAP'::Text AS sym,   MAX(price) AS MAXprice INTO temp70  FROM quotes WHERE sym = 'NTAP' """)
totalCost+= int(db.total_cost(""" SELECT 'NTAP'::Text AS sym,   MAX(price) AS MAXprice INTO temp70  FROM quotes WHERE sym = 'NTAP' """))

db.make_query(""" SELECT 'ODP'::Text AS sym,   MAX(price) AS MAXprice INTO temp71  FROM quotes WHERE sym = 'ODP' """)
totalCost+= int(db.total_cost(""" SELECT 'ODP'::Text AS sym,   MAX(price) AS MAXprice INTO temp71  FROM quotes WHERE sym = 'ODP' """))

db.make_query(""" SELECT 'ORCL'::Text AS sym,   MAX(price) AS MAXprice INTO temp72  FROM quotes WHERE sym = 'ORCL' """)
totalCost+= int(db.total_cost(""" SELECT 'ORCL'::Text AS sym,   MAX(price) AS MAXprice INTO temp72  FROM quotes WHERE sym = 'ORCL' """))

db.make_query(""" SELECT 'OTT'::Text AS sym,   MAX(price) AS MAXprice INTO temp73  FROM quotes WHERE sym = 'OTT' """)
totalCost+= int(db.total_cost(""" SELECT 'OTT'::Text AS sym,   MAX(price) AS MAXprice INTO temp73  FROM quotes WHERE sym = 'OTT' """))

db.make_query(""" SELECT 'PEP'::Text AS sym,   MAX(price) AS MAXprice INTO temp74  FROM quotes WHERE sym = 'PEP' """)
totalCost+= int(db.total_cost(""" SELECT 'PEP'::Text AS sym,   MAX(price) AS MAXprice INTO temp74  FROM quotes WHERE sym = 'PEP' """))

db.make_query(""" SELECT 'PG'::Text AS sym,   MAX(price) AS MAXprice INTO temp75  FROM quotes WHERE sym = 'PG' """)
totalCost+= int(db.total_cost(""" SELECT 'PG'::Text AS sym,   MAX(price) AS MAXprice INTO temp75  FROM quotes WHERE sym = 'PG' """))

db.make_query(""" SELECT 'RDSA'::Text AS sym,   MAX(price) AS MAXprice INTO temp76  FROM quotes WHERE sym = 'RDSA' """)
totalCost+= int(db.total_cost(""" SELECT 'RDSA'::Text AS sym,   MAX(price) AS MAXprice INTO temp76  FROM quotes WHERE sym = 'RDSA' """))

db.make_query(""" SELECT 'RIG'::Text AS sym,   MAX(price) AS MAXprice INTO temp77  FROM quotes WHERE sym = 'RIG' """)
totalCost+= int(db.total_cost(""" SELECT 'RIG'::Text AS sym,   MAX(price) AS MAXprice INTO temp77  FROM quotes WHERE sym = 'RIG' """))

db.make_query(""" SELECT 'ROST'::Text AS sym,   MAX(price) AS MAXprice INTO temp78  FROM quotes WHERE sym = 'ROST' """)
totalCost+= int(db.total_cost(""" SELECT 'ROST'::Text AS sym,   MAX(price) AS MAXprice INTO temp78  FROM quotes WHERE sym = 'ROST' """))

db.make_query(""" SELECT 'RVM'::Text AS sym,   MAX(price) AS MAXprice INTO temp79  FROM quotes WHERE sym = 'RVM' """)
totalCost+= int(db.total_cost(""" SELECT 'RVM'::Text AS sym,   MAX(price) AS MAXprice INTO temp79  FROM quotes WHERE sym = 'RVM' """))

db.make_query(""" SELECT 'SIAL'::Text AS sym,   MAX(price) AS MAXprice INTO temp80  FROM quotes WHERE sym = 'SIAL' """)
totalCost+= int(db.total_cost(""" SELECT 'SIAL'::Text AS sym,   MAX(price) AS MAXprice INTO temp80  FROM quotes WHERE sym = 'SIAL' """))

db.make_query(""" SELECT 'SIRI'::Text AS sym,   MAX(price) AS MAXprice INTO temp81  FROM quotes WHERE sym = 'SIRI' """)
totalCost+= int(db.total_cost(""" SELECT 'SIRI'::Text AS sym,   MAX(price) AS MAXprice INTO temp81  FROM quotes WHERE sym = 'SIRI' """))

db.make_query(""" SELECT 'SNDK'::Text AS sym,   MAX(price) AS MAXprice INTO temp82  FROM quotes WHERE sym = 'SNDK' """)
totalCost+= int(db.total_cost(""" SELECT 'SNDK'::Text AS sym,   MAX(price) AS MAXprice INTO temp82  FROM quotes WHERE sym = 'SNDK' """))

db.make_query(""" SELECT 'T'::Text AS sym,   MAX(price) AS MAXprice INTO temp83  FROM quotes WHERE sym = 'T' """)
totalCost+= int(db.total_cost(""" SELECT 'T'::Text AS sym,   MAX(price) AS MAXprice INTO temp83  FROM quotes WHERE sym = 'T' """))

db.make_query(""" SELECT 'UTX'::Text AS sym,   MAX(price) AS MAXprice INTO temp84  FROM quotes WHERE sym = 'UTX' """)
totalCost+= int(db.total_cost(""" SELECT 'UTX'::Text AS sym,   MAX(price) AS MAXprice INTO temp84  FROM quotes WHERE sym = 'UTX' """))

db.make_query(""" SELECT 'V'::Text AS sym,   MAX(price) AS MAXprice INTO temp85  FROM quotes WHERE sym = 'V' """)
totalCost+= int(db.total_cost(""" SELECT 'V'::Text AS sym,   MAX(price) AS MAXprice INTO temp85  FROM quotes WHERE sym = 'V' """))

db.make_query(""" SELECT 'VZ'::Text AS sym,   MAX(price) AS MAXprice INTO temp86  FROM quotes WHERE sym = 'VZ' """)
totalCost+= int(db.total_cost(""" SELECT 'VZ'::Text AS sym,   MAX(price) AS MAXprice INTO temp86  FROM quotes WHERE sym = 'VZ' """))

db.make_query(""" SELECT 'WMT'::Text AS sym,   MAX(price) AS MAXprice INTO temp87  FROM quotes WHERE sym = 'WMT' """)
totalCost+= int(db.total_cost(""" SELECT 'WMT'::Text AS sym,   MAX(price) AS MAXprice INTO temp87  FROM quotes WHERE sym = 'WMT' """))

db.make_query(""" SELECT 'XOM'::Text AS sym,   MAX(price) AS MAXprice INTO temp88  FROM quotes WHERE sym = 'XOM' """)
totalCost+= int(db.total_cost(""" SELECT 'XOM'::Text AS sym,   MAX(price) AS MAXprice INTO temp88  FROM quotes WHERE sym = 'XOM' """))

db.make_query(""" SELECT 'ZION'::Text AS sym,   MAX(price) AS MAXprice INTO temp89  FROM quotes WHERE sym = 'ZION' """)
totalCost+= int(db.total_cost(""" SELECT 'ZION'::Text AS sym,   MAX(price) AS MAXprice INTO temp89  FROM quotes WHERE sym = 'ZION' """))

db.make_query(""" SELECT 'ZLCS'::Text AS sym,   MAX(price) AS MAXprice INTO temp90  FROM quotes WHERE sym = 'ZLCS' """)
totalCost+= int(db.total_cost(""" SELECT 'ZLCS'::Text AS sym,   MAX(price) AS MAXprice INTO temp90  FROM quotes WHERE sym = 'ZLCS' """))


#To find the union of the result sets
db.make_query(""" SELECT * INTO finaloutputTable FROM temp0 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" SELECT * INTO finaloutputTable FROM temp0 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp1 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp1 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp2 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp2 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp3 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp3 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp4 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp4 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp5 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp5 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp6 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp6 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp7 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp7 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp8 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp8 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp9 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp9 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp10 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp10 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp11 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp11 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp12 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp12 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp13 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp13 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp14 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp14 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp15 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp15 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp16 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp16 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp17 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp17 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp18 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp18 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp19 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp19 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp20 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp20 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp21 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp21 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp22 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp22 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp23 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp23 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp24 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp24 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp25 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp25 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp26 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp26 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp27 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp27 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp28 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp28 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp29 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp29 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp30 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp30 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp31 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp31 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp32 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp32 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp33 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp33 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp34 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp34 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp35 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp35 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp36 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp36 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp37 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp37 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp38 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp38 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp39 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp39 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp40 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp40 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp41 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp41 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp42 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp42 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp43 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp43 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp44 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp44 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp45 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp45 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp46 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp46 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp47 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp47 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp48 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp48 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp49 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp49 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp50 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp50 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp51 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp51 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp52 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp52 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp53 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp53 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp54 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp54 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp55 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp55 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp56 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp56 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp57 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp57 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp58 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp58 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp59 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp59 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp60 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp60 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp61 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp61 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp62 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp62 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp63 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp63 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp64 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp64 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp65 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp65 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp66 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp66 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp67 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp67 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp68 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp68 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp69 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp69 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp70 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp70 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp71 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp71 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp72 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp72 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp73 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp73 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp74 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp74 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp75 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp75 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp76 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp76 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp77 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp77 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp78 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp78 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp79 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp79 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp80 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp80 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp81 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp81 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp82 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp82 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp83 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp83 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp84 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp84 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp85 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp85 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp86 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp86 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp87 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp87 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp88 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp88 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp89 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp89 WHERE MAXprice IS NOT NULL"""))

db.make_query(""" INSERT INTO finaloutputTable  SELECT * FROM temp90 WHERE MAXprice IS NOT NULL""")
totalCost+= int(db.total_cost(""" INSERT INTO finaloutputTable  SELECT * FROM temp90 WHERE MAXprice IS NOT NULL"""))


#Final query
db.make_pquery("""SELECT sym,_MAXprice FROM finalOutputTable """)
totalCost+= int(db.total_cost("""SELECT sym,_MAXprice FROM finalOutputTable """))
print 'Cost to execute script is: %s' %(totalCost)
