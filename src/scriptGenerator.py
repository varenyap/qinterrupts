#===============================================================================
# scriptGenerator.py
# Creates insert statements for 'quotes' table having the day and price as 
# its attributes.
#===============================================================================
import random

filename = "quotesScript.sql"
FILE = open(filename,"w")
#FILE.write("--SELECT pg_size_pretty(pg_database_size('postgres'));\n")
FILE.write("drop table if exists quotes cascade;\n")
FILE.write("create table quotes (sym varchar(128), days integer,price integer);-- Table: quotes\n")

numDays = 5
stocks = ['MSFT', 'ERTS','MSN','EQIX','JST','PEP','ORCL', 'IBM',
          'GOOG', 'NFLX', 'BA', 'KO', 'EDC','MOS', 'OTT', 'CNOOC',
          'AGU','EVR','CTSH','ZION','ZLCS','RIG','RDSA','BAC','V',
          'LEN','MSPD','EWBC','BCE','BMC','AXP','T','CAT','CVX',
          'CSCO','GE', 'XOM','HD','INTC','JNJ','JPM','MCD','PG',
          'UTX','VZ','WMT','DIS','MMM','ODP','AKRX','KEYN','DK',
          'NGD','KCI','FSI','BAS','GENE','RVM','HSTM','HPQ','AA',
          'DD','ADBE','AMZN','APOL','ADSK','BIDU','BBBY','BRCM',
          'CTXS','CMCSA','DELL','EBAY','GRMN','INFY','EXPE',
          'INTU','LRCX','JOYG','ILMN','KLAC','NTAP','MYL','NIHD',
          'ORCL','ROST','SNDK','SIRI','SIAL','MCHP','MU','MAT']

stocks = ['MSFT', 'ERTS','MSN','IBM']
          
for stock in stocks:
    day = 1
    while(day<numDays+1):
        price = random.randint(0,1000)
        insertSt = "INSERT INTO QUOTES(sym,days,price) VALUES('" + str(stock) + "'," + str(day)+"," + str(price)+ ");\n"
        FILE.write(insertSt)
        day+=1

FILE.close()
print "File: %s Created" %filename
