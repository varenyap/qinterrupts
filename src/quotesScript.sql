drop table if exists quotes cascade;
create table quotes (sym varchar(128), days integer,price integer);-- Table: quotes
INSERT INTO QUOTES(sym,days,price) VALUES('MSFT',1,178);
INSERT INTO QUOTES(sym,days,price) VALUES('MSFT',2,469);
INSERT INTO QUOTES(sym,days,price) VALUES('MSFT',3,357);
INSERT INTO QUOTES(sym,days,price) VALUES('MSFT',4,28);
INSERT INTO QUOTES(sym,days,price) VALUES('ORCL',1,429);
INSERT INTO QUOTES(sym,days,price) VALUES('ORCL',2,454);
INSERT INTO QUOTES(sym,days,price) VALUES('ORCL',3,887);
INSERT INTO QUOTES(sym,days,price) VALUES('ORCL',4,880);
INSERT INTO QUOTES(sym,days,price) VALUES('IBM',1,2);
INSERT INTO QUOTES(sym,days,price) VALUES('IBM',2,866);
INSERT INTO QUOTES(sym,days,price) VALUES('IBM',3,155);
INSERT INTO QUOTES(sym,days,price) VALUES('IBM',4,299);
