drop table if exists quotes cascade;
drop table if exists output cascade;
drop table if exists q_ibm cascade;
drop table if exists q_orcl cascade;
drop table if exists q_msft cascade;
drop table if exists q_max_overall cascade;
create table quotes (sym varchar(128), day integer,price integer);-- Table: quotes
create table output(sym varchar(128), day integer, dayjump integer);-- Table: output
INSERT INTO quotes(sym,day,price) VALUES('MSFT',1,30);-- stock inserts week1
INSERT INTO quotes(sym,day,price) VALUES('MSFT',2,70);
INSERT INTO quotes(sym,day,price) VALUES('MSFT',3,10);
INSERT INTO quotes(sym,day,price) VALUES('MSFT',4,1000);
INSERT INTO quotes(sym,day,price) VALUES('MSFT',5,50);
INSERT INTO quotes(sym,day,price) VALUES('ORCL',1,890);
INSERT INTO quotes(sym,day,price) VALUES('ORCL',2,74);
INSERT INTO quotes(sym,day,price) VALUES('ORCL',3,23);
INSERT INTO quotes(sym,day,price) VALUES('ORCL',4,87);
INSERT INTO quotes(sym,day,price) VALUES('ORCL',5,650);
INSERT INTO quotes(sym,day,price) VALUES('IBM',1,330);
INSERT INTO quotes(sym,day,price) VALUES('IBM',2,80);
INSERT INTO quotes(sym,day,price) VALUES('IBM',3,34);
INSERT INTO quotes(sym,day,price) VALUES('IBM',4,760);
INSERT INTO quotes(sym,day,price) VALUES('IBM',5,5);
INSERT INTO quotes(sym,day,price) VALUES('MSFT',6,300);-- stock inserts week2
INSERT INTO quotes(sym,day,price) VALUES('MSFT',7,750);
INSERT INTO quotes(sym,day,price) VALUES('MSFT',8,34);
INSERT INTO quotes(sym,day,price) VALUES('MSFT',9,45);
INSERT INTO quotes(sym,day,price) VALUES('MSFT',10,550);
INSERT INTO quotes(sym,day,price) VALUES('ORCL',6,230);
INSERT INTO quotes(sym,day,price) VALUES('ORCL',7,156);
INSERT INTO quotes(sym,day,price) VALUES('ORCL',8,126);
INSERT INTO quotes(sym,day,price) VALUES('ORCL',9,451);
INSERT INTO quotes(sym,day,price) VALUES('ORCL',10,559);
INSERT INTO quotes(sym,day,price) VALUES('IBM',6,1200);
INSERT INTO quotes(sym,day,price) VALUES('IBM',7,420);
INSERT INTO quotes(sym,day,price) VALUES('IBM',8,84);
INSERT INTO quotes(sym,day,price) VALUES('IBM',9,49);
INSERT INTO quotes(sym,day,price) VALUES('IBM',10,500);
