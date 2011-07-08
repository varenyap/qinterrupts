drop table if exists d_name_cosi cascade;
drop table if exists d_name_hist cascade;
drop table if exists d_name_math cascade;
drop table if exists quotes cascade;
drop table if exists department cascade;
drop table if exists employee cascade;
create table department (id integer, name varchar(128));-- Table: department
create table employee (id integer,dept_id integer, name varchar(128), salary integer);-- Table: department
INSERT INTO department(id,name) VALUES(1,'COSI');-- insert departments
INSERT INTO department(id,name) VALUES(2,'HIST');
INSERT INTO department(id,name) VALUES(3,'MATH');
INSERT INTO employee(id, dept_id, name, salary) VALUES(1,1,'Tom Wilson',39802);--insert employees
INSERT INTO employee(id, dept_id, name, salary) VALUES(2,1,'Henry Thomas',93437);
INSERT INTO employee(id, dept_id, name, salary) VALUES(3,2,'Lisa Link Thomas',29372);
INSERT INTO employee(id, dept_id, name, salary) VALUES(4,2,'John Holden',87363);
INSERT INTO employee(id, dept_id, name, salary) VALUES(5,3,'Mike Hunter',459891);
INSERT INTO employee(id, dept_id, name, salary) VALUES(5,3,'Teresa Young',65478);
