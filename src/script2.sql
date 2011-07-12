drop table if exists d_name_hr cascade;
drop table if exists d_name_it cascade;
drop table if exists d_name_marketing cascade;
drop table if exists d_name_sales cascade;
drop table if exists quotes cascade;
drop table if exists department cascade;
drop table if exists employee cascade;
drop table if exists dept_manager cascade;
drop table if exists employee_skill cascade;
create table department (id integer, name varchar(128));-- Table: department
create table employee (id integer,dept_id integer, name varchar(128), salary integer);-- Table: employee
create table dept_manager (dept_id integer, emp_id integer, name varchar(128));--Table:department manager
create table employee_skill (emp_id integer,skill varchar(128)); --Table: employee skills
INSERT INTO department(id,name) VALUES(1,'HR');-- insert departments
INSERT INTO department(id,name) VALUES(2,'IT');
INSERT INTO department(id,name) VALUES(3,'Marketing');
INSERT INTO department(id,name) VALUES(4,'Sales');
INSERT INTO employee(id, dept_id, name, salary) VALUES(1,1,'Mary Tolson',45418);
INSERT INTO employee(id, dept_id, name, salary) VALUES(1,2,'Mary Tolson',45418);
INSERT INTO employee(id, dept_id, name, salary) VALUES(1,3,'Mary Tolson',45418);
INSERT INTO employee(id, dept_id, name, salary) VALUES(1,4,'Mary Tolson',45418);
INSERT INTO employee(id, dept_id, name, salary) VALUES(2,1,'Tom Wilson',39802);--insert employees
INSERT INTO employee(id, dept_id, name, salary) VALUES(3,1,'Henry Thomas',93437);
INSERT INTO employee(id, dept_id, name, salary) VALUES(4,2,'Lisa Link Thomas',29372);
INSERT INTO employee(id, dept_id, name, salary) VALUES(5,2,'John Holden',87363);
INSERT INTO employee(id, dept_id, name, salary) VALUES(6,3,'Mike Hunter',459891);
INSERT INTO employee(id, dept_id, name, salary) VALUES(7,3,'Teresa Young',65478);
INSERT INTO employee(id, dept_id, name, salary) VALUES(8,4,'Lewis Black',23547);
INSERT INTO employee(id, dept_id, name, salary) VALUES(9,4,'Caiti Lowe',18654);
INSERT INTO dept_manager(dept_id, emp_id,name) VALUES(1,2,'Tom Wilson');--insert into dept_manager
INSERT INTO dept_manager(dept_id, emp_id,name) VALUES(2,5,'John Holden');
INSERT INTO dept_manager(dept_id, emp_id,name) VALUES(3,1,'Mary Tolson');
INSERT INTO dept_manager(dept_id, emp_id,name) VALUES(4,8,'Lewis Black');
INSERT INTO employee_skill(emp_id, skill) VALUES(1,'Typing');--insert into employee skills
INSERT INTO employee_skill(emp_id, skill) VALUES(2,'Shorthand');
INSERT INTO employee_skill(emp_id, skill) VALUES(3,'Public Speaking');
INSERT INTO employee_skill(emp_id, skill) VALUES(4,'Shorthand');
INSERT INTO employee_skill(emp_id, skill) VALUES(5,'Typing');
INSERT INTO employee_skill(emp_id, skill) VALUES(6,'Shorthand');
INSERT INTO employee_skill(emp_id, skill) VALUES(7,'Shorthand');
INSERT INTO employee_skill(emp_id, skill) VALUES(8,'Typing');
INSERT INTO employee_skill(emp_id, skill) VALUES(9,'Public Speaking');
