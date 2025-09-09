create database test;
show databases;
use test;
show tables;
create table user(sno char(4),sname char(9));
insert into user values("1","小明");
insert into user values("2","小红");
insert into user values("3","小强");
select * from user;

update user set sname = "小强强" where sno = "3";

select * from user;




