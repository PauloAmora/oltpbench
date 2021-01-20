DROP TABLE IF EXISTS SYSTEM1;
DROP TABLE IF EXISTS SYSTEM2;
DROP TABLE IF EXISTS SYSTEM3;
DROP TABLE IF EXISTS MASTER_RECORD;
create table SYSTEM1
(
    id integer, 
    field0 varchar(100), 
    field1 varchar(100)
    );
create table SYSTEM2
(
    id integer, 
    field0 varchar(100), 
    field1 varchar(100)
    );
create table SYSTEM3
(
    id integer, 
    field0 varchar(100), 
    field1 varchar(100)
    );
create table MASTER_RECORD
(
    id integer, 
    system varchar(100), 
    id_system integer
    );
