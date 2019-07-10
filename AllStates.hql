--use existing databases

show databases;
use partition;

-- do non strict mode for dynamic partition

set hive.exec.dynamic.partition.mode=nonstrict;


-- create fist state table


create table state(state string, district string, enrollments int) row format delimited fields terminated by ',' ;

-- load data into state

load data local inpath '/home/cloudera/hive_project/AllStates.csv' into table state;


-- create dynamic partition table 

create table state_part_dynamic(district string,enrollments int) partitioned by(state string);

-- load data into partiton table

insert into table state_part_dynamic partition(state) select district , enrollments ,state from state;


-- select *from state where state = 'Andhra_Pradesh';
-- select *from state_part where state = 'Andhra_Pradesh';

-- end query
