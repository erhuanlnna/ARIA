DROP DATABASE IF EXISTS `ssb1g`;
CREATE DATABASE `ssb1g`;

create table CUSTOMER (
c_custkey     integer,
c_name        varchar(25) not null,
c_address     varchar(40) not null,
c_city        varchar(10) not null,
c_nation      varchar(15) not null,
c_region      varchar(12) not null,
c_phone       varchar(15) not null,
c_mktsegment  varchar(10) not null);

create table DATES (
d_datekey          integer,
d_date             varchar(18) not null,
d_dayofweek        varchar(18) not null,
d_month            varchar(9) not null,
d_year             integer not null, 
d_yearmonthnum     integer,
d_yearmonth        varchar(7) not null,
d_daynuminweek     integer,
d_daynuminmonth    integer,
d_daynuminyear     integer,
d_monthnuminyear   integer,
d_weeknuminyear    integer,
d_sellingseason    varchar(12) not null,
d_lastdayinweekfl  integer,
d_lastdayinmonthfl integer,
d_holidayfl        integer,
d_weekdayfl        integer);

create table PART  (
p_partkey     integer,
p_name        varchar(22) not null,
p_mfgr        varchar(6) not null,
p_category    varchar(7) not null,
p_brand       varchar(9) not null,
p_color       varchar(11) not null,
p_type        varchar(25) not null,
p_size        integer not null,
p_container   varchar(10) not null);

create table SUPPLIER (
s_suppkey     integer,
s_name        varchar(25) not null,
s_address     varchar(25) not null,
s_city        varchar(10) not null,
s_nation      varchar(15) not null,
s_region      varchar(12) not null,
s_phone       varchar(15) not null);

create table LINEORDER (
lo_orderkey       bigint,
lo_linenumber     bigint,
lo_custkey        integer not null,
lo_partkey        integer not null,
lo_suppkey        integer not null,
lo_orderdate      integer not null,
lo_orderpriotity  varchar(15) not null,
lo_shippriotity   integer,
lo_quantity       bigint,
lo_extendedprice  bigint,
lo_ordtotalprice  bigint,
lo_discount       bigint,
lo_revenue        bigint,
lo_supplycost     bigint,
lo_tax            bigint,
lo_commitdate     integer not null,
lo_shipmode       varchar(10) not null);

create table customer (
c_custkey     integer,
c_name        varchar(25) not null,
c_address     varchar(40) not null,
c_city        varchar(10) not null,
c_nation      varchar(15) not null,
c_region      varchar(12) not null,
c_phone       varchar(15) not null,
c_mktsegment  varchar(10) not null);

create table dates (
d_datekey          integer,
d_date             varchar(18) not null,
d_dayofweek        varchar(18) not null,
d_month            varchar(9) not null,
d_year             integer not null, 
d_yearmonthnum     integer,
d_yearmonth        varchar(7) not null,
d_daynuminweek     integer,
d_daynuminmonth    integer,
d_daynuminyear     integer,
d_monthnuminyear   integer,
d_weeknuminyear    integer,
d_sellingseason    varchar(12) not null,
d_lastdayinweekfl  integer,
d_lastdayinmonthfl integer,
d_holidayfl        integer,
d_weekdayfl        integer);

create table part  (
p_partkey     integer,
p_name        varchar(22) not null,
p_mfgr        varchar(6) not null,
p_category    varchar(7) not null,
p_brand       varchar(9) not null,
p_color       varchar(11) not null,
p_type        varchar(25) not null,
p_size        integer not null,
p_container   varchar(10) not null);

create table supplier (
s_suppkey     integer,
s_name        varchar(25) not null,
s_address     varchar(25) not null,
s_city        varchar(10) not null,
s_nation      varchar(15) not null,
s_region      varchar(12) not null,
s_phone       varchar(15) not null);

create table lineorder (
lo_orderkey       bigint,
lo_linenumber     bigint,
lo_custkey        integer not null,
lo_partkey        integer not null,
lo_suppkey        integer not null,
lo_orderdate      integer not null,
lo_orderpriotity  varchar(15) not null,
lo_shippriotity   integer,
lo_quantity       bigint,
lo_extendedprice  bigint,
lo_ordtotalprice  bigint,
lo_discount       bigint,
lo_revenue        bigint,
lo_supplycost     bigint,
lo_tax            bigint,
lo_commitdate     integer not null,
lo_shipmode       varchar(10) not null);


load data LOCAL infile '/data/phh/SIPricer/ssb-data-eyalroz/ssb-dbgen/date.tbl' into table dates fields terminated by '|' lines terminated by '|\n';
commit;

load data LOCAL infile '/data/phh/SIPricer/ssb-data-eyalroz/ssb-dbgen/lineorder.tbl' into table lineorder fields terminated by '|' lines terminated by '|\n';
commit;

load data LOCAL infile '/data/phh/SIPricer/ssb-data-eyalroz/ssb-dbgen/supplier.tbl' into table supplier fields terminated by '|' lines terminated by '|\n';
commit;

load data LOCAL infile '/data/phh/SIPricer/ssb-data-eyalroz/ssb-dbgen/customer.tbl' into table customer fields terminated by '|' lines terminated by '|\n';
commit;

load data LOCAL infile '/data/phh/SIPricer/ssb-data-eyalroz/ssb-dbgen/part.tbl' into table part fields terminated by '|' lines terminated by '|\n';
commit;



alter table dates add primary key (d_datekey);
alter table dates add index d_year(d_year);

alter table part add primary key (p_partkey);
alter table part add index p_brand(p_brand);

alter table supplier add primary key (s_suppkey);
alter table supplier add index s_city(s_city);
alter table supplier add index s_nation(s_nation);
alter table supplier add index s_region(s_region);

alter table customer add primary key (c_custkey);
alter table customer add index c_nation(c_nation);
alter table customer add index c_region(c_region);

alter table lineorder add index lo_orderkey(lo_orderkey);
alter table lineorder add index lo_linenumber(lo_linenumber);
alter table lineorder add index lo_custkey(lo_custkey);
alter table lineorder add index lo_partkey(lo_partkey);
alter table lineorder add index lo_suppkey(lo_suppkey);
alter table lineorder add index lo_orderdate(lo_orderdate);
alter table lineorder add index lo_revenue(lo_revenue);
alter table lineorder add index lo_supplycost(lo_supplycost);



alter table customer add column aID INT NOT NULL;
set @aID = 0;
UPDATE customer SET aID=(@aID:=@aID+1);
create index aid_index on customer (aID);

alter table dates add column aID INT NOT NULL;
set @aID = 0;
UPDATE dates SET aID=(@aID:=@aID+1);
create index aid_index on dates (aID);

alter table lineorder add column aID INT NOT NULL;
set @aID = 0;
UPDATE lineorder SET aID=(@aID:=@aID+1);
create index aid_index on lineorder (aID);

alter table part add column aID INT NOT NULL;
set @aID = 0;
UPDATE part SET aID=(@aID:=@aID+1);
create index aid_index on part (aID);

alter table supplier add column aID INT NOT NULL;
set @aID = 0;
UPDATE supplier SET aID=(@aID:=@aID+1);
create index aid_index on supplier (aID);

