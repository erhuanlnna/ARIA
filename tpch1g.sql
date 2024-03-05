-- Sccsid: @(#)dss.ri 2.1.8.1  
-- tpch Benchmark Version 8.0  
-- CONNECT TO tpch;  
-- Sccsid:     @(#)dss.ddl      2.1.8.1

DROP DATABASE IF EXISTS `tpch1g`;
CREATE DATABASE `tpch1g`;
CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       CHAR(25) NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152));

CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       CHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152));

CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
                          P_NAME        VARCHAR(55) NOT NULL,
                          P_MFGR        CHAR(25) NOT NULL,
                          P_BRAND       CHAR(10) NOT NULL,
                          P_TYPE        VARCHAR(25) NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   CHAR(10) NOT NULL,
                          P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                          P_COMMENT     VARCHAR(23) NOT NULL );

CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
                             S_NAME        CHAR(25) NOT NULL,
                             S_ADDRESS     VARCHAR(40) NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       CHAR(15) NOT NULL,
                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                             S_COMMENT     VARCHAR(101) NOT NULL);

CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL );

CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
                             C_NAME        VARCHAR(25) NOT NULL,
                             C_ADDRESS     VARCHAR(40) NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       CHAR(15) NOT NULL,
                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                             C_MKTSEGMENT  CHAR(10) NOT NULL,
                             C_COMMENT     VARCHAR(117) NOT NULL);

CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    CHAR(1) NOT NULL,
                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  CHAR(15) NOT NULL,
                           O_CLERK          CHAR(15) NOT NULL,
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL);

CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                             L_TAX         DECIMAL(15,2) NOT NULL,
                             L_RETURNFLAG  CHAR(1) NOT NULL,
                             L_LINESTATUS  CHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
                             L_SHIPMODE     CHAR(10) NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL);


-- ALTER TABLE REGION DROP PRIMARY KEY;  
-- ALTER TABLE NATION DROP PRIMARY KEY;  
-- ALTER TABLE PART DROP PRIMARY KEY;  
-- ALTER TABLE SUPPLIER DROP PRIMARY KEY;  
-- ALTER TABLE PARTSUPP DROP PRIMARY KEY;  
-- ALTER TABLE ORDERS DROP PRIMARY KEY;  
-- ALTER TABLE LINEITEM DROP PRIMARY KEY;  
-- ALTER TABLE CUSTOMER DROP PRIMARY KEY;  
-- For table REGION  
ALTER TABLE REGION  
ADD PRIMARY KEY (R_REGIONKEY);  
-- For table NATION  
ALTER TABLE NATION  
ADD PRIMARY KEY (N_NATIONKEY);  
ALTER TABLE NATION  
-- ADD FOREIGN KEY NATION_FK1 (N_REGIONKEY) references REGION;  
ADD FOREIGN KEY NATION_FK1 (N_REGIONKEY) references REGION(R_REGIONKEY);  
COMMIT WORK;  
-- For table PART  
ALTER TABLE PART  
ADD PRIMARY KEY (P_PARTKEY);  
COMMIT WORK;  
-- For table SUPPLIER  
ALTER TABLE SUPPLIER  
ADD PRIMARY KEY (S_SUPPKEY);  
ALTER TABLE SUPPLIER  
ADD FOREIGN KEY SUPPLIER_FK1 (S_NATIONKEY) references NATION(N_NATIONKEY);  
COMMIT WORK;  
-- For table PARTSUPP  
ALTER TABLE PARTSUPP  
ADD PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY);  
COMMIT WORK;  
-- For table CUSTOMER  
ALTER TABLE CUSTOMER  
ADD PRIMARY KEY (C_CUSTKEY);  
ALTER TABLE CUSTOMER  
ADD FOREIGN KEY CUSTOMER_FK1 (C_NATIONKEY) references NATION(N_NATIONKEY);  
COMMIT WORK;  
-- For table LINEITEM  
ALTER TABLE LINEITEM  
ADD PRIMARY KEY (L_ORDERKEY,L_LINENUMBER);  
COMMIT WORK;  
-- For table ORDERS  
ALTER TABLE ORDERS  
ADD PRIMARY KEY (O_ORDERKEY);  
COMMIT WORK;  
-- For table PARTSUPP  
ALTER TABLE PARTSUPP  
ADD FOREIGN KEY PARTSUPP_FK1 (PS_SUPPKEY) references SUPPLIER(S_SUPPKEY);  
COMMIT WORK;  
ALTER TABLE PARTSUPP  
ADD FOREIGN KEY PARTSUPP_FK2 (PS_PARTKEY) references PART(P_PARTKEY);  
COMMIT WORK;  
-- For table ORDERS  
ALTER TABLE ORDERS  
ADD FOREIGN KEY ORDERS_FK1 (O_CUSTKEY) references CUSTOMER(C_CUSTKEY);  
COMMIT WORK;  
-- For table LINEITEM  
ALTER TABLE LINEITEM  
ADD FOREIGN KEY LINEITEM_FK1 (L_ORDERKEY) references ORDERS(O_ORDERKEY);  
COMMIT WORK;  
ALTER TABLE LINEITEM  
ADD FOREIGN KEY LINEITEM_FK2 (L_PARTKEY,L_SUPPKEY) references  
PARTSUPP(PS_PARTKEY,PS_SUPPKEY);  
alter table CUSTOMER rename to customer ;  
alter table LINEITEM rename to lineitem ;  
alter table NATION rename to nation ;  
alter table ORDERS rename to orders ;  
alter table PART rename to part ;  
alter table PARTSUPP rename to partsupp ;  
alter table REGION rename to region ;  
alter table SUPPLIER rename to supplier ;  
COMMIT WORK;

LOAD DATA LOCAL INFILE '/home/phh/data/tpch_2_17_0/dbgen/region.tbl'     INTO TABLE region     FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/home/phh/data/tpch_2_17_0/dbgen/nation.tbl'     INTO TABLE nation     FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/home/phh/data/tpch_2_17_0/dbgen/customer.tbl' INTO TABLE customer   FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/home/phh/data/tpch_2_17_0/dbgen/supplier.tbl' INTO TABLE supplier   FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/home/phh/data/tpch_2_17_0/dbgen/part.tbl'         INTO TABLE part       FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/home/phh/data/tpch_2_17_0/dbgen/partsupp.tbl' INTO TABLE partsupp   FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/home/phh/data/tpch_2_17_0/dbgen/orders.tbl'     INTO TABLE orders     FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/home/phh/data/tpch_2_17_0/dbgen/lineitem.tbl' INTO TABLE lineitem   FIELDS TERMINATED BY '|';


alter table customer
add column aID INT NOT NULL;
set @aID = 0;
UPDATE customer SET aID=(@aID:=@aID+1);

alter table lineitem
add column aID INT NOT NULL;
set @aID = 0;
UPDATE lineitem SET aID=(@aID:=@aID+1);

alter table nation
add column aID INT NOT NULL;
set @aID = 0;
UPDATE nation SET aID=(@aID:=@aID+1);

alter table orders
add column aID INT NOT NULL;
set @aID = 0;
UPDATE orders SET aID=(@aID:=@aID+1);

alter table part
add column aID INT NOT NULL;
set @aID = 0;
UPDATE part SET aID=(@aID:=@aID+1);

alter table partsupp
add column aID INT NOT NULL;
set @aID = 0;
UPDATE partsupp SET aID=(@aID:=@aID+1);

alter table region
add column aID INT NOT NULL;
set @aID = 0;
UPDATE region SET aID=(@aID:=@aID+1);

alter table supplier
add column aID INT NOT NULL;
set @aID = 0;
UPDATE supplier SET aID=(@aID:=@aID+1);

create index aid_index on customer (aID);
create index aid_index on lineitem (aID);
create index aid_index on nation (aID);
create index aid_index on orders (aID);
create index aid_index on part (aID);
create index aid_index on partsupp (aID);
create index aid_index on region (aID);
create index aid_index on supplier (aID);

CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                            N_NAME       CHAR(25) NOT NULL,
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152));

CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                            R_NAME       CHAR(25) NOT NULL,
                            R_COMMENT    VARCHAR(152));

CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
                          P_NAME        VARCHAR(55) NOT NULL,
                          P_MFGR        CHAR(25) NOT NULL,
                          P_BRAND       CHAR(10) NOT NULL,
                          P_TYPE        VARCHAR(25) NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   CHAR(10) NOT NULL,
                          P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                          P_COMMENT     VARCHAR(23) NOT NULL );

CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
                             S_NAME        CHAR(25) NOT NULL,
                             S_ADDRESS     VARCHAR(40) NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       CHAR(15) NOT NULL,
                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                             S_COMMENT     VARCHAR(101) NOT NULL);

CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL );

CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
                             C_NAME        VARCHAR(25) NOT NULL,
                             C_ADDRESS     VARCHAR(40) NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       CHAR(15) NOT NULL,
                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                             C_MKTSEGMENT  CHAR(10) NOT NULL,
                             C_COMMENT     VARCHAR(117) NOT NULL);

CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    CHAR(1) NOT NULL,
                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  CHAR(15) NOT NULL,
                           O_CLERK          CHAR(15) NOT NULL,
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL);

CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    DECIMAL(15,2) NOT NULL,
                             L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                             L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                             L_TAX         DECIMAL(15,2) NOT NULL,
                             L_RETURNFLAG  CHAR(1) NOT NULL,
                             L_LINESTATUS  CHAR(1) NOT NULL,
                             L_SHIPDATE    DATE NOT NULL,
                             L_COMMITDATE  DATE NOT NULL,
                             L_RECEIPTDATE DATE NOT NULL,
                             L_SHIPINSTRUCT CHAR(25) NOT NULL,
                             L_SHIPMODE     CHAR(10) NOT NULL,
                             L_COMMENT      VARCHAR(44) NOT NULL);