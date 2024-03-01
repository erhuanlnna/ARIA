# ARIA

We describe how to employ ARIA to price the select-project-join (SPJ) and aggregate queries on the MySQL database server.


# Preparation
## Environments
1. Python: 3.7
2. Necessary libraries:
  - mysql
  - mysql-connector-python
  - mysqlclient
  - pymysql
  - pandas
  - sqlglot
3. All libraries are provided in the requirements.txt.
## Preparing data
There are four datasets used in the experiments, including the world, MovieLens 1M, SSB, and TPC-H.
One can download (or generate) each database from the database source and import the database into MySQL server.
After that, we add a new column `aID` for each table in the database.
Here are the corresponding codes used in MySQL.

## Generate the support sets

# Experiments

## Results in the default case

## Results of varying the support size

## Results of pricing queries with different selectivities

## Results of pricing queries with different limit numbers 

## Results of pricing queries with the distinct clause on different attributes

## Results of pricing different aggregate queries

## Results of varying the scale factor on SSB and TPCH
