# ARIA

We describe how to employ ARIA to price the select-project-join (SPJ) and aggregate queries on the MySQL database server.
This README provides instructions on how to set up and run the default experiments, including data preparation, environment setup, and execution of experiments. Additionally, we guide you through various experiment variations, such as adjusting the support size, scale factor, and pricing queries with different selectivities, limit numbers, distinct, and aggregate queries.

# Getting Started
1. **Environment Setup**

   Before running any experiments, ensure your environment is correctly set up. This project requires:
- Python: 3.7
- MySQL Server (5.7 or newer)
- Necessary libraries:
  - mysql
  - mysql-connector-python
  - mysqlclient
  - pymysql
  - pandas
  - sqlglot
  
All libraries are provided in the requirements.txt.
Make sure Python, MySQL, and other libraries are properly installed and configured on your system. It's also recommended to create a virtual environment for Python dependencies to avoid any conflicts with other projects.

2. **Data Preparation**

**Downloading/Generating Datasets**:

There are four datasets used in the experiments:
- [World](https://dev.mysql.com/doc/world-setup/en/)
- [MovieLens 1 M](https://grouplens.org/datasets/movielens/1m/)
- [TPC-H](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
- [SSB](https://github.com/eyalroz/ssb-dbgen)

You can download or generate these datasets from their respective sources online. Below are the steps to prepare your data:
- For World, we have provided the SQL file (`world.sql`) for the World dataset. You can directly import them into your MySQL server.
- For the other datasets (MovieLens 1M, TPC-H and SSB), follow the instructions on their respective websites to generate the datasets.

**Importing Datasets into MySQL**:

After preparing your datasets, import them into your MySQL server. Use the MySQL command line to execute the SQL files corresponding to each dataset. For example, to import the World dataset, you might use a command like the following:
```
mysql -u username -p database_name < path/to/world.sql
```
Replace username, database_name, and path/to/movie.sql with your MySQL username, the name of the database where you want to import the data, and the path to the SQL file, respectively.
Note that, the name of data file in the sql file should correspond to your own path.

3. **Run the Experiments**
To run the experiments:

- Open the main.py file.
- Specify the database name and the SQL list containing all SQL queries for the experiments.
- Execute `main.py` to start the experiments and observe the performance of our pricing framework, ARIA.
Example command to run the experiments:
```bash
python main.py
```
This will run the default experiments and output the prices of queries over World dataset based on the pricing framework ARIA.

# Advanced Experimentation

## Varying the Support Size

For the World and MovieLens 1M datasets, you can directly run the `exp-size.py` to observe the corresponding experimental results.
For the TPC-H and SSB datasets, follow these steps to generate the support sets with different sizes and then run `exp-size.py`.
1. Run the file `generate_support_set.py` to generate the support sets on the database.
```python
python generate_support_set.py -d database_name
```

2. Import these generated support sets into MySQL with the following codes.
```MySQL
-- Create each table xxxx_ar_support_all for each table xxx in the database.
-- For example, in the database Movies, you have to execute these codes.
create table users_ar_support_all like USERS;
alter table users_ar_support_all add column aID INT NOT NULL;
create table ratings_ar_support_all like RATINGS;
alter table ratings_ar_support_all add column aID INT NOT NULL;
create table movies_ar_support_all like MOVIES;
alter table movies_ar_support_all add column aID INT NOT NULL;
--- Import the generated data into the database
LOAD DATA LOCAL INFILE '/path/to/movies_movies_ar_support_all.tbl' INTO TABLE movies_ar_support_all   FIELDS TERMINATED BY ';';
LOAD DATA LOCAL INFILE '/path/to/movies_ratings_ar_support_all.tbl' INTO TABLE ratings_ar_support_all   FIELDS TERMINATED BY ';';
LOAD DATA LOCAL INFILE '/path/to/movie/movies_users_ar_support_all.tbl' INTO TABLE users_ar_support_all   FIELDS TERMINATED BY ';';
```
3. Run the `exp-size.py`.
```python
python exp-size.py
```
If you want to run the experiments on Movielens/TPC-H/SSB datasets, change the variable `db` and `sql_list` as the corresponding database and the priced queries.
The `domain_list` variable should be specified as that on the corresponding database and the `create_db` should be set as True to automatically create different sizes of support sets in the database.
## Varying the Scale Factor

1. For TPC-H and SSB datasets, generate the datasets with different scale factors. (Do the same work in **Data Preparation**)
2. Run the `exp-sf.py`
Note that, one can use the `Utils.py` to replace `dbUtils.py` on these two datasets.
This is because, the `mysql.connector` library is faster than `pymysql` library in communicating with MySQL, especially when the query results are large.

## Pricing Queries with Different Selectivities
Run `exp-selection-world.py` for World dataset and `exp-selection-movies` for MovieLens 1M dataset.
## Pricing Limit Queries with Different Limit Numbers
Run `exp-limit-world.py` for World dataset and `exp-limit-movies` for MovieLens 1M dataset.
## Pricing Distinct Queries
Run `exp-distinct-world.py` for World dataset and `exp-distinct-movies` for MovieLens 1M dataset.
## Pricing Aggregate Queries
Run `exp-agg-world.py` for World dataset and `exp-agg-movies` for MovieLens 1M dataset.
