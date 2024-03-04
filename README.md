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
- World
- MovieLens 1 M
- [TPC-H](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
- [SSB](https://github.com/eyalroz/ssb-dbgen)

You can download or generate these datasets from their respective sources online. Below are the steps to prepare your data:
- For World, we have provided a SQL file (`world.sql`). You can directly import this into your MySQL server.
- For the other datasets (MovieLens 1M, TPC-H, SSB), follow the instructions on their respective websites to download or generate the datasets.

**Importing Datasets into MySQL**:

After preparing your datasets, import them into your MySQL server. Use the MySQL command line to execute the SQL files corresponding to each dataset. For example, to import the MovieLens 1M dataset, you might use a command like the following:
```
mysql -u username -p database_name < path/to/movie.sql
```
Replace username, database_name, and path/to/movie.sql with your MySQL username, the name of the database where you want to import the data, and the path to the SQL file, respectively.
Note that, the name of data file in the sql file should correspond to you own path.

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

## Varying the Scale Factor

## Pricing Queries with Different Selectivities

## Pricing Limit Queries with Different Limit Numbers

## Pricing Distinct Queries

## Pricing Aggregate Queries

