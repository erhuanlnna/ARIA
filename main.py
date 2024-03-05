
from MyPricer import Pricer
from dbUtils import *
from collections import defaultdict
import time
import pandas as pd

db = "world"
exp = "compare-" + db
sql_list =[
    # "select * from city",
    "select count(Name) from country where Continent = 'Asia'",
    "select avg(Population) from country",
    "select max(Population) from country",
    "select min(LifeExpectancy) from country",
    "select count(Name) from country where Name like 'A%'",
    "select Region, max(SurfaceArea) from country group by Region",
    "select Continent, max(Population) from country group by Continent",
    "select Continent, count(*) from country group by Continent",
    "select * from country where Continent='Europe' and Population between 10000000 and 20000000",
    "select * from country where Continent='Europe' limit 2",
    "select distinct GovernmentForm from country",
    "select * from city where Population >= 1000000 and CountryCode = 'USA'",
    "select Language, count(*) from countrylanguage group by Language",
    "select CountryCode, sum(Population) from city group by CountryCode",
    "select CountryCode, count(*) from city group by CountryCode",
    "select distinct 1 from city where CountryCode = 'USA' and Population > 10000000",
    "select Name, Code, CountryCode from country, countrylanguage where Code = CountryCode and Language = 'GREEK' and Percentage >= 50",
    "select district, country.Capital, city.ID from country, city where Code = 'USA' and country.Capital = city.ID",
    "select * from country, countrylanguage where Code = CountryCode and Language = 'Spanish'",
    "select * from country, countrylanguage where Code = CountryCode ",
]
domain_list = {"countrylanguage.Percentage": [0, 100], # use its meaning
            "city.Population": [0, 20000000], # use the maximum value 10500000 (int type is to large)
            "country.SurfaceArea": [0, 20000000], # [0.4, 17075400.00] decimal(10,2)
            "country.IndepYear": [-2000, 3000], # [-1523, 1994] smallint(6) 
            "country.Population": [0, 2000000000], # [0, 1277558000], int(11)
            "country.LifeExpectancy": [0, 100], # 37.2, 83.5
            "country.GNP": [0, 10000000], # 0, 8510700
            "country.GNPOld": [0, 10000000] # 157,8110900.00
            # other columns with int type is the ID type, which do not have the avg computation
            }

# For MovieLens 1M dataset
#db = "movies"
#sql_list = [
#    'select distinct genres from movies',
#    'select * from movies where title like "%2000%"',
#    'select count(rating) from ratings where rating > 2',
#    'select gender, max(age) from users where occupation > 10 group by gender' ,
#    'select zipcode, avg(age) from users where age between 20 and 40 group by zipcode',
#    'select movies.movieID, ratings.movieID, genres, rating from movies, ratings where movies.movieID = ratings.movieID and genres like "Comedy%"',
#    'select * from users, ratings where users.userID = ratings.userID and rating > 3 limit 100',
#    'select * from users, movies, ratings where users.userID = ratings.userID and movies.movieID = ratings.movieID and (rating >=3 or genres like "Comedy%")' ,
#]
#domain_list = {"ratings.rating": [0, 5], "users.age": [0, 56]}
#
# For SSB dataset
#db = "ssb1g"
#sql_list = [
#    "select lo_orderdate, d_datekey, lo_orderkey from lineorder, dates where lo_orderdate = d_datekey and d_year = '1995' and lo_discount between 1 and 10 and lo_quantity < 50;",
#    "select lo_orderdate, d_datekey, lo_orderkey from lineorder, dates where lo_orderdate = d_datekey and d_yearmonthnum = '199401' and lo_discount between 4 and 6 and lo_quantity between 26 and 35;",
#    "select lo_orderdate, d_datekey, lo_orderkey from lineorder, dates where lo_orderdate = d_datekey and d_weeknuminyear = 6 and d_year = 1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35;",
#    "select lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey, d_year, p_brand from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand between 'MFGR#2220' and 'MFGR#2229' and s_region = 'ASIA';",
#    "select lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey, d_year, p_brand from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand = 'MFGR#2221' and s_region = 'EUROPE';",
#    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, d_year, c_city, s_city from lineorder, dates, customer, supplier where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and d_year between 1992 and 1997;",
#    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, d_year, c_city, s_city from lineorder, dates, customer, supplier where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and (c_city = 'UNITED KI5' or c_city = 'UNITED KI1') and (s_city = 'UNITED KI5' or s_city = 'UNITED KI1') and d_yearmonth = 'Dec1997';",
#    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_partkey, p_partkey, c_nation, d_year from lineorder, dates, customer, supplier, part where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and c_region = 'AMERICA' and c_region = 'AMERICA' and s_nation = 'UNITED STATES' and p_category = 'MFGR#14' and (d_year = 1997 or d_year = 1998);"
#] 
#domain_list = None
#
# For TPC-H dataset
#db = "tpch1g"
#query1 = 'select o_orderkey, o_orderpriority, l_commitdate, l_receiptdate from orders, lineitem where   o_orderdate >= date \'1993-07-01\'   and o_orderdate < date \'1993-07-01\' + interval \'3\' month   and l_orderkey = o_orderkey   and l_commitdate < l_receiptdate; ' 
#query2 = 'select  l_orderkey, o_orderkey,c_custkey,o_custkey, l_suppkey, s_suppkey,c_nationkey, n_nationkey,s_nationkey, n_regionkey, r_regionkey from   customer,   orders,   lineitem,   supplier,   nation,   region where   c_custkey = o_custkey   and l_orderkey = o_orderkey   and l_suppkey = s_suppkey   and c_nationkey = s_nationkey   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey   and r_name = \'ASIA\'   and o_orderdate >= date \'1994-01-01\'   and o_orderdate < date \'1994-01-01\' + interval \'1\' year' 
#query3 = 'select  l_shipdate  from   lineitem where   l_shipdate >= date \'1994-01-01\'   and l_shipdate < date \'1994-01-01\' + interval \'1\' year   and l_discount between 0.05 and 0.07   and l_quantity < 24; ' 
#query4 = 'select o_orderkey,l_orderkey, l_shipmode,o_orderpriority   from   orders,   lineitem where   o_orderkey = l_orderkey   and (l_shipmode = \'MAIL\' or l_shipmode = \'SHIP\')   and l_commitdate < l_receiptdate   and l_shipdate < l_commitdate   and l_receiptdate >= date \'1994-01-01\'   and l_receiptdate < date \'1994-01-01\' + interval \'1\' year' 
#query5 = 'select   ps_partkey, p_partkey, p_type from   partsupp,   part where   p_partkey = ps_partkey   and p_brand <> \'Brand#45\'   and p_type not like \'MEDIUM POLISHED%\'   and p_size in (49, 14, 23, 45, 19, 3, 36, 9)' 
#query6 = 'select  l_partkey, p_partkey from   lineitem,   part where   p_partkey = l_partkey   and p_brand = \'Brand#23\'   and p_container = \'MED BOX\'   and l_quantity < 5.10736 ' 
#query7 = 'select *  from   lineitem where   l_shipdate <= date \'1998-09-01\'' 
#query8 = 'select p_partkey,ps_partkey,s_suppkey,ps_suppkey,s_nationkey,n_nationkey,r_regionkey,s_acctbal,s_name,n_name   from   part,   supplier,   partsupp,   nation,   region where   p_partkey = ps_partkey   and s_suppkey = ps_suppkey   and p_size = 15   and p_type like \'%BRASS\'   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey   and r_name = \'EUROPE\'   and ps_supplycost = 100 limit 100; ' 
#sql_list = [query1, query2, query3, query4, query5, query6, query7, query8] 
#domain_list = None

repeat_num = 5
result_list = []
sql_idx_list = [i+1 for i in range(len(sql_list))]

methods = ["Query", "ARIA"]
avg_time_list = defaultdict(list)
avg_price_list = defaultdict(list)
time_results = defaultdict(list)
price_results = defaultdict(list)
print("------------------------")
table_list, table_size_list, table_fields = get_fields_of_all_tables(database=db)
tuple_price_list = defaultdict(int)
history = None

history_aware = False
for table in table_list:
    tuple_price_list[table] = 1
for m in methods:
    if(m == "ARIA"):
        support_size_list = table_size_list
        support_suffix = ""
        pricer = Pricer(db, table_size_list, support_size_list, history, tuple_price_list, table_fields, domain_list, support_suffix, history_aware)
    for sql in sql_list:
        price_list = []
        time_list = []
        for i in range(repeat_num):
            if(m != "Query"):
                start_time = time.time()
                price = pricer.price_SQL_query(sql)
                # price = 0
                end_time = time.time()
                price_list.append(price)
                time_list.append(end_time - start_time)
            else:
                start_time = time.time()
                rs = select(sql, database=db)
                # price = 0
                end_time = time.time()
                time_list.append(end_time - start_time)
                price_list.append(0)
        price_results[m].append(sum(price_list)/len(price_list))
        time_results[m].append(sum(time_list)/len(time_list))
        # print(m, price_list[-1], time_list[-1], sql)
        
    avg_price_list[m].append(sum(price_results[m])/len(price_results[m]))
    avg_time_list[m].append(sum(time_results[m])/len(time_results[m]))
    # print(m, avg_price_list[m][-1], avg_time_list[m][-1])

print(time_results)
print(price_results)
print(avg_price_list)
print(avg_time_list)
file_name = "./rs/" + exp + ".csv"
all_results = {}
all_results["Query-Time"] = avg_time_list['Query']
all_results["ARIA-Time"] = avg_time_list['ARIA']
all_results["ARIA-Price"] = avg_price_list['ARIA']
print(all_results)

df = pd.DataFrame(all_results)
df.to_csv(file_name)
        
file_name = "./rs/" + exp + ".xlsx"
writer = pd.ExcelWriter(file_name)            

df = pd.DataFrame(time_results)
df.to_excel(writer, sheet_name= "Time", index= True)

df = pd.DataFrame(price_results)
df.to_excel(writer, sheet_name= "Price", index= True)


writer.save()
writer.close()





