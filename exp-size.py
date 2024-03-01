from MyPricer import Pricer
from dbUtils import *
from collections import defaultdict
import time
import pandas as pd

create_db = False
exp = "size"

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

repeat_num = 10
exp = "size"
db = "world"
support_suffix = "_ar_support"

base_size = 1000 
size_list = [0, 0.2, 0.4, 0.6, 0.8, 1]
table_list, table_size_list, table_fields = get_fields_of_all_tables(database=db)

tuple_price_list = defaultdict(int)
history = None
history_aware = False
for table in table_list:
    tuple_price_list[table] = 1


time_results = defaultdict(list)
price_results = defaultdict(list)
query_time_results = defaultdict(list)
query_price_results = defaultdict(list)
for size in size_list:
    
    r_size = size * base_size
    conn = connect(database=db)
    cursor = conn.cursor()
    r_support = ""
    # create the view as the support set
    support_size_list = defaultdict(int)
    if(size != 0):
        r_support = support_suffix + str(int(size*10))
        num_list = defaultdict(int)
        all_sum = sum([table_size_list[table] for table in table_list])
        for table in table_list:
            num_list[table] = int(table_size_list[table] / all_sum * r_size)
            support_size_list[table] = table_size_list[table] + num_list[table]
        print(size, num_list)
        if(create_db):
            for table in table_list:
                support_name = table + r_support
                all_support = table + "_ar_support_all"
                sql = f"drop table if exists {support_name}"
                cursor.execute(sql)      
                sql = f"create table {support_name} as (select * from {all_support} limit {num_list[table]})"
                print(sql)
                cursor.execute(sql)
                sql = f"show index from {table}"
                cursor.execute(sql)
                index_res = cursor.fetchall()
                column_list = []
                for row in index_res:
                    column_name = row[4]
                    index_name = column_name + "_index"
                    if(column_name not in column_list):
                        sql = f"create index {index_name} on {support_name} ({column_name})"
                        print(sql)
                        cursor.execute(sql)
                        column_list.append(column_name)
            
    else:
        support_size_list = table_size_list
    cursor.close()
    conn.close()
    print("---------------------", size, r_size, support_size_list)
    # start to price queries with different support sets 
    # print(support_size_list)
    print("-----------start pricing----------")
    pricer= Pricer(db, table_size_list, support_size_list, history, tuple_price_list, table_fields, domain_list, r_support, history_aware)
    # print(pricer.field_domin)
    for sql in sql_list:
        price_list = []
        time_list = []
        for i in range(repeat_num):
            start_time = time.time()
            price = pricer.price_SQL_query(sql)
            end_time = time.time()
            price_list.append(price)
            time_list.append(end_time - start_time)
        print(size, sql, price)
        query_time_results[size].append(sum(time_list)/len(time_list))
        query_price_results[size].append(sum(price_list)/len(price_list))
    time_results['AIRA'].append(sum(query_time_results[size])/len(query_time_results[size]))
    price_results['AIRA'].append(sum(query_price_results[size])/len(query_price_results[size]))

print(time_results)
print(price_results)

file_name = "./rs/" + exp + ".csv"
all_results = {}
all_results["Time"] = time_results['AIRA']
all_results["Price"] = price_results['AIRA']


df = pd.DataFrame(all_results, index = size_list)
df.to_csv(file_name)
        
file_name = "./rs/" + exp + ".xlsx"
writer = pd.ExcelWriter(file_name)            

df = pd.DataFrame(time_results, index = size_list)
df.to_excel(writer, sheet_name= "Time", index= True)

df = pd.DataFrame(price_results, index = size_list)
df.to_excel(writer, sheet_name= "Price", index= True)

sql_idx_list = [i+1 for i in range(len(sql_list))]
df = pd.DataFrame(query_time_results, index = sql_idx_list)
df.to_excel(writer, sheet_name= "Each Time", index= True)

df = pd.DataFrame(query_price_results, index = sql_idx_list)
df.to_excel(writer, sheet_name= "Each Price", index= True)

writer.save()
writer.close()

