from Pricer import Pricer
from dbUtils import *
from collections import defaultdict
import time
import pandas as pd
import numpy as np
# study the effect of the selectivities on query pricing
db = "world"
exp = "selecvitiy-" + db 


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
support_suffix = ""

selectivity_list = [0, 0.2, 0.4, 0.6, 0.8, 1]
table_list, table_size_list, table_fields = get_fields_of_all_tables(database=db)
support_size_list = table_size_list
tuple_price_list = defaultdict(int)
history = None
history_aware = False
for table in table_list:
    tuple_price_list[table] = 1
my_pricer= Pricer(db, table_size_list, support_size_list, history, tuple_price_list, table_fields, domain_list, support_suffix, history_aware)


time_results = []
price_results = []

methods = ["ARIA"]
pricers = {"ARIA": my_pricer}
time_results = {"ARIA": []}
price_results = {"ARIA": []}
table = "city"


for i in range(len(selectivity_list)):
    num = int(selectivity_list[i] * table_size_list[table])
    sql = f"select * from {table} where ID <= {num}"

    print(sql)
    # start pricing
    for m in methods:
        pricer = pricers[m]
        time_list = []
        price_list = []
        for j in range((repeat_num)):
            start_time = time.time()
            price = pricer.price_SQL_query(sql)
            end_time = time.time()
            time_list.append(end_time - start_time)
            price_list.append(price)
        price_results[m].append(sum(price_list)/(repeat_num))
        time_results[m].append(sum(time_list)/(repeat_num))
    # print("My time is ", aria_time_list[-1], aria_price_list[-1])



file_name = "./rs/" + exp + ".csv"


all_results = {}
all_results["ARIA-Price"] = price_results["ARIA"]
all_results["ARIA-Time"] = time_results["ARIA"]


df = pd.DataFrame(all_results, index = selectivity_list)
print(df)
df.to_csv(file_name)
file_name = "./rs/" + exp + ".xlsx"
writer = pd.ExcelWriter(file_name)

df = pd.DataFrame(time_results, index = selectivity_list)
df.to_excel(writer, sheet_name= "Time", index= True)

df = pd.DataFrame(price_results, index = selectivity_list)
df.to_excel(writer, sheet_name= "Price", index= True)

writer.save()
writer.close()
