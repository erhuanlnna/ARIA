from Pricer import Pricer

from dbUtils import *
from collections import defaultdict
import time
import pandas as pd
import numpy as np


db = "world"
exp = "limit-" + db 
support_suffix = ""
repeat_num = 5

limit_num_list = [0, 100, 222, 223, 300, 500]
table_list, table_size_list, table_fields = get_fields_of_all_tables(database=db)
support_size_list = table_size_list
tuple_price_list = defaultdict(int)
history = None
history_aware = False
for table in table_list:
    tuple_price_list[table] = 1
domain_list = None
my_pricer= Pricer(db, table_size_list, support_size_list, history, tuple_price_list, table_fields, domain_list, support_suffix, history_aware)



time_results = []
price_results = []

methods = ["ARIA"]
pricers = {"ARIA": my_pricer}
time_results = {"ARIA": []}
price_results = {"ARIA": []}
table = "country"


for i in range(len(limit_num_list)):
    limit_num = limit_num_list[i]
    sql = f"select * from {table} where Code <= 'UMI'  limit {limit_num}"
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


df = pd.DataFrame(all_results, index = limit_num_list)
print(df)
df.to_csv(file_name)
file_name = "./rs/" + exp + ".xlsx"
writer = pd.ExcelWriter(file_name)

df = pd.DataFrame(time_results, index = limit_num_list)
df.to_excel(writer, sheet_name= "Time", index= True)

df = pd.DataFrame(price_results, index = limit_num_list)
df.to_excel(writer, sheet_name= "Price", index= True)

writer.save()
writer.close()
