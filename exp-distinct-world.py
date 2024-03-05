from Pricer import *
import numpy as np
import pandas as pd
db = "world"
exp = "distinct-" + db
repeat_num = 10
table_list, table_size_list, table_fields = get_fields_of_all_tables(database=db)
table_price_list = table_size_list
tuple_price_list = {}
for table in table_list:
    tuple_price_list[table] = 1

history_aware = False
support_suffix = ""
support_size_list = table_size_list
domain_list = None
history = None



my_pricer = Pricer(db, table_size_list, support_size_list, history, tuple_price_list, table_fields, domain_list, support_suffix, history_aware)


# test the following attributes 
attibute_list = ['CountryCode', 'Name', 'District', 'Population']
tables_list = ["city", "city", "city", "city"]

methods = ["ARIA"]
pricers = {"ARIA": my_pricer}
time_results = {"ARIA": []}
price_results = {"ARIA": []}



distinct_ratio_list = []
for i in range(len(attibute_list)):
    o_sql = f"select {attibute_list[i]} from {tables_list[i]}"
    sql = f"select distinct {attibute_list[i]} from {tables_list[i]}"
    # print(o_sql, sql)
    # start pricing
    for m in methods:
        pricer = pricers[m]
        time_list = []
        price_list = []
        o_price = pricer.price_SQL_query(o_sql)
        for j in range((repeat_num)):
            start_time = time.time()
            price = pricer.price_SQL_query(sql)
            end_time = time.time()
            time_list.append(end_time - start_time)
            price_list.append(price)
        price_results[m].append(sum(price_list)/(repeat_num)/o_price)
        time_results[m].append(sum(time_list)/(repeat_num))
    # print("My time is ", aria_time_list[-1], aria_price_list[-1])

    # compute the distinct rate
    o_sql = f"select count({attibute_list[i]}) from {tables_list[i]}"
    sql = f"select count(distinct {attibute_list[i]}) from {tables_list[i]}"
    distinct_num = select(sql, database= db)[0][0]
    o_num = select(o_sql, database= db)[0][0]
    distinct_ratio_list.append(distinct_num/o_num)




all_results = {}
all_results["ARIA-Price"] = price_results["ARIA"]

all_results["ARIA-Time"] = time_results["ARIA"]

all_results["Distinct-rate"] = distinct_ratio_list
file_name = "./rs/" + exp + ".csv"
df = pd.DataFrame(all_results, index = attibute_list)
print(distinct_ratio_list)
print(df)
df.to_csv(file_name)

file_name = "./rs/" + exp + ".xlsx"
writer = pd.ExcelWriter(file_name)

df = pd.DataFrame(time_results, index = attibute_list)
df.to_excel(writer, sheet_name= "Time", index= True)

df = pd.DataFrame(price_results, index = attibute_list)
df.to_excel(writer, sheet_name= "Price ratio", index= True)

writer.save()
writer.close()










