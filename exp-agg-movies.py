from Pricer import *
import numpy as np
import pandas as pd
db = "movies"
exp = "agg-" + db
table_list, table_size_list, table_fields = get_fields_of_all_tables(database=db)
table_price_list = table_size_list
tuple_price_list = {}
for table in table_list:
    tuple_price_list[table] = 1


repeat_num = 10
history_aware = False
support_suffix = ""
support_size_list = table_size_list
domain_list = {"ratings.rating": [0, 5],
               "users.age": [0, 56],
            }
history = None
# for table in table_list:
#     price_his = {}
#     for i in range(table_size_list[table]):
#         price_his[i + 1] = set([i+1 for i in range(support_size_list[table])])
#     history[table] = price_his
my_pricer = Pricer(db, table_size_list, support_size_list, history, tuple_price_list, table_fields, domain_list, support_suffix, history_aware)


# test the effect of the selectivity 
agg_list = ['count', 'avg', 'max', 'min']
methods = ["ARIA"]
pricers = {"ARIA": my_pricer}
time_results = {"ARIA": []}
price_results = {"ARIA": []}
table = "users"


for i in range(len(agg_list)):
    o_sql = f"select age from {table}"
    sql = f"select {agg_list[i]}(age) from {table}"
    print(sql)
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




file_name = "./rs/" + exp + ".csv"
np.savetxt("./rs/" + exp + "-price-ratio.txt", np.array([price_results[m] for m in methods]))
np.savetxt("./rs/" + exp + "-time.txt", np.array([time_results[m] for m in methods]))
all_results = {}
all_results["ARIA-Price"] = price_results["ARIA"]

all_results["ARIA-Time"] = time_results["ARIA"]


df = pd.DataFrame(all_results, index = agg_list)
df.to_csv(file_name)
print(df)
file_name = "./rs/" + exp + ".xlsx"
writer = pd.ExcelWriter(file_name)

df = pd.DataFrame(time_results, index = agg_list)
df.to_excel(writer, sheet_name= "Time", index= True)

df = pd.DataFrame(price_results, index = agg_list)
df.to_excel(writer, sheet_name= "Price ratio", index= True)

writer.save()
writer.close()









