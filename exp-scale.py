
from MyPricer import Pricer
from dbUtils import *
#from MyPricer import Pricer
#from Utils import *
from collections import defaultdict
import time
import pandas as pd

# study the effect of the scale factor on the pricing time
exp = "sf"



sql_list = [
    "select lo_orderdate, d_datekey, lo_orderkey from lineorder, dates where lo_orderdate = d_datekey and d_year = '1995' and lo_discount between 1 and 10 and lo_quantity < 50;",
    "select lo_orderdate, d_datekey, lo_orderkey from lineorder, dates where lo_orderdate = d_datekey and d_yearmonthnum = '199401' and lo_discount between 4 and 6 and lo_quantity between 26 and 35;",
    "select lo_orderdate, d_datekey, lo_orderkey from lineorder, dates where lo_orderdate = d_datekey and d_weeknuminyear = 6 and d_year = 1994 and lo_discount between 5 and 7 and lo_quantity between 26 and 35;",
    "select lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey, d_year, p_brand from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand between 'MFGR#2220' and 'MFGR#2229' and s_region = 'ASIA';",
    "select lo_orderdate, d_datekey, lo_partkey, p_partkey, lo_suppkey, s_suppkey, d_year, p_brand from lineorder, dates, part, supplier where lo_orderdate = d_datekey and lo_partkey = p_partkey and lo_suppkey = s_suppkey and p_brand = 'MFGR#2221' and s_region = 'EUROPE';",
    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, d_year, c_city, s_city from lineorder, dates, customer, supplier where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and c_nation = 'UNITED STATES' and s_nation = 'UNITED STATES' and d_year between 1992 and 1997;",
    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, d_year, c_city, s_city from lineorder, dates, customer, supplier where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and (c_city = 'UNITED KI5' or c_city = 'UNITED KI1') and (s_city = 'UNITED KI5' or s_city = 'UNITED KI1') and d_yearmonth = 'Dec1997';",
    "select lo_orderdate, d_datekey, lo_custkey, c_custkey, lo_suppkey, s_suppkey, lo_partkey, p_partkey, c_nation, d_year from lineorder, dates, customer, supplier, part where lo_orderdate = d_datekey and lo_custkey = c_custkey and lo_suppkey = s_suppkey and lo_partkey = p_partkey and c_region = 'AMERICA' and c_region = 'AMERICA' and s_nation = 'UNITED STATES' and p_category = 'MFGR#14' and (d_year = 1997 or d_year = 1998);"
] 


scale_factor_list = [1, 2, 3, 4, 5] 
o_db = "ssb"
repeat_num = 5
result_list = []
sql_idx_list = [i+1 for i in range(len(sql_list))]



avg_time_list = defaultdict(list)
avg_price_list = defaultdict(list)
time_results = defaultdict(list)
price_results = defaultdict(list)

# print("------------Query------------")
# for sf in scale_factor_list:
#     db = o_db + str(sf) +"g"
#     idx = f"query-on-{db}"
#     for sql in sql_list:
#         time_list = []
#         for i in range(repeat_num):
#             start_time = time.time()
#             conn = connect(database=db)
#             cursor = conn.cursor(buffered=True)
#             cursor.execute(sql)
#             cursor.close()
#             conn.close()
#             # rs = select(sql, database=db)
#             end_time = time.time()
#             time_list.append(end_time - start_time)
#         time_results[idx].append(sum(time_list)/len(time_list))
#     avg_time_list["query"].append(sum(time_results[idx])/len(time_results[idx]))
# print(avg_time_list["query"])

# print("------------ARIA------------")
# base_size = 1000 * 1000
# size = 0
# r_size = size * base_size
for sf in scale_factor_list:
    db = o_db + str(sf) +"g"
    table_list, table_size_list, table_fields = get_fields_of_all_tables(database=db)
    tuple_price_list = defaultdict(int)
    history = None
    domain_list = None
    history_aware = False
    for table in table_list:
        tuple_price_list[table] = 1
    
    # initiliaze the ARIA pricer
    support_suffix = ""
    support_size_list = table_size_list
    my_pricer = Pricer(db, table_size_list, support_size_list, history, tuple_price_list, table_fields, domain_list, support_suffix, history_aware)

    support_size_list = table_size_list
    table_price_list = table_size_list
    for sql in sql_list:
        idx = f"query-on-{db}"
        time_list = []
        for i in range(repeat_num):
            start_time = time.time()
            conn = connect(database=db)
            cursor = conn.cursor(buffered=True)
            cursor.execute(sql)
            cursor.close()
            conn.close()
            # rs = select(sql, database=db)
            end_time = time.time()
            time_list.append(end_time - start_time)
        time_results[idx].append(sum(time_list)/len(time_list))

        time_list = []
        price_list = []
        idx = f"ARIA-on-{db}"
        for i in range(repeat_num):
            start_time = time.time()
            price = my_pricer.price_SQL_query(sql)
            end_time = time.time()
            time_list.append(end_time - start_time)
            price_list.append(price)
        time_results[idx].append(sum(time_list)/len(time_list))
        price_results[idx].append(sum(price_list)/len(price_list))
        # print(idx, sql, sum(price_list)/len(price_list), price_results[idx][-1])
        

    idx = f"query-on-{db}"
    avg_time_list["query"].append(sum(time_results[idx])/len(time_results[idx]))
    idx = f"ARIA-on-{db}"
    avg_time_list["ARIA"].append(sum(time_results[idx])/len(time_results[idx]))
    avg_price_list["ARIA"].append(sum(price_results[idx])/len(price_results[idx]))
    
print(avg_time_list["ARIA"])
print(avg_price_list["ARIA"])
    


file_name = "./rs/" + exp + ".csv"


all_results = {}
all_results["ARIA-Price"] = avg_price_list["ARIA"]
all_results["ARIA-Time"] = avg_time_list["ARIA"]


df = pd.DataFrame(all_results, index = scale_factor_list)
print(df)
df.to_csv(file_name)

file_name = "./rs/" + exp + ".xlsx"
writer = pd.ExcelWriter(file_name)

df = pd.DataFrame(avg_time_list, index = scale_factor_list)
df.to_excel(writer, sheet_name= "Time", index= True)

df = pd.DataFrame(avg_price_list, index = scale_factor_list)
df.to_excel(writer, sheet_name= "Price", index= True)

print(time_results)
print(price_results)

df = pd.DataFrame(time_results)
df.to_excel(writer, sheet_name= "Time-query", index= True)

df = pd.DataFrame(price_results)
df.to_excel(writer, sheet_name= "Price-query", index= True)
writer.save()
writer.close()
