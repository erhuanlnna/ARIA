
from MyPricer import Pricer
from dbUtils import *
#from MyPricer import Pricer
#from Utils import *
from collections import defaultdict
import time
import pandas as pd

# study the effect of the scale factor on the pricing time
o_db = "ssb"
exp = "sf-" + o_db


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


# sql list on TPC-H dataset
#query1 = 'select o_orderkey, o_orderpriority, l_commitdate, l_receiptdate from orders, lineitem where   o_orderdate >= date \'1993-07-01\'   and o_orderdate < date \'1993-07-01\' + interval \'3\' month   and l_orderkey = o_orderkey   and l_commitdate < l_receiptdate; ' 
#query2 = 'select  l_orderkey, o_orderkey,c_custkey,o_custkey, l_suppkey, s_suppkey,c_nationkey, n_nationkey,s_nationkey, n_regionkey, r_regionkey from   customer,   orders,   lineitem,   supplier,   nation,   region where   c_custkey = o_custkey   and l_orderkey = o_orderkey   and l_suppkey = s_suppkey   and c_nationkey = s_nationkey   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey   and r_name = \'ASIA\'   and o_orderdate >= date \'1994-01-01\'   and o_orderdate < date \'1994-01-01\' + interval \'1\' year' 
#query3 = 'select  l_shipdate  from   lineitem where   l_shipdate >= date \'1994-01-01\'   and l_shipdate < date \'1994-01-01\' + interval \'1\' year   and l_discount between 0.05 and 0.07   and l_quantity < 24; ' 
#query4 = 'select o_orderkey,l_orderkey, l_shipmode,o_orderpriority   from   orders,   lineitem where   o_orderkey = l_orderkey   and (l_shipmode = \'MAIL\' or l_shipmode = \'SHIP\')   and l_commitdate < l_receiptdate   and l_shipdate < l_commitdate   and l_receiptdate >= date \'1994-01-01\'   and l_receiptdate < date \'1994-01-01\' + interval \'1\' year' 
#query5 = 'select   ps_partkey, p_partkey, p_type from   partsupp,   part where   p_partkey = ps_partkey   and p_brand <> \'Brand#45\'   and p_type not like \'MEDIUM POLISHED%\'   and p_size in (49, 14, 23, 45, 19, 3, 36, 9)' 
#query6 = 'select  l_partkey, p_partkey from   lineitem,   part where   p_partkey = l_partkey   and p_brand = \'Brand#23\'   and p_container = \'MED BOX\'   and l_quantity < 5.10736 ' 
#query7 = 'select *  from   lineitem where   l_shipdate <= date \'1998-09-01\'' 
#query8 = 'select p_partkey,ps_partkey,s_suppkey,ps_suppkey,s_nationkey,n_nationkey,r_regionkey,s_acctbal,s_name,n_name   from   part,   supplier,   partsupp,   nation,   region where   p_partkey = ps_partkey   and s_suppkey = ps_suppkey   and p_size = 15   and p_type like \'%BRASS\'   and s_nationkey = n_nationkey   and n_regionkey = r_regionkey   and r_name = \'EUROPE\'   and ps_supplycost = 100 limit 100; ' 
#
#sql_list = [query1, query2, query3, query4, query5, query6, query7, query8] 


scale_factor_list = [1, 2, 3, 4, 5] 

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
