import time
from dbUtils import *
import random
from decimal import Decimal


def write_supports_to_file(file_path, support_sets):
    with open(file_path, 'w') as file:
        for support in support_sets:
            str_support = [str(item) for item in support]
            tmp_str = ";".join(str_support)
            tmp_str = tmp_str + ";"
            file.write(tmp_str + '\n')

def create_table(support_name, table):
    sql = f"drop table if EXISTS  {support_name} "
    insert(sql, database=db)
    sql = f"create table {support_name} like {str.upper(table)}"
    insert(sql, database=db)
    sql = f"alter table {support_name} add column aID INT NOT NULL;"
    insert(sql, database=db)
    sql = f"delete from {support_name}"
    insert(sql, database=db)
def write_into_db(db, support_name, file_name):
    # sql = "set global local_infile=on;"
    # insert(sql, database=db)
    sql = f"use {db}"
    insert(sql, database=db)
    sql = f"LOAD DATA LOCAL INFILE '/xxxxx/{file_name}' INTO TABLE {support_name}   FIELDS TERMINATED BY ';';"
    # insert(sql, database=db)
    print(sql)

def generate_support_sets(db, num_list, table_list, table_size_list, table_fields):
    support_suffix = "_ar_support_all"
    # sql = f"use {db}"
    # insert(sql, database=db)
    conn = pymysql.connect(host = host, user=user, passwd=password, database=db)
    cursor = conn.cursor()
    sql = f"use {db}"
    cursor.execute(sql)
    for table in table_list:
        support_sets = []
        support_name = table + support_suffix
        # create_table(support_name, table)
        table_size = table_size_list[table]
        column_num = len(table_fields[table])
        exsiting_data_num = 0
        # sql = f"select count(*) from {support_name}"
        # exsiting_data_num = select(sql, database=db)[0][0]

        new_support_num = num_list[table] - exsiting_data_num
        # print(table, new_support_num)
        

        for i in range(new_support_num):
            new_aid = i + 1 + table_size + exsiting_data_num
            # randomly choose a tuple
            r_aid = random.randint(0, table_size - 1)
            sql = f"select * from {table} where aID = {r_aid + 1}"
            # data = select(sql, database=db)[0]
            cursor.execute(sql)
            data = cursor.fetchall()[0]
            while(True):
                j2 = random.randint(0, table_size - 1)
                sql = f"select * from {table} where aID = {j2 + 1}"
                # n_data = select(sql, database=db)[0]
                cursor.execute(sql)
                n_data = cursor.fetchall()[0]
                k = random.randint(0, column_num - 1)
                v = data[k]
                # print(table_size, j, k)
                if(isinstance(v, Decimal)):
                    v = float(v)
                new_v = n_data[k]
                if(isinstance(new_v, Decimal)):
                    new_v = float(new_v)
                if(new_v != v):
                    # print(data)
                    new_data = list(data[:-1])
                    new_data[k] = new_v
                    # do not check the unique of new data
                    new_data = new_data + [new_aid]
                    break
                    # str_list = []
                    # for ii, field in enumerate(table_fields[table]):
                    #     str_list.append(f"{field} = '{new_data[ii]}'")
                    # tm_str = " and ".join(str_list)
                    # sql = f"select count(*) from {support_name} where "
                    # sql = sql + tm_str
                    # # print(sql)
                    # rs = select(sql, database=db)
                    # if(rs[0][0] == 0):
                    #     new_data = new_data + [new_aid]
                    #     break

            support_sets.append(new_data) 
        file_name = db +"_" + support_name + ".tbl"
        write_supports_to_file(file_path=file_name, support_sets=support_sets)    
        print(f"Have generated on table {table} in {file_name}")
    cursor.close()
    conn.close()

# db = "world"     
# s_size = 1000   
import argparse
parser = argparse.ArgumentParser()

# Add arguments
parser.add_argument('-d', '--db', help='The database')
parser.add_argument('-s', '--size', help='The size of extra generated tuples in support sets')
# Parse the arguments
args = parser.parse_args()

# Access the parsed arguments
db = args.db
s_size = args.size 
table_list, table_size_list, table_fields = get_fields_of_all_tables(database=db)

num_list = {}
data_size = 0
for table in table_list:
    data_size += table_size_list[table]
ratio = s_size/data_size
for table in table_list:
    num_list[table] = int(table_size_list[table] * ratio)
print(num_list)
generate_support_sets(db, num_list, table_list, table_size_list, table_fields)
for table in table_list:
    support_suffix = "_ar_support_all"
    support_name = table + support_suffix
    file_name = db +"_" + support_name + ".tbl"
    print("Please using the following code (with your own file path) in MySQL to import the generated tuples")
    write_into_db(db, support_name, file_name)


        