from parse_sql import QueryMetaData
from collections import defaultdict
import numpy as np
from dbUtils import *
#from Utils import *
import time
import bisect

def count_and_idx(list1, l, r):
	# sort the list
	list1.sort()

	# find the index of leftmost occurrence of l in list1
	left_idx = bisect.bisect_left(list1, l)

	# find the index of rightmost occurrence of r in list1
	right_idx = bisect.bisect_right(list1, r)

	# calculate the count of numbers in the range
	cnt = right_idx - left_idx

	return cnt, left_idx, right_idx
class Pricer:
    def __init__(self, db, table_size_list, support_size_list, price_history, tuple_price_list, table_fields, field_domin, support_suffix, history_aware):
        self.db = db
        self.table_fields = table_fields
        self.table_size_list = table_size_list
        self.price_history = price_history
        self.support_size_list = support_size_list
        self.tuple_price_list = tuple_price_list
        self.field_domin = field_domin
        self.support_suffix = support_suffix
        self.history_aware = history_aware 



    def __price_cnt_query_history_aware__(self, sql):
        # the count(*) equals to the select 1 query
        # the group by count query equals to the original query
        # the count over joined tables eqauls to the one "select joined keys from ...."
        # since the table always has the primary key
        md = QueryMetaData(sql)
        # print(md)
        query_tables = md.tables
        price = 0
        if(len(query_tables) == 1):
            if("group by" in sql):
                # remove the count(*) from the original sql
                sql = sql.split("group")[0]
                tmp_str = sql.split("from")
                # print("--- ", tmp_str)
                tmp_str1 = tmp_str[0]
                tmp_str2 = tmp_str[1]
                id_select = tmp_str1.split(",")[:-1]
                tmp_str1 = ",".join(id_select)
                new_sql = tmp_str1 + " from " + tmp_str2
                # pricing as normal
                # print("--- ", new_sql)
                price = self.__price_SPJ_query_history_aware__(new_sql)
                return price
            else:
                sql = sql.split("group")[0]
                tmp_str2 = sql.split("from")[1]
                # print("--- ", tmp_str)
                new_sql = "select aID " + " from " + tmp_str2
                # print("---", new_sql)
                # pricing as normal
                table = query_tables[0]
                tuple_price = self.tuple_price_list[table]
                table_price_history = self.price_history[table]
                support_size = self.support_size_list[table]
                o_results = select(new_sql, database= self.db)
                if(self.support_suffix == ""):
                    s_results = []
                    
                else:
                    new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
                    new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                    new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                    s_results = select(new_sql, database= self.db)
                    results_id = set([int(item[0]) for item in s_results])
                    updated_id_list = [int(item[0]) for item in o_results]
                # group by the results based on the selected attributes
                updated_id_list = [int(item[0]) for item in o_results]
                results_id = set([int(item[0]) for item in s_results] + updated_id_list)
                for item in o_results:
                    aID = int(item[0])
                    his = table_price_history[aID]
                    tmp_his = his & results_id
                    tmp_price = len(his) - len(tmp_his)
                    # tmp_list.append(tmp_price)
                    table_price_history[aID] = tmp_his
                    price += tmp_price/(support_size - 1) * tuple_price

                for aID in table_price_history.keys():
                    if(aID not in updated_id_list):
                        his = table_price_history[aID]
                        tmp_his = his - results_id
                        tmp_price = len(his) - len(tmp_his)
                        table_price_history[aID] = tmp_his
                        price += tmp_price/(support_size - 1) * tuple_price
                return price
        else:
            # the joined keys are outputted, i.e., the query is a group-by count query
            # remove the count(*) from the original sql
            sql = sql.split("group")[0]
            tmp_str = sql.split("from")[0]
            tmp_str1 = tmp_str[0]
            tmp_str2 = tmp_str[1]
            id_select = tmp_str1.split(",")[:-1]
            tmp_str1 = ",".join(id_select)
            new_sql = tmp_str1 + " from " + tmp_str2
            # pricing as normal
            price = self.__price_SPJ_query_history_aware__(new_sql)
            return price

    def __price_cnt_query_no_history__(self, sql):
        # the count(*) equals to the select 1 query
        # the group by count query equals to the original query
        # the count over joined tables eqauls to the one "select joined keys from ...."
        # since the table always has the primary key
        md = QueryMetaData(sql)
        price = 0
        # print(md)
        query_tables = md.tables
        if(len(query_tables) == 1):
            if("group by" in sql):
                # remove the count(*) from the original sql
                sql = sql.split("group")[0]
                tmp_str = sql.split("from")
                tmp_str1 = tmp_str[0]
                tmp_str2 = tmp_str[1]
                id_select = tmp_str1.split(",")[:-1]
                tmp_str1 = ",".join(id_select)
                new_sql = tmp_str1 + " from " + tmp_str2
                # pricing as normal
                price = self.__price_SPJ_query_no_history__(new_sql)
                return price
            else:
                # pricing as normal
                table = query_tables[0]
                tuple_price = self.tuple_price_list[table]
                table_size = self.table_size_list[table]
                support_size = self.support_size_list[table]
                o_results = select(sql, database= self.db)
                s_results = [[0]]
                if(self.support_suffix != ""):
                    # new_sql = sql.replace(table, table + self.support_suffix)
                    new_sql = sql.replace(table + " ", table + self.support_suffix + " ")
                    new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                    new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                    s_results = select(new_sql, database= self.db)
                
                n = o_results[0][0]
                E_n = s_results[0][0] + n
                E_a = E_n
                tmp_price = support_size * n - n * E_n 
                price += tmp_price/(support_size - 1) * tuple_price
                tmp_price = (table_size - n) * E_a 
                price += tmp_price/(support_size - 1) * tuple_price
                return price
        else:
            # the joined keys are outputted, i.e., the query is a group-by count query
            # remove the count(*) from the original sql
            sql = sql.split("group")[0]
            tmp_str = sql.split("from")[0]
            tmp_str1 = tmp_str[0]
            tmp_str2 = tmp_str[1]
            id_select = tmp_str1.split(",")[:-1]
            tmp_str1 = ",".join(id_select)
            new_sql = tmp_str1 + " from " + tmp_str2
            # pricing as normal
            price = self.__price_SPJ_query_no_history__(new_sql)
            return price        

    def __price_distinct_star_history_aware__(self, sql):
        # the distinct * query equals to * query
        # since the table always has the primary key
        sql = sql.replace("distinct", "")
        return self.__price_SPJ_star_history_aware__(sql)
    def __price_distinct_star_no_history__(self, sql):
        # the distinct * query equals to * query
        # since the table always has the primary key
        sql = sql.replace("distinct", "")
        return self.__price_SPJ_star_no_history__(sql)
    def __price_distinct_query_history_aware__(self, sql):
        sql = sql.replace("distinct", "")
        md = QueryMetaData(sql)
        # print(md)
        query_tables = md.tables
        price_list = []
        if(len(query_tables) == 1):
            price = 0
            # directly compute the query price
            table = query_tables[0]
            support_size = self.support_size_list[table]
            tuple_price = self.tuple_price_list[table]
            table_price_history = self.price_history[table]   
            updated_id_list = []
            sql = sql.replace("select", "select aID, ")
            o_results = select(sql, database= self.db)

            if(self.support_suffix == ""):
                s_results = []
            else:
                # new_sql = sql.replace(table, table + self.support_suffix)
                new_sql = sql.replace(table + " ", table + self.support_suffix + " ")
                new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                s_results = select(new_sql, database= self.db)
            
            # group by the results based on the selected attributes
            query_groups = defaultdict(list)
            results_id = []
            updated_id_list = []
            o_query_groups = defaultdict(list)
            for item in o_results:
                aID = int(item[0])
                o_query_groups[tuple(item[1:])].append(aID)
                query_groups[tuple(item[1:])].append(aID)
                results_id.append(aID)
                
            for item in s_results:
                aID = int(item[0])
                query_groups[tuple(item[1:])].append(aID)
                results_id.append(aID)
                
            # tmp_list = []
            
            
            results_id = set(results_id)
            for item in o_query_groups.keys():
                # each group produce one result
                # find the tuple with the maximum price
                distinct_id_list = o_query_groups[item]
                effective_id_list = set(query_groups[item])
                tmp_price = -1
                tmp_id = -1
                for id in distinct_id_list:
                    eff_his = set(table_price_history[id])
                    tmp_eff = eff_his & effective_id_list
                    if(tmp_price < len(eff_his) - len(tmp_eff)):
                        tmp_price = len(eff_his) - len(tmp_eff)
                        tmp_id = id
                # update the effective states
                eff_his = set(table_price_history[tmp_id])
                table_price_history[tmp_id] = eff_his & effective_id_list
                price += tmp_price/(support_size - 1) * tuple_price
                # print(tmp_price)
                updated_id_list.append(tmp_id)
                if(self.support_suffix != ""):
                    results_id = results_id - effective_id_list
            # print(tmp_list)
            if(self.support_suffix != ""):
                for aID in table_price_history.keys():
                    if(aID not in updated_id_list):
                        his = table_price_history[aID]
                        tmp_his = his - results_id
                        tmp_price = len(his) - len(tmp_his)
                        table_price_history[aID] = tmp_his
                        price += tmp_price/(support_size - 1) * tuple_price
            price_list.append(price)
        else:
            str1 = sql.split("from")[1]
            selected_attributes = {table: [table + ".aID"] for table in query_tables}
            # joined_keys() = {table: [] for table in query_tables}
            # for tmp_att in md.joins:
            #     att = tmp_att.sql()
            #     if "." in att:
            #         table = att.split(".")[0]
            #         # joined_keys()[table].append(att)
            #         selected_attributes[table].append(att)
            #     else:
            #         selected_attributes[attributes_list[att]].append(att)
            #         # joined_keys()[attributes_list[att]].append(att)
            for att in md.projections:
                if "." in att:
                    str_list = att.split(".")
                    table = str_list[0]
                    selected_attributes[table].append(att)
                else:
                    for tt in query_tables:
                        if(att in self.table_fields[tt]):
                            selected_attributes[tt].append(att)
                            break

            id_select = [",".join(selected_attributes[table]) for table in query_tables]
            selected_idx = [len(selected_attributes[table]) for table in query_tables]
            str2 = ",".join(id_select)
            new_sql = "select " + str2 + " from " + str1
            # print(new_sql)
            o_results = select(new_sql, database= self.db)
            o_results = np.array(o_results)
            for i, table in enumerate(query_tables):
                price = 0
                support_size = self.support_size_list[table]
                tuple_price = self.tuple_price_list[table]
                table_price_history = self.price_history[table]  
                o_1 = []
                o_2 = []
                updated_id_list = []
                idx = selected_idx[i]
                if(i == 0):
                    pre_idx = 0
                else:
                    pre_idx = pre_idx + selected_idx[i-1]
                if(len(o_results) != 0):
                    o_1 = o_results[:, pre_idx:pre_idx+idx]
                    # remove duplicate
                    o_1 = np.unique(o_1, axis = 0)
                # n = len(o_1)
                if(self.support_suffix != ""):
                    new_sql = "select " + id_select[i] + " from " + str1
                    # new_sql = new_sql.replace(table, table + self.support_suffix)
                    new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
                    new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                    new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                    s_results = select(new_sql, database= self.db)
                    if(len(s_results) != 0):
                        o_2 = np.array(s_results)
                        o_2 = np.unique(o_2, axis = 0)
                # group by the query results on the support set
                # group by the results based on the selected attributes
                o_query_groups = defaultdict(list)
                query_groups = defaultdict(list)
                results_id = []
                for item in o_1:
                    aID = int(item[0])
                    o_query_groups[tuple(item[1:])].append(aID)
                    query_groups[tuple(item[1:])].append(aID)
                    results_id.append(aID)
                
                for item in o_2:
                    aID = int(item[0])
                    query_groups[tuple(item[1:])].append(aID)
                    results_id.append(aID)
                results_id = set(results_id)
                
                
                
                # print("----------------")
                for item in o_query_groups.keys():
                    # each group produce one result
                    # find the tuple with the maximum price
                    effective_id_list = set(query_groups[item])
                    if(self.support_suffix  != ""):
                        distinct_id_list = o_query_groups[item]
                    else:
                        distinct_id_list = effective_id_list
                    tmp_price = -1
                    tmp_id = -1
                    for id in distinct_id_list:
                        eff_his = set(table_price_history[id])
                        tmp_eff = eff_his & effective_id_list
                        if(tmp_price < len(eff_his) - len(tmp_eff)):
                            tmp_price = len(eff_his) - len(tmp_eff)
                            tmp_id = id
                    # update the effective states
                    eff_his = set(table_price_history[tmp_id])
                    table_price_history[tmp_id] = eff_his & effective_id_list
                    price += tmp_price/(support_size - 1) * tuple_price
                    # print(tmp_price)
                    updated_id_list.append(tmp_id)
                    if(self.support_suffix != ""):
                        results_id = results_id - effective_id_list
                # print(tmp_list)
                if(self.support_suffix  != ""):
                    for aID in table_price_history.keys():
                        if(aID not in updated_id_list):
                            his = table_price_history[aID]
                            tmp_his = his - results_id
                            tmp_price = len(his) - len(tmp_his)
                            table_price_history[aID] = tmp_his
                            price += tmp_price/(support_size - 1) * tuple_price
                price_list.append(price)
        return sum(price_list)       


    def __price_distinct_query_no_history__(self, sql):
        sql = sql.replace("distinct", "")
        md = QueryMetaData(sql)
        # print(md)
        query_tables = md.tables
        price_list = []
        if(len(query_tables) == 1):
            price = 0
            # directly compute the query price
            table = query_tables[0]
            support_size = self.support_size_list[table]
            table_size = self.table_size_list[table]
            tuple_price = self.tuple_price_list[table]  
            o_results = select(sql, database= self.db)

            if(self.support_suffix == ""):
                s_results = []
            else:
                # new_sql = sql.replace(table, table + self.support_suffix)
                new_sql = sql.replace(table + " ", table + self.support_suffix + " ")
                new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                s_results = select(new_sql, database= self.db)
            
            # group by the results based on the selected attributes
            query_groups = defaultdict(int)
            o_query_groups = defaultdict(int)
            for item in o_results:
                query_groups[tuple(item)] += 1
                o_query_groups[tuple(item)] += 1
            for item in s_results:
                query_groups[tuple(item)] += 1
            
            # tmp_list = []
            effective_states = [query_groups[tuple(item)] for item in o_query_groups.keys()]
            E_n = sum(effective_states)
            E_a = len(s_results) + len(o_results) - E_n
            n = len(o_query_groups.keys())
            price = support_size * n  - E_n
            price += (table_size - n) * (E_a)
            price = price / (support_size - 1) * tuple_price
            price_list.append(price)
        else:
            str1 = sql.split("from")[1]
            selected_attributes = {table: [table + ".aID"] for table in query_tables}
            # joined_keys() = {table: [] for table in query_tables}
            # for tmp_att in md.joins:
            #     att = tmp_att.sql()
            #     if "." in att:
            #         table = att.split(".")[0]
            #         # joined_keys()[table].append(att)
            #         selected_attributes[table].append(att)
            #     else:
            #         selected_attributes[attributes_list[att]].append(att)
            #         # joined_keys()[attributes_list[att]].append(att)
            for att in md.projections:
                if "." in att:
                    str_list = att.split(".")
                    table = str_list[0]
                    selected_attributes[table].append(att)
                else:
                    for tt in query_tables:
                        if(att in self.table_fields[tt]):
                            selected_attributes[tt].append(att)
                            break

            id_select = [",".join(selected_attributes[table]) for table in query_tables]
            selected_idx = [len(selected_attributes[table]) for table in query_tables]
            str2 = ",".join(id_select)
            new_sql = "select " + str2 + " from " + str1
            # print(new_sql)
            o_results = select(new_sql, database= self.db)
            o_results = np.array(o_results)
            for i, table in enumerate(query_tables):
                price = 0
                support_size = self.support_size_list[table]
                tuple_price = self.tuple_price_list[table]
                table_size = self.table_size_list[table]  
                o_1 = []
                o_2 = []
                idx = selected_idx[i]
                if(i == 0):
                    pre_idx = 0
                else:
                    pre_idx = pre_idx + selected_idx[i-1]
                if(len(o_results) != 0):
                    o_1 = o_results[:, pre_idx:pre_idx+idx]
                    # remove duplicate
                    o_1 = np.unique(o_1, axis = 0)
                    o_1 = o_1[:, 1:] # remove the aID column
                
                if(self.support_suffix != ""):
                    new_sql = "select " + id_select[i] + " from " + str1
                    # new_sql = new_sql.replace(table, table + self.support_suffix)
                    new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
                    new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                    new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                    s_results = select(new_sql, database= self.db)
                    if(len(s_results) != 0):
                        o_2 = np.array(s_results)
                        o_2 = np.unique(o_2, axis = 0)
                        o_2 = o_2[:, 1:] # remove the aID column
                    
                # group by the query results on the support set
                # group by the results based on the selected attributes
                query_groups = defaultdict(int)
                o_query_groups = defaultdict(int)

                for item in o_1:
                    o_query_groups[tuple(item)] += 1
                    query_groups[tuple(item)] += 1
                for item in o_2:
                    query_groups[tuple(item)] += 1

                    
                # tmp_list = []
                effective_states = [query_groups[tuple(item)] for item in o_query_groups.keys()]
                E_n = sum(effective_states)
                E_a = len(o_2) + len(o_1) - E_n
                n = len(o_query_groups.keys())
                price = support_size * n  - E_n
                price += (table_size - n) * (E_a)
                price = price / (support_size - 1) * tuple_price       
                price_list.append(price)
        return sum(price_list)       


    def __price_SPJ_query_history_aware__(self, sql):
        md = QueryMetaData(sql)
        # print(md)
        query_tables = md.tables
        price_list = []
        if(len(query_tables) == 1):
            price = 0
            # directly compute the query price
            table = query_tables[0]
            support_size = self.support_size_list[table]
            tuple_price = self.tuple_price_list[table]
            table_price_history = self.price_history[table]   
            updated_id_list = []
            sql = sql.replace("select", "select aID, ")
            o_results = select(sql, database= self.db)

            if(self.support_suffix == ""):
                s_results = []
            else:
                # new_sql = sql.replace(table, table + self.support_suffix)
                new_sql = sql.replace(table + " ", table + self.support_suffix + " ")
                new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                s_results = select(new_sql, database= self.db)
            
            # group by the results based on the selected attributes
            query_groups = defaultdict(list)
            results_id = []
            updated_id_list = []
            for item in o_results:
                aID = int(item[0])
                query_groups[tuple(item[1:])].append(aID)
                results_id.append(aID)
            for item in s_results:
                aID = int(item[0])
                query_groups[tuple(item[1:])].append(aID)
                results_id.append(aID)
            # tmp_list = []
            for item in o_results:
                aID = int(item[0])
                his = table_price_history[aID]
                tmp_his = his & set(query_groups[tuple(item[1:])])
                tmp_price = len(his) - len(tmp_his)
                # tmp_list.append(tmp_price)
                table_price_history[aID] = tmp_his
                price += tmp_price/(support_size - 1) * tuple_price
                updated_id_list.append(aID)
            # print(tmp_list)
            results_id = set(results_id) 
            for aID in table_price_history.keys():
                if(aID not in updated_id_list):
                    his = table_price_history[aID]
                    tmp_his = his - results_id
                    tmp_price = len(his) - len(tmp_his)
                    table_price_history[aID] = tmp_his
                    price += tmp_price/(support_size - 1) * tuple_price
            price_list.append(price)
        else:
            str1 = sql.split("from")[1]
            selected_attributes = {table: [table + ".aID"] for table in query_tables}
            # joined_keys() = {table: [] for table in query_tables}
            # for tmp_att in md.joins:
            #     att = tmp_att.sql()
            #     if "." in att:
            #         table = att.split(".")[0]
            #         # joined_keys()[table].append(att)
            #         selected_attributes[table].append(att)
            #     else:
            #         selected_attributes[attributes_list[att]].append(att)
            #         # joined_keys()[attributes_list[att]].append(att)
            for att in md.projections:
                if "." in att:
                    str_list = att.split(".")
                    table = str_list[0]
                    selected_attributes[table].append(att)
                else:
                    for tt in query_tables:
                        if(att in self.table_fields[tt]):
                            selected_attributes[tt].append(att)
                            break

            id_select = [",".join(selected_attributes[table]) for table in query_tables]
            selected_idx = [len(selected_attributes[table]) for table in query_tables]
            str2 = ",".join(id_select)
            new_sql = "select " + str2 + " from " + str1
            # print(new_sql)
            o_results = select(new_sql, database= self.db)
            o_results = np.array(o_results)
            for i, table in enumerate(query_tables):
                price = 0
                support_size = self.support_size_list[table]
                tuple_price = self.tuple_price_list[table]
                table_price_history = self.price_history[table]  
                updated_id_list = []
                o_1 = []
                o_2 = []
                idx = selected_idx[i]
                if(i == 0):
                    pre_idx = 0
                else:
                    pre_idx = pre_idx + selected_idx[i-1]
                if(len(o_results) != 0):
                    o_1 = o_results[:, pre_idx:pre_idx+idx]
                    # remove duplicate
                    o_1 = np.unique(o_1, axis = 0)
                
                if(self.support_suffix != ""):
                    new_sql = "select " + id_select[i] + " from " + str1
                    new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
                    new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                    new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                    s_results = select(new_sql, database= self.db)
                    if(len(s_results) != 0):
                        o_2 = np.array(s_results)
                        o_2 = np.unique(o_2, axis = 0)
                    
                # group by the query results on the support set
                # group by the results based on the selected attributes
                query_groups = defaultdict(list)
                results_id = []
                for item in o_1:
                    aID = int(item[0])
                    query_groups[tuple(item[1:])].append(aID)
                    results_id.append(aID)
                for item in o_2:
                    aID = int(item[0])
                    query_groups[tuple(item[1:])].append(aID)
                    results_id.append(aID)
                # print("----------------")
                # tmp_list = []
                for item in o_1:
                    aID = int(item[0])
                    his = table_price_history[aID]
                    tmp_his = his & set(query_groups[tuple(item[1:])])
                    tmp_price = len(his) - len(tmp_his)
                    # tmp_list.append(tmp_price)
                    table_price_history[aID] = tmp_his
                    price += tmp_price/(support_size - 1) * tuple_price
                    updated_id_list.append(aID)
                # print(sum(tmp_list), len(tmp_list))
                results_id = set(results_id) 
                for aID in table_price_history.keys():
                    if(aID not in updated_id_list):
                        his = table_price_history[aID]
                        tmp_his = his - results_id
                        tmp_price = len(his) - len(tmp_his)
                        table_price_history[aID] = tmp_his
                        price += tmp_price/(support_size - 1) * tuple_price
                price_list.append(price)
        return sum(price_list)
    
    def __price_SPJ_star_history_aware__(self, sql):
        md = QueryMetaData(sql)
        # print(md)
        query_tables = md.tables
        price_list = []
        if(len(query_tables) == 1):
            price = 0
            # directly compute the query price
            table = query_tables[0]
            support_size = self.support_size_list[table]
            tuple_price = self.tuple_price_list[table]
            table_price_history = self.price_history[table]   
            updated_id_list = []
            # sql = sql.replace("select", "select aID, ")
            sql = sql.replace("*", "aID")
            o_results = select(sql, database= self.db)
            s_results = []
            if(self.support_suffix != ""):
                # new_sql = sql.replace(table, table + self.support_suffix)
                new_sql = sql.replace(table + " ", table + self.support_suffix + " ")
                new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                s_results = select(new_sql, database= self.db)
                
            updated_id_list = [int(item[0])for item in o_results]
            results_id = set([int(item[0]) for item in s_results] + updated_id_list)
            # each result is only with one effective state, i.e., itself
            # tmp_list = []
            for item in o_results:
                aID = int(item[0])
                his = table_price_history[aID]
                tmp_his = set([aID])
                tmp_price = len(his) - len(tmp_his)
                # tmp_list.append(tmp_price)
                table_price_history[aID] = tmp_his
                price += tmp_price/(support_size - 1) * tuple_price
            # print(tmp_list)

            for aID in table_price_history.keys():
                if(aID not in updated_id_list):
                    his = table_price_history[aID]
                    tmp_his = his - results_id
                    tmp_price = len(his) - len(tmp_his)
                    table_price_history[aID] = tmp_his
                    price += tmp_price/(support_size - 1) * tuple_price
            price_list.append(price)
        else:
            str1 = sql.split("from")[1]
            selected_attributes = {table: [table + ".aID"] for table in query_tables}
            # joined_keys() = {table: [] for table in query_tables}
            # for tmp_att in md.joins:
            #     att = tmp_att.sql()
            #     if "." in att:
            #         table = att.split(".")[0]
            #         # joined_keys()[table].append(att)
            #         selected_attributes[table].append(att)
            #     else:
            #         selected_attributes[attributes_list[att]].append(att)
            #         # joined_keys()[attributes_list[att]].append(att)

            id_select = [",".join(selected_attributes[table]) for table in query_tables]
            selected_idx = [len(selected_attributes[table]) for table in query_tables]
            str2 = ",".join(id_select)
            new_sql = "select " + str2 + " from " + str1
            # print(new_sql)
            o_results = select(new_sql, database= self.db)
            o_results = np.array(o_results)
            for i, table in enumerate(query_tables):
                price = 0
                support_size = self.support_size_list[table]
                tuple_price = self.tuple_price_list[table]
                table_price_history = self.price_history[table]  
                updated_id_list = []
                o_1 = []
                o_2 = []
                idx = selected_idx[i]
                if(i == 0):
                    pre_idx = 0
                else:
                    pre_idx = pre_idx + selected_idx[i-1]
                if(len(o_results) != 0):
                    o_1 = o_results[:, pre_idx:pre_idx+idx]
                    # remove duplicate
                    o_1 = np.unique(o_1, axis = 0)
                
                # n = len(o_1)
                if(self.support_suffix != ""):
                    new_sql = "select " + id_select[i] + " from " + str1
                    new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
                    new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                    new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                    s_results = select(new_sql, database= self.db)
                    if(len(s_results) != 0):
                        o_2 = np.array(s_results)
                        o_2 = np.unique(o_2, axis = 0)
                    
                updated_id_list = [int(item[0]) for item in o_1]
                results_id = set([int(item[0]) for item in o_2] + updated_id_list) 
                # print("----------------")
                # tmp_list = []
                for item in o_1:
                    aID = int(item[0])
                    his = table_price_history[aID]
                    tmp_his = set([aID])
                    tmp_price = len(his) - len(tmp_his)
                    # tmp_list.append(tmp_price)
                    table_price_history[aID] = tmp_his
                    price += tmp_price/(support_size - 1) * tuple_price
                # print(sum(tmp_list), len(tmp_list))

                for aID in table_price_history.keys():
                    if(aID not in updated_id_list):
                        his = table_price_history[aID]
                        tmp_his = his - results_id
                        tmp_price = len(his) - len(tmp_his)
                        table_price_history[aID] = tmp_his
                        price += tmp_price/(support_size - 1) * tuple_price
                price_list.append(price)
        return sum(price_list)

    def __price_SPJ_query_no_history__(self, sql):
        md = QueryMetaData(sql)
        # print(md)
        query_tables = md.tables
        price_list = []
        if(len(query_tables) == 1):
            price = 0
            # directly compute the query price
            table = query_tables[0]
            support_size = self.support_size_list[table]
            table_size = self.table_size_list[table]
            tuple_price = self.tuple_price_list[table]  
            o_results = select(sql, database= self.db)
            s_results = []
            if(self.support_suffix != ""):
                # new_sql = sql.replace(table, table + self.support_suffix)
                new_sql = sql.replace(table + " ", table + self.support_suffix + " ")
                new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                s_results = select(new_sql, database= self.db)
            
            # group by the results based on the selected attributes
            query_groups = defaultdict(int)
            for item in s_results:
                query_groups[tuple(item)] += 1
            for item in o_results:
                query_groups[tuple(item)] += 1
                
            # tmp_list = []
            effective_states = [query_groups[tuple(item)] for item in o_results]
            E_n = sum(effective_states)
            E_a = len(s_results) + len(o_results)
            n = len(o_results)
            price = support_size * n  - E_n
            price += (table_size - n) * (E_a)
            price = price / (support_size - 1) * tuple_price
            price_list.append(price)
        else:
            str1 = sql.split("from")[1]
            selected_attributes = {table: [table + ".aID"] for table in query_tables}
            # joined_keys() = {table: [] for table in query_tables}
            # for tmp_att in md.joins:
            #     att = tmp_att.sql()
            #     if "." in att:
            #         table = att.split(".")[0]
            #         # joined_keys()[table].append(att)
            #         selected_attributes[table].append(att)
            #     else:
            #         selected_attributes[attributes_list[att]].append(att)
            #         # joined_keys()[attributes_list[att]].append(att)
            for att in md.projections:
                if "." in att:
                    str_list = att.split(".")
                    table = str_list[0]
                    selected_attributes[table].append(att)
                else:
                    for tt in query_tables:
                        # print(tt)
                        # print(self.table_fields)
                        if(att in self.table_fields[tt]):
                            selected_attributes[tt].append(att)
                            break

            id_select = [",".join(selected_attributes[table]) for table in query_tables]
            selected_idx = [len(selected_attributes[table]) for table in query_tables]
            str2 = ",".join(id_select)
            new_sql = "select " + str2 + " from " + str1
            # print(new_sql)
            o_results = select(new_sql, database= self.db)
            o_results = np.array(o_results)
            for i, table in enumerate(query_tables):
                price = 0
                support_size = self.support_size_list[table]
                table_size = self.table_size_list[table]
                tuple_price = self.tuple_price_list[table]
                o_1 = []
                o_2 = []
                idx = selected_idx[i]
                if(i == 0):
                    pre_idx = 0
                else:
                    pre_idx = pre_idx + selected_idx[i-1]
                if(len(o_results) != 0):
                    o_1 = o_results[:, pre_idx:pre_idx+idx]
                    # remove duplicate
                    o_1 = np.unique(o_1, axis = 0)
                    o_1 = o_1[:, 1:] # remove the aID column
                    
                if(self.support_suffix != ""):
                    new_sql = "select " + id_select[i] + " from " + str1
                    # new_sql = new_sql.replace(table, table + self.support_suffix)
                    new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
                    new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                    new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                    s_results = select(new_sql, database= self.db)
                    if(len(s_results) != 0):
                        o_2 = np.array(s_results)
                        o_2 = np.unique(o_2, axis = 0)
                        o_2 = o_2[:, 1:] # remove the aID column
                    
                # group by the query results on the support set
                # group by the results based on the selected attributes
                query_groups = defaultdict(int)
                for item in o_2:
                    query_groups[tuple(item)] += 1
                for item in o_1:
                    query_groups[tuple(item)] += 1
                # tmp_list = []
                effective_states = [query_groups[tuple(item)] for item in o_1]
                E_n = sum(effective_states)
                E_a = len(o_2) + len(o_1)
                n = len(o_1)
                price = support_size * n  - E_n
                price += (table_size - n) * (E_a)
                price = price / (support_size - 1) * tuple_price
                price_list.append(price)
        return sum(price_list)
    

    def __price_SPJ_star_no_history__(self, sql):
        md = QueryMetaData(sql)
        # print(md)
        query_tables = md.tables
        price_list = []
        if(len(query_tables) == 1):
            price = 0
            # directly compute the query price
            table = query_tables[0]
            support_size = self.support_size_list[table]
            table_size = self.table_size_list[table]
            tuple_price = self.tuple_price_list[table]  
            sql = sql.replace("*", "count(*)")
            # print(sql)
            o_results = select(sql, database= self.db)
            s_results = [[0]]
            if(self.support_suffix != ""):
                # new_sql = sql.replace(table, table + self.support_suffix)
                new_sql = sql.replace(table + " ", table + self.support_suffix + " ")
                new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                s_results = select(new_sql, database= self.db)

            
            E_n = o_results[0][0] * 1
            E_a = s_results[0][0] + E_n
            n = o_results[0][0]
            price = support_size * n  - E_n
            price += (table_size - n) * (E_a)
            price = price / (support_size - 1) * tuple_price
            price_list.append(price)
        else:
            str1 = sql.split("from")[1]
            selected_attributes = {table: [table + ".aID"] for table in query_tables}
            # joined_keys() = {table: [] for table in query_tables}
            # for tmp_att in md.joins:
            #     att = tmp_att.sql()
            #     if "." in att:
            #         table = att.split(".")[0]
            #         # joined_keys()[table].append(att)
            #         selected_attributes[table].append(att)
            #     else:
            #         selected_attributes[attributes_list[att]].append(att)
            #         # joined_keys()[attributes_list[att]].append(att)

            id_select = [",".join(selected_attributes[table]) for table in query_tables]
            selected_idx = [len(selected_attributes[table]) for table in query_tables]
            str2 = ",".join(id_select)
            new_sql = "select " + str2 + " from " + str1
            # print(new_sql)
            o_results = select(new_sql, database= self.db)
            o_results = np.array(o_results)
            for i, table in enumerate(query_tables):
                price = 0
                support_size = self.support_size_list[table]
                table_size = self.table_size_list[table]
                tuple_price = self.tuple_price_list[table]
                o_1 = []
                o_2 = []
                idx = selected_idx[i]
                if(i == 0):
                    pre_idx = 0
                else:
                    pre_idx = pre_idx + selected_idx[i-1]
                if(len(o_results) != 0):
                    o_1 = o_results[:, pre_idx:pre_idx+idx]
                    # remove duplicate
                    o_1 = np.unique(o_1, axis = 0)
                
                
                # n = len(o_1)
                if(self.support_suffix != ""):
                    new_sql = "select " + id_select[i] + " from " + str1
                    # new_sql = new_sql.replace(table, table + self.support_suffix)
                    new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
                    new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                    new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                    s_results = select(new_sql, database= self.db)
                    if(len(s_results) != 0):
                        o_2 = np.array(s_results)
                        o_2 = np.unique(o_2, axis = 0)
                    
                   
                # tmp_list = []
                E_n = len(o_1) * 1
                E_a = len(o_2) + E_n
                n = len(o_1)
                price = support_size * n  - E_n
                price += (table_size - n) * (E_a)
                price = price / (support_size - 1) * tuple_price
                price_list.append(price)
        return sum(price_list)
    

    def __price_limit_query_history_aware__(self, sql):
        tmp_sql = sql.split("limit")
        sql = tmp_sql[0]
        k = int(tmp_sql[1])
        if(k == 0):
            return 0
        md = QueryMetaData(sql)
        # print(md)
        query_tables = md.tables
        price_list = []
        if(len(query_tables) == 1):
            price = 0
            # directly compute the query price
            table = query_tables[0]
            support_size = self.support_size_list[table]
            tuple_price = self.tuple_price_list[table]
            table_price_history = self.price_history[table]   
            updated_id_list = []
            sql = sql.replace("select", "select aID, ")
            o_results = select(sql, database= self.db)
            s_results = []
            if(self.support_suffix != ""):
                # new_sql = sql.replace(table, table + self.support_suffix)
                new_sql = sql.replace(table + " ", table + self.support_suffix + " ")
                new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                s_results = select(new_sql, database= self.db)
            
            # group by the results based on the selected attributes
            query_groups = defaultdict(list)
            results_id = []
            updated_id_list = []
            for item in o_results:
                aID = int(item[0])
                query_groups[tuple(item[1:])].append(aID)
                results_id.append(aID)
            for item in s_results:
                aID = int(item[0])
                query_groups[tuple(item[1:])].append(aID)
                results_id.append(aID)
            # tmp_list = []
            for item in o_results[:k]:
                aID = int(item[0])
                his = table_price_history[aID]
                tmp_his = his & set(query_groups[tuple(item[1:])])
                tmp_price = len(his) - len(tmp_his)
                # tmp_list.append(tmp_price)
                table_price_history[aID] = tmp_his
                price += tmp_price/(support_size - 1) * tuple_price
                updated_id_list.append(aID)
            # print(tmp_list)
            results_id = set(results_id) 
            if(k > len(o_results)):
                for aID in table_price_history.keys():
                    if(aID not in updated_id_list):
                        his = table_price_history[aID]
                        tmp_his = his - results_id
                        tmp_price = len(his) - len(tmp_his)
                        table_price_history[aID] = tmp_his
                        price += tmp_price/(support_size - 1) * tuple_price
            price_list.append(price)
        else:
            str1 = sql.split("from")[1]
            selected_attributes = {table: [table + ".aID"] for table in query_tables}
            # joined_keys() = {table: [] for table in query_tables}
            # for tmp_att in md.joins:
            #     att = tmp_att.sql()
            #     if "." in att:
            #         table = att.split(".")[0]
            #         # joined_keys()[table].append(att)
            #         selected_attributes[table].append(att)
            #     else:
            #         selected_attributes[attributes_list[att]].append(att)
            #         # joined_keys()[attributes_list[att]].append(att)
            for att in md.projections:
                if "." in att:
                    str_list = att.split(".")
                    table = str_list[0]
                    selected_attributes[table].append(att)
                else:
                    for tt in query_tables:
                        if(att in self.table_fields[tt]):
                            selected_attributes[tt].append(att)
                            break

            id_select = [",".join(selected_attributes[table]) for table in query_tables]
            selected_idx = [len(selected_attributes[table]) for table in query_tables]
            str2 = ",".join(id_select)
            new_sql = "select " + str2 + " from " + str1
            # print(new_sql)
            o_results = select(new_sql, database= self.db)
            o_results = np.array(o_results)
            tmp_n = len(o_results)
            o_results = o_results[:k]

            for i, table in enumerate(query_tables):
                o_1 = []
                o_2 = []
                price = 0
                support_size = self.support_size_list[table]
                tuple_price = self.tuple_price_list[table]
                table_price_history = self.price_history[table]  
                updated_id_list = []
                idx = selected_idx[i]
                if(i == 0):
                    pre_idx = 0
                else:
                    pre_idx = pre_idx + selected_idx[i-1]
                if(len(o_results)!=0):
                    o_1 = o_results[:, pre_idx:pre_idx+idx]
                    # remove duplicate
                    o_1 = np.unique(o_1, axis = 0)
                # n = len(o_1)
                if(self.support_suffix != ""):
                    new_sql = "select " + id_select[i] + " from " + str1
                    # new_sql = new_sql.replace(table, table + self.support_suffix)
                    new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
                    new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                    new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                    s_results = select(new_sql, database= self.db)
                    if(len(s_results) != 0):
                        o_2 = np.array(s_results)
                        o_2 = np.unique(o_2, axis = 0)
                # group by the query results on the support set
                # group by the results based on the selected attributes
                query_groups = defaultdict(list)
                results_id = []
                for item in o_1:
                    aID = int(item[0])
                    query_groups[tuple(item[1:])].append(aID)
                    results_id.append(aID)
                for item in o_2:
                    aID = int(item[0])
                    query_groups[tuple(item[1:])].append(aID)
                    results_id.append(aID)
                # print("----------------")
                # tmp_list = []
                for item in o_1:
                    aID = int(item[0])
                    his = table_price_history[aID]
                    tmp_his = his & set(query_groups[tuple(item[1:])])
                    tmp_price = len(his) - len(tmp_his)
                    # tmp_list.append(tmp_price)
                    table_price_history[aID] = tmp_his
                    price += tmp_price/(support_size - 1) * tuple_price
                    updated_id_list.append(aID)
                # print(sum(tmp_list), len(tmp_list))
                results_id = set(results_id) 
                if(k > tmp_n):
                    for aID in table_price_history.keys():
                        if(aID not in updated_id_list):
                            his = table_price_history[aID]
                            tmp_his = his - results_id
                            tmp_price = len(his) - len(tmp_his)
                            table_price_history[aID] = tmp_his
                            price += tmp_price/(support_size - 1) * tuple_price
                price_list.append(price)
        return sum(price_list)

    def __price_limit_star_history_aware__(self, sql):
        tmp_sql = sql.split("limit")
        sql = tmp_sql[0]
        k = int(tmp_sql[1])
        if(k == 0):
            return 0
        md = QueryMetaData(sql)
        # print(md)
        query_tables = md.tables
        price_list = []
        if(len(query_tables) == 1):
            price = 0
            # directly compute the query price
            table = query_tables[0]
            support_size = self.support_size_list[table]
            tuple_price = self.tuple_price_list[table]
            table_price_history = self.price_history[table]   
            updated_id_list = []
            sql = sql.replace("*", "aID")
            o_results = select(sql, database= self.db)
            s_results = []
            if(self.support_suffix != ""):
                # new_sql = sql.replace(table, table + self.support_suffix)
                new_sql = sql.replace(table + " ", table + self.support_suffix + " ")
                new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                s_results = select(new_sql, database= self.db)
            
            updated_id_list = [int(item[0]) for item in o_results]
            results_id = set([int(item[0]) for item in s_results] + updated_id_list)
            for item in o_results[:k]:
                aID = int(item[0])
                his = table_price_history[aID]
                tmp_his = set([aID])
                tmp_price = len(his) - len(tmp_his)
                # tmp_list.append(tmp_price)
                table_price_history[aID] = tmp_his
                price += tmp_price/(support_size - 1) * tuple_price
                
            if(k > len(o_results)):
                for aID in table_price_history.keys():
                    if(aID not in updated_id_list):
                        his = table_price_history[aID]
                        tmp_his = his - results_id
                        tmp_price = len(his) - len(tmp_his)
                        table_price_history[aID] = tmp_his
                        price += tmp_price/(support_size - 1) * tuple_price
            price_list.append(price)
        else:
            str1 = sql.split("from")[1]
            selected_attributes = {table: [table + ".aID"] for table in query_tables}
            # joined_keys() = {table: [] for table in query_tables}
            # for tmp_att in md.joins:
            #     att = tmp_att.sql()
            #     if "." in att:
            #         table = att.split(".")[0]
            #         # joined_keys()[table].append(att)
            #         selected_attributes[table].append(att)
            #     else:
            #         selected_attributes[attributes_list[att]].append(att)
            #         # joined_keys()[attributes_list[att]].append(att)

            id_select = [",".join(selected_attributes[table]) for table in query_tables]
            selected_idx = [len(selected_attributes[table]) for table in query_tables]
            str2 = ",".join(id_select)
            new_sql = "select " + str2 + " from " + str1
            # print(new_sql)
            o_results = select(new_sql, database= self.db)
            o_results = np.array(o_results)
            tmp_n = len(o_results)
            o_results = o_results[:k]

            for i, table in enumerate(query_tables):
                price = 0
                o_1 = []
                o_2 = []
                support_size = self.support_size_list[table]
                tuple_price = self.tuple_price_list[table]
                table_price_history = self.price_history[table]  
                updated_id_list = []
                idx = selected_idx[i]
                if(i == 0):
                    pre_idx = 0
                else:
                    pre_idx = pre_idx + selected_idx[i-1]
                if(len(o_results) != 0):
                    o_1 = o_results[:, pre_idx:pre_idx+idx]
                    # remove duplicate
                    o_1 = np.unique(o_1, axis = 0)
                # n = len(o_1)
                if(self.support_suffix != ""):
                    new_sql = "select " + id_select[i] + " from " + str1
                    # new_sql = new_sql.replace(table, table + self.support_suffix)
                    new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
                    new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                    new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                    s_results = select(new_sql, database= self.db)
                    if(len(s_results) != 0):
                        o_2 = np.array(s_results)
                        o_2 = np.unique(o_2, axis = 0)
                
                updated_id_list = [int(item[0]) for item in o_1]
                results_id = set([int(item[0]) for item in o_2] + updated_id_list)
                for item in o_1:
                    aID = int(item[0])
                    his = table_price_history[aID]
                    tmp_his = set([aID])
                    tmp_price = len(his) - len(tmp_his)
                    # tmp_list.append(tmp_price)
                    table_price_history[aID] = tmp_his
                    price += tmp_price/(support_size - 1) * tuple_price
                     
                if(k > tmp_n):
                    for aID in table_price_history.keys():
                        if(aID not in updated_id_list):
                            his = table_price_history[aID]
                            tmp_his = his - results_id
                            tmp_price = len(his) - len(tmp_his)
                            table_price_history[aID] = tmp_his
                            price += tmp_price/(support_size - 1) * tuple_price
                price_list.append(price)
        return sum(price_list)

    def __price_limit_star_no_history__(self, sql):
        tmp_sql = sql.split("limit")
        sql = tmp_sql[0]
        k = int(tmp_sql[1])
        if(k == 0):
            return 0
        md = QueryMetaData(sql)
        # print(md)
        query_tables = md.tables
        price_list = []
        if(len(query_tables) == 1):
            price = 0
            # directly compute the query price
            table = query_tables[0]
            support_size = self.support_size_list[table]
            table_size = self.table_size_list[table]
            tuple_price = self.tuple_price_list[table]  
            sql = sql.replace("*", "count(*)")
            o_results = select(sql, database= self.db)
            s_results = [[0]]
            if(self.support_suffix != ""):
                # new_sql = sql.replace(table, table + self.support_suffix)
                new_sql = sql.replace(table + " ", table + self.support_suffix + " ")
                new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                s_results = select(new_sql, database= self.db)
            
            n = min(k, o_results[0][0])
            E_n = n * 1
            if(k > o_results[0][0]):
                E_a = s_results[0][0] + o_results[0][0]
            else:
                E_a = 0
            price = support_size * n  - E_n
            price += (table_size - n) * (E_a)
            price = price / (support_size - 1) * tuple_price
            price_list.append(price)
        else:
            str1 = sql.split("from")[1]
            selected_attributes = {table: [table + ".aID"] for table in query_tables}
            # joined_keys() = {table: [] for table in query_tables}
            # for tmp_att in md.joins:
            #     att = tmp_att.sql()
            #     if "." in att:
            #         table = att.split(".")[0]
            #         # joined_keys()[table].append(att)
            #         selected_attributes[table].append(att)
            #     else:
            #         selected_attributes[attributes_list[att]].append(att)
            #         # joined_keys()[attributes_list[att]].append(att)

            id_select = [",".join(selected_attributes[table]) for table in query_tables]
            selected_idx = [len(selected_attributes[table]) for table in query_tables]
            str2 = ",".join(id_select)
            new_sql = "select " + str2 + " from " + str1
            # print(new_sql)
            o_results = select(new_sql, database= self.db)
            o_results = np.array(o_results)
            tmp_n = len(o_results)
            o_results = o_results[:k]
            for i, table in enumerate(query_tables):
                price = 0
                o_1 = []
                o_2 = []
                support_size = self.support_size_list[table]
                table_size = self.table_size_list[table]
                tuple_price = self.tuple_price_list[table]
                idx = selected_idx[i]
                if(i == 0):
                    pre_idx = 0
                else:
                    pre_idx = pre_idx + selected_idx[i-1]
                if(len(o_results) != 0):
                    o_1 = o_results[:, pre_idx:pre_idx+idx]
                    # remove duplicate
                    o_1 = np.unique(o_1, axis = 0)
                    o_1 = o_1[:, 1:] # remove the aID column
                # n = len(o_1)
                if(self.support_suffix != ""):
                    new_sql = "select " + id_select[i] + " from " + str1
                    # new_sql = new_sql.replace(table, table + self.support_suffix)
                    new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
                    new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                    new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                    s_results = select(new_sql, database= self.db)
                    if(len(s_results) != 0):
                        o_2 = np.array(s_results)
                        o_2 = np.unique(o_2, axis = 0)
                        o_2 = o_2[:, 1:] # remove the aID column
                   
                if(k > tmp_n):
                    E_a = len(o_2) + len(o_1)
                else:
                    E_a = 0
                n = len(o_1)
                E_n = n * 1
                price = support_size * n  - E_n
                price += (table_size - n) * (E_a)
                price = price / (support_size - 1) * tuple_price
                price_list.append(price)
        return sum(price_list)
    def __price_limit_query_no_history__(self, sql):
        tmp_sql = sql.split("limit")
        sql = tmp_sql[0]
        k = int(tmp_sql[1])
        if(k == 0):
            return 0
        md = QueryMetaData(sql)
        # print(md)
        query_tables = md.tables
        price_list = []
        if(len(query_tables) == 1):
            price = 0
            # directly compute the query price
            table = query_tables[0]
            support_size = self.support_size_list[table]
            table_size = self.table_size_list[table]
            tuple_price = self.tuple_price_list[table]  
            o_results = select(sql, database= self.db)
            s_results = []
            if(self.support_suffix != ""):
                
                # new_sql = sql.replace(table, table + self.support_suffix)
                new_sql = sql.replace(table + " ", table + self.support_suffix + " ")
                new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                s_results = select(new_sql, database= self.db)
            
            # group by the results based on the selected attributes
            query_groups = defaultdict(int)
            for item in o_results:
                query_groups[tuple(item)] += 1
            for item in s_results:
                query_groups[tuple(item)] += 1
                
            # tmp_list = []
            effective_states = [query_groups[tuple(item)] for item in o_results[:k]]
            E_n = sum(effective_states)
            n = len(o_results[:k])
            if(k > len(o_results)):
                E_a = len(s_results) + len(o_results)
            else:
                E_a = 0
            price = support_size * n  - E_n
            price += (table_size - n) * (E_a)
            price = price / (support_size - 1) * tuple_price
            price_list.append(price)
        else:
            str1 = sql.split("from")[1]
            selected_attributes = {table: [table + ".aID"] for table in query_tables}
            # joined_keys() = {table: [] for table in query_tables}
            # for tmp_att in md.joins:
            #     att = tmp_att.sql()
            #     if "." in att:
            #         table = att.split(".")[0]
            #         # joined_keys()[table].append(att)
            #         selected_attributes[table].append(att)
            #     else:
            #         selected_attributes[attributes_list[att]].append(att)
            #         # joined_keys()[attributes_list[att]].append(att)
            for att in md.projections:
                if "." in att:
                    str_list = att.split(".")
                    table = str_list[0]
                    selected_attributes[table].append(att)
                else:
                    for tt in query_tables:
                        if(att in self.table_fields[tt]):
                            selected_attributes[tt].append(att)
                            break

            id_select = [",".join(selected_attributes[table]) for table in query_tables]
            selected_idx = [len(selected_attributes[table]) for table in query_tables]
            str2 = ",".join(id_select)
            new_sql = "select " + str2 + " from " + str1
            # print(new_sql)
            o_results = select(new_sql, database= self.db)
            o_results = np.array(o_results)
            tmp_n = len(o_results)
            o_results = o_results[:k]
            for i, table in enumerate(query_tables):
                price = 0
                o_1 = []
                o_2 = []
                support_size = self.support_size_list[table]
                table_size = self.table_size_list[table]
                tuple_price = self.tuple_price_list[table]
                idx = selected_idx[i]
                if(i == 0):
                    pre_idx = 0
                else:
                    pre_idx = pre_idx + selected_idx[i-1]
                if(len(o_results) != 0):
                    
                    # print(pre_idx, idx)
                    o_1 = o_results[:, pre_idx:pre_idx+idx]
                    # remove duplicate
                    o_1 = np.unique(o_1, axis = 0)
                    o_1 = o_1[:, 1:] # remove the aID column
                    # n = len(o_1)
                if(self.support_suffix != ""):
                    new_sql = "select " + id_select[i] + " from " + str1
                    # new_sql = new_sql.replace(table, table + self.support_suffix)
                    new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
                    new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
                    new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
                    s_results = select(new_sql, database= self.db)
                    o_2 = np.array(s_results)
                    if(len(o_2) != 0):
                        o_2 = np.unique(o_2, axis = 0)
                        o_2 = o_2[:, 1:] # remove the aID column
                # group by the query results on the support set
                # group by the results based on the selected attributes
                query_groups = defaultdict(int)
                for item in o_1:
                    query_groups[tuple(item)] += 1
                for item in o_2:
                    query_groups[tuple(item)] += 1 
                # tmp_list = []
                effective_states = [query_groups[tuple(item)] for item in o_1]
                E_n = sum(effective_states)
                if(k > tmp_n):
                    E_a = len(o_2) + len(o_1)
                else:
                    E_a = 0
                n = len(o_1)
                price = support_size * n  - E_n
                price += (table_size - n) * (E_a)
                price = price / (support_size - 1) * tuple_price
                price_list.append(price)

        return sum(price_list)
    
    def __price_extreme_query_history_aware__(self, sql):
        md = QueryMetaData(sql)
        if(len(md.tables) != 1):
            print("ARIA cannot directly price the max/min queries over joined tables")
            print("Please do the join first and compute the aggregations by yourself")
            return -1
        table = md.tables[0]
        price = 0
        support_size = self.support_size_list[table]
        # table_size = self.table_size_list[table]
        tuple_price = self.tuple_price_list[table]
        price_history = self.price_history[table]
        # construct the new query without max/min and group by clauses
        new_sql = sql.split("group")[0]
        new_sql = new_sql.replace("max(", "(")
        new_sql = new_sql.replace("min(", "(")
        # add the aID column into the sql query
        new_sql = new_sql.replace("select ", "select aID, ")
        # print(new_sql, sql)
        # obtain the query answer 
        o_results = select(new_sql, database= self.db)
        if(self.support_suffix != ""):
            # new_sql = new_sql.replace(table, table + self.support_suffix)
            new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
            new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
            new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
            s_results = select(new_sql, database= self.db)
        else:
            s_results = []
        # get the max/min value
        n = len(o_results)
        if(n == 0):
            # empty results
            results_id = [item[0] for item in s_results] + [item[0] for item in o_results]
            if(len(results_id) == 0):
                return 0
            else:
                results_id = set(results_id)
                for aID in price_history.keys():
                    tmp_his = price_history[aID]
                    new_his = tmp_his - results_id
                    tmp_price = len(tmp_his) - len(new_his)
                    price_history[aID] = new_his
                    price += tmp_price / (support_size - 1) * tuple_price
                return price
        if("group by" not in sql and self.support_suffix == ""):
            # sort the original results 
            extreme_value = None
            if("max(" in sql):
                extreme_value = max(o_results, key=lambda x: float('-inf') if x is None or x[-1] is None else x[-1])[-1]
                # o_results = sorted(o_results, key = lambda x: (x is None or x[-1] is None, x[-1]), reverse= True)
            else:
                extreme_value = min(o_results, key=lambda x: float('inf') if x is None or x[-1] is None else x[-1])[-1]
                # o_results = sorted(o_results, key = lambda x: (x is None or x[-1] is None, x[-1]))
            # for item in o_results:
            #     if(item is not None and item[-1] is not None):
            #         extreme_value = item[-1]
            #         # extreme_id_list.append(item[0])
            #         break
            extreme_id_list = [item[0] for item in o_results if item[-1] == extreme_value]
            # find the tuple with the maximum price
            tmp_price = -1
            tmp_id = -1
            for id in extreme_id_list:
                eff_his = set(price_history[id])
                tmp_eff = eff_his & set(extreme_id_list)
                if(tmp_price < len(eff_his) - len(tmp_eff)):
                    tmp_price = len(eff_his) - len(tmp_eff)
                    tmp_id = id
            # update the effective states
            eff_his = set(price_history[tmp_id])
            price_history[tmp_id] = eff_his & set(extreme_id_list)
            # print(tmp_price/(support_size-1))
            price = tmp_price/(support_size - 1) * tuple_price
            return price
            # the effective states of other tuples are the S_t 
            # have no change and no price
        if("group by" in sql and self.support_suffix == ""):
            results_id = [item[0] for item in s_results]
            # group by the original results
            query_groups = defaultdict(list)
            for item in o_results:
                # print(tuple(item[1:-1]))
                query_groups[tuple(item[1:-1])].append(item)
            for group_v in query_groups.keys():
                group_results = query_groups[group_v]
                extreme_value = None
                if("max(" in sql):
                    extreme_value = max(group_results, key=lambda x: float('-inf') if x is None or x[-1] is None else x[-1])[-1]
                    # o_results = sorted(o_results, key = lambda x: (x is None or x[-1] is None, x[-1]), reverse= True)
                else:
                    extreme_value = min(group_results, key=lambda x: float('inf') if x is None or x[-1] is None else x[-1])[-1]
                extreme_id_list = [item[0] for item in group_results if item[-1] == extreme_value]
                # find the tuple with the maximum price
                tmp_price = -1
                tmp_id = -1
                for id in extreme_id_list:
                    eff_his = set(price_history[id])
                    tmp_eff = eff_his & set(extreme_id_list)
                    if(tmp_price < len(eff_his) - len(tmp_eff)):
                        tmp_price = len(eff_his) - len(tmp_eff)
                        tmp_id = id
                # update the effective states
                eff_his = set(price_history[tmp_id])
                price_history[tmp_id] = eff_his & set(extreme_id_list)
                # print(tmp_price)
                price += tmp_price/(support_size - 1) * tuple_price
            return price

        if("group by" not in sql and self.support_suffix != ""):
            # do not group by 
        
            
            # s_results = sorted(s_results, key = lambda x: x[-1])
            extreme_value = None
            if("max(" in sql):
                extreme_value = max(o_results, key=lambda x: float('-inf') if x is None or x[-1] is None else x[-1])[-1]
                # o_results = sorted(o_results, key = lambda x: (x is None or x[-1] is None, x[-1]), reverse= True)
            else:
                extreme_value = min(o_results, key=lambda x: float('inf') if x is None or x[-1] is None else x[-1])[-1]
            
            extreme_id_list = []
            s_extreme_id_list = []
            results_id = []
            effective_id_list = []
            for item in s_results:
                results_id.append(item[0])
                if(item[-1] is not None and item[-1] == extreme_value):
                    s_extreme_id_list.append(item[0])
                if("max(" in sql):
                    if(item[-1] is None or item[-1] <= extreme_value):
                        effective_id_list.append(item[0])
                elif("min(" in sql):
                    if(item[-1] is None or item[-1] >= extreme_value):
                        effective_id_list.append(item[0])
            for item in o_results:
                results_id.append(item[0])
                if(item[-1] is not None and item[-1] == extreme_value):
                    extreme_id_list.append(item[0])
                    s_extreme_id_list.append(item[0])
                if("max(" in sql):
                    if(item[-1] is None or item[-1] <= extreme_value):
                        effective_id_list.append(item[0])
                elif("min(" in sql):
                    if(item[-1] is None or item[-1] >= extreme_value):
                        effective_id_list.append(item[0])

            # find the tuple with the maximum price
            tmp_price = -1
            tmp_id = -1
            for id in extreme_id_list:
                eff_his = set(price_history[id])
                tmp_eff = eff_his & set(s_extreme_id_list)
                if(tmp_price < len(eff_his) - len(tmp_eff)):
                    tmp_price = len(eff_his) - len(tmp_eff)
                    tmp_id = id
            # update the effective states
            eff_his = set(price_history[tmp_id])
            price_history[tmp_id] = eff_his & set(s_extreme_id_list)
            price += tmp_price/(support_size - 1) * tuple_price
            # the effective states of other tuples are the S_t - results_id + effective_id_list 
            ineffective_set = set(results_id) - set(effective_id_list)
            for id in price_history.keys():
                if(id not in [tmp_id]):
                    eff_his = set(price_history[id])
                    tmp_eff = eff_his - ineffective_set
                    tmp_price = len(eff_his) - len(tmp_eff)
                    price_history[id]  = tmp_eff
                    price += tmp_price/(support_size - 1) * tuple_price
            return price
        if("group by" in sql and self.support_suffix != ""):
            results_id = [item[0] for item in s_results]
            effective_id_list = []
            updated_extreme_id_list = []
            price = 0
            # group by the original results 
            o_query_groups = defaultdict(list)
            s_query_groups = defaultdict(list)
            for item in o_results:
                o_query_groups[tuple(item[1:-1])].append(item)
                s_query_groups[tuple(item[1:-1])].append(item)
            for item in s_results:
                s_query_groups[tuple(item[1:-1])].append(item)
            for group_v in o_query_groups.keys():
                group_r = o_query_groups[group_v]
                group_s = s_query_groups[group_v]
                extreme_value = 0
                extreme_id_list = []
                s_extreme_id_list = []
                if("max(" in sql):
                    extreme_value = max(group_r, key=lambda x: float('-inf') if x is None else x[-1])[-1]
                    extreme_id_list = [item[0] for item in group_r if item[-1] == extreme_value ]
                    for item in group_s:
                        # print(item[-1], extreme_value)
                        if(item[-1] is not None and (item[-1]) == extreme_value): # take the same value 
                            s_extreme_id_list.append(item[0])
                        if(item[-1] is None or item[-1] <= extreme_value): # take the effective (no higher) value 
                            effective_id_list.append(item[0])
                else:
                    extreme_value = min(group_r, key=lambda x: float('inf') if x is None else x[-1])[-1] 
                    extreme_id_list = [item[0] for item in group_r if item[-1] == extreme_value ]
                    for item in group_s:
                        if(item[-1] is not None and item[-1] == extreme_value): # take the same value 
                            s_extreme_id_list.append(item[0])
                        if(item[-1] is None or item[-1] >= extreme_value): # take the effective (no less) value 
                            effective_id_list.append(item[0])
                # find the tuple with the maximum price
                tmp_price = -1
                tmp_id = -1
                for id in extreme_id_list:
                    eff_his = set(price_history[id])
                    tmp_eff = eff_his & set(s_extreme_id_list)
                    if(tmp_price < len(eff_his) - len(tmp_eff)):
                        tmp_price = len(eff_his) - len(tmp_eff)
                        tmp_id = id
                # update the effective states
                eff_his = set(price_history[tmp_id])
                price_history[tmp_id] = eff_his & set(s_extreme_id_list)
                price += tmp_price/(support_size - 1) * tuple_price
                # print(tmp_price)
                updated_extreme_id_list.append(tmp_id)
                # the effective states of other tuples are the S_t - results_id + effective_id_list 
                ineffective_set = set(results_id) - set(effective_id_list)
            for id in price_history.keys():
                if(id not in updated_extreme_id_list):
                    eff_his = set(price_history[id])
                    tmp_eff = eff_his - ineffective_set
                    tmp_price = len(eff_his) - len(tmp_eff)
                    price_history[id]  = tmp_eff
                    price += tmp_price/(support_size - 1) * tuple_price
            return price

    def __price_extreme_query_no_history__(self, sql):
        md = QueryMetaData(sql)
        if(len(md.tables) != 1):
            print("ARIA cannot directly price the max/min queries over joined tables")
            print("Please do the join first and compute the aggregations by yourself")
            return -1
        table = md.tables[0]
        price = 0
        support_size = self.support_size_list[table]
        table_size = self.table_size_list[table]
        tuple_price = self.tuple_price_list[table]
        # construct the new query without max/min and group by clauses
        new_sql = sql.split("group")[0]
        new_sql = new_sql.replace("max(", "(")
        new_sql = new_sql.replace("min(", "(")
        # print(new_sql, sql)
        # obtain the query answer 
        o_results = select(new_sql, database= self.db)
        if(self.support_suffix != ""):
            # new_sql = new_sql.replace(table, table + self.support_suffix)
            new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
            new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
            new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
            s_results = select(new_sql, database= self.db)
        else:
            s_results = []
        # get the max/min value
        n = len(o_results)
        if(n == 0):
            # empty results
            # results_id = [item[0] for item in s_results] + [item[0] for item in o_results]
            E_a = len(s_results) + len(o_results)
            tmp_price = E_a / (support_size - 1) * tuple_price
            price += (table_size - n) * tmp_price
            return price
        if("group by" not in sql and self.support_suffix == ""):
            # sort the original results 
            # print("-------")
            # s1 = time.time()
            extreme_value = None
            if("max(" in sql):
                # extreme_value = max(o_results)[-1]
                extreme_value = max(o_results, key=lambda x: float('-inf') if x is None or x[-1] is None else x[-1])[-1]
                # o_results = sorted(o_results, key = lambda x: (x is None or x[-1] is None, x[-1]), reverse= True)
            else:
                extreme_value = min(o_results, key=lambda x: float('inf') if x is None or x[-1] is None else x[-1])[-1]
            extreme_id_list = [item[0] for item in o_results if item[-1] == extreme_value]
            tmp_price = (support_size - len(extreme_id_list))
            price = tmp_price/(support_size - 1) * tuple_price
            # print(time.time()-s1)
            return price
            # the effective states of other tuples are the S_t 
            # have no change and no price
        if("group by" in sql and self.support_suffix == ""):
            price = 0
            # group by the original results
            query_groups = defaultdict(list)
            for item in o_results:
                # print(tuple(item[1:-1]))
                query_groups[tuple(item[1:-1])].append(item)
            for group_v in query_groups.keys():
                group_results = query_groups[group_v]
                extreme_value = None
                if("max(" in sql):
                    extreme_value = max(group_results, key=lambda x: float('-inf') if x is None or x[-1] is None else x[-1])[-1]
                    # o_results = sorted(o_results, key = lambda x: (x is None or x[-1] is None, x[-1]), reverse= True)
                else:
                    extreme_value = min(group_results, key=lambda x: float('inf') if x is None or x[-1] is None else x[-1])[-1]
                extreme_id_list = [item[0] for item in group_results if item[-1] == extreme_value]
                # find the tuple with the maximum price
                tmp_price = (support_size - len(extreme_id_list))
                price += tmp_price/(support_size - 1) * tuple_price
            return price

        if("group by" not in sql and self.support_suffix != ""):
            # do not group by
            
            E_a = len(s_results) + len(o_results)
            # sort the original results 
            # extreme_value = max(o_results, key=lambda x: float('-inf') if x is None or x[-1] is None else x[-1])[-1]
            o_results = sorted(o_results, key = lambda x: float('-inf') if x is None or x[-1] is None else x[-1])
            s_results = sorted(s_results, key = lambda x: float('inf') if x is None or x[-1] is None else x[-1])
            extreme_value = None
            if("max(" in sql):
                extreme_value = max(o_results, key=lambda x: float('-inf') if x is None or x[-1] is None else x[-1])[-1]
                # o_results = sorted(o_results, key = lambda x: (x is None or x[-1] is None, x[-1]), reverse= True)
            else:
                extreme_value = min(o_results, key=lambda x: float('inf') if x is None or x[-1] is None else x[-1])[-1]
            extreme_value = None
            if("max(" in sql):
                extreme_value = max(o_results, key=lambda x: float('-inf') if x is None or x[-1] is None else x[-1])[-1]
                # o_results = sorted(o_results, key = lambda x: (x is None or x[-1] is None, x[-1]), reverse= True)
            else:
                extreme_value = min(o_results, key=lambda x: float('inf') if x is None or x[-1] is None else x[-1])[-1]
            
            
            s_extreme_id_list = []
            effective_id_list = []
            for item in s_results:
                if(item[-1] is not None and item[-1] == extreme_value):
                    s_extreme_id_list.append(item[0])
                if("max(" in sql):
                    if(item[-1] is None or item[-1] <= extreme_value):
                        effective_id_list.append(item[0])
                elif("min(" in sql):
                    if(item[-1] is None or item[-1] >= extreme_value):
                        effective_id_list.append(item[0])
            for item in o_results:
                if(item[-1] is not None and item[-1] == extreme_value):
                    s_extreme_id_list.append(item[0])
                if("max(" in sql):
                    if(item[-1] is None or item[-1] <= extreme_value):
                        effective_id_list.append(item[0])
                elif("min(" in sql):
                    if(item[-1] is None or item[-1] >= extreme_value):
                        effective_id_list.append(item[0])
            
            # find the tuple with the maximum price
            tmp_price = (support_size - len(s_extreme_id_list))
            price += tmp_price/(support_size - 1) * tuple_price
            # the effective states of other tuples are the S_t - results_id + effective_id_list 
            E_a = E_a - len(effective_id_list)
            price += (E_a/(support_size - 1) * tuple_price) * (table_size - 1)
            return price
        if("group by" in sql and self.support_suffix != ""):
            E_a = len(s_results) + len(o_results)
            effective_id_list = []
            price = 0
            # group by the original results 
            o_query_groups = defaultdict(list)
            s_query_groups = defaultdict(list)
            for item in o_results:
                o_query_groups[tuple(item[1:-1])].append(item)
                s_query_groups[tuple(item[1:-1])].append(item)
            for item in s_results:
                s_query_groups[tuple(item[1:-1])].append(item)
            for group_v in o_query_groups.keys():
                group_r = o_query_groups[group_v]
                group_s = s_query_groups[group_v]
                extreme_value = None
                extreme_id_list = []
                s_extreme_id_list = []
                if("max(" in sql):
                    extreme_value = max(group_r, key=lambda x: float('-inf') if x is None else x[-1])[-1]
                    extreme_id_list = [item[0] for item in group_r if item[-1] == extreme_value ]
                    for item in group_s:
                        # print(item[-1], extreme_value)
                        if(item[-1] is not None and (item[-1]) == extreme_value): # take the same value 
                            s_extreme_id_list.append(item[0])
                        if(item[-1] is None or item[-1] <= extreme_value): # take the effective (no higher) value 
                            effective_id_list.append(item[0])
                else:
                    extreme_value = min(group_r, key=lambda x: float('inf') if x is None else x[-1])[-1]
                    extreme_id_list = [item[0] for item in group_r if item[-1] == extreme_value ]
                    for item in group_s:
                        if(item[-1] is not None and item[-1] == extreme_value): # take the same value 
                            s_extreme_id_list.append(item[0])
                        if(item[-1] is None or item[-1] >= extreme_value): # take the effective (no less) value 
                            effective_id_list.append(item[0])
                # find the tuple with the maximum price
                tmp_price = support_size - len(s_extreme_id_list)
                price += tmp_price/(support_size - 1) * tuple_price
                # print(tmp_price)
                
                # the effective states of other tuples are the S_t - results_id + effective_id_list 
                E_a = E_a - len(effective_id_list)
            
            price += E_a/(support_size - 1) * tuple_price * (table_size - len(o_query_groups.keys()))
            return price
    
    def __price_avg_query_history_aware__(self, sql):
        md = QueryMetaData(sql)
        if(len(md.tables) != 1):
            print("ARIA cannot directly price the average queries over joined tables")
            print("Please do the join first and compute the aggregations by yourself")
            return -1
        table = md.tables[0]
        price = 0
        support_size = self.support_size_list[table]
        price_history = self.price_history[table]
        tuple_price = self.tuple_price_list[table]
        # construct the new query without avg and group by clauses
        new_sql = sql.split("group")[0]
        new_sql = new_sql.replace("avg(", "")
        new_sql = new_sql.replace("sum(", "")
        # add the aID column into the sql query
        new_sql = new_sql.replace("select ", "select aID, ")
        new_sql = new_sql.replace(")", "")
        md = QueryMetaData(new_sql)
        # get the average column
        # print(new_sql, md.projections)
        att = md.projections[-1]
        x_l = self.field_domin[table+ "." + att][0]
        x_h = self.field_domin[table+ "." + att][1]
        # obtain the query answer 
        o_results = select(new_sql, database= self.db)
        # get the average value and group by the results
        if(self.support_suffix != ""):
            # new_sql = new_sql.replace(table, table + self.support_suffix)
            new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
            new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
            new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
            s_results = select(new_sql, database= self.db)
        else:
            s_results = []
        # get the average value
        n = len(o_results)
        if(n == 0):
            # empty results
            price = 0
            if(len(s_results) != 0):
                results_id = [item[0] for item in s_results] + [item[0] for item in o_results]
                for id in price_history.keys():
                    his = price_history[id]
                    his = his - set(results_id)
                    tmp_price = len(price_history[id]) - len(his)
                    price += tmp_price/(support_size - 1) * tuple_price
                    price_history[id] = his
            return price
        # self.support_suffix = "1"
        if("group by" in sql and self.support_suffix == ""):
            # group by the results based on the selected attributes
            real_groups = defaultdict(list)
            updated_id_list = []
            results_id = [item[0] for item in o_results]
            for item in o_results:
                real_groups[tuple(item[1:-1])].append([item[0], item[-1]])
            price = 0
            sum_list = [sum([kk[-1] for kk in real_groups[item]])  for item in real_groups.keys()]
            n_list = [len(real_groups[item]) for item in real_groups.keys()]
            for j, item in enumerate(real_groups.keys()):
                item_groups = real_groups[item]
                item_groups = sorted(item_groups, key = lambda x: x[-1])
                s_values = [kk[-1] for kk in item_groups]
                results_groups = real_groups[item]
                sum_value = float(sum_list[j])
                # print(j, float(sum_list[j]))
                n = n_list[j]
                for i in range(n):
                    effective_id_list = []
                    a_i = max(x_l, (sum_value - (n - i - 1) * x_h)/(i + 1))
                    b_i = min(x_h, (sum_value - (i) * x_l)/(n - i))
                    # count the number of values in the range
                    aID = item_groups[i][0]
                    cnt, left_idx, right_idx = count_and_idx(s_values, a_i, b_i)
                    for k in range(left_idx, right_idx):
                        effective_id_list.append(item_groups[k][0])
                    # for tmp_item in item_groups:
                    #     if(a_i <= tmp_item[-1] and tmp_item[-1] <= b_i):
                    #         effecitve_states[item].append(tmp_item[0])
                    his = price_history[aID]
                    his = his & set(effective_id_list)
                    tmp_price = len(price_history[aID]) - len(his)
                    price += tmp_price/(support_size - 1) * tuple_price
                    # print(tmp_price, len(effective_id_list))
                    price_history[aID] = his
                    updated_id_list.append(aID)
            for aID in price_history.keys():
                if(aID not in updated_id_list):
                    his = price_history[aID]
                    his = his - set(results_id)
                    tmp_price = len(price_history[id]) - len(his)
                    price += tmp_price/(support_size - 1) * tuple_price
                    price_history[aID] = his
            # price_list.append(price)
            return price
        if("group by" in sql and self.support_suffix != ""):
            # group by the results based on the selected attributes
            query_groups = defaultdict(list)
            real_groups = defaultdict(list)
            results_id = []
            updated_id_list = []
            price = 0
            for item in s_results:
                query_groups[tuple(item[1:-1])].append([item[0], item[-1]])
                results_id.append(item[0])
            
            for item in o_results:
                real_groups[tuple(item[1:-1])].append([item[0], item[-1]])
                query_groups[tuple(item[1:-1])].append([item[0], item[-1]])
                results_id.append(item[0])
            sum_list = [sum([kk[-1] for kk in real_groups[item]])  for item in real_groups.keys()]
            n_list = [len(real_groups[item]) for item in real_groups.keys()]
            for j, item in enumerate(real_groups.keys()):
                item_groups = query_groups[item]
                item_groups = sorted(item_groups, key = lambda x: x[-1])
                s_values = [kk[-1] for kk in item_groups]
                results_groups = real_groups[item]
                results_groups = sorted(results_groups, key = lambda x: x[-1])
                sum_value = float(sum_list[j])
                n = n_list[j]
                for i in range(n):
                    effective_id_list = []
                    a_i = max(x_l, (sum_value - (n - i - 1) * x_h)/(i + 1))
                    b_i = min(x_h, (sum_value - (i) * x_l)/(n - i))
                    # count the number of values in the range
                    aID = results_groups[i][0]
                    cnt, left_idx, right_idx = count_and_idx(s_values, a_i, b_i)
                    for k in range(left_idx, right_idx):
                        effective_id_list.append(item_groups[k][0])
                    # for tmp_item in item_groups:
                    #     if(a_i <= tmp_item[-1] and tmp_item[-1] <= b_i):
                    #         effecitve_states[item].append(tmp_item[0])
                    his = price_history[aID]
                    his = his & set(effective_id_list)
                    
                    tmp_price = len(price_history[aID]) - len(his)
                    price += tmp_price/(support_size - 1) * tuple_price
                    price_history[aID] = his
                    updated_id_list.append(aID)
            for aID in price_history.keys():
                if(aID not in updated_id_list):
                    his = price_history[aID]
                    his = his - set(results_id)
                    tmp_price = len(price_history[aID]) - len(his)
                    price += tmp_price/(support_size - 1) * tuple_price
                    price_history[aID] = his
            return price

        if("group by" not in sql and self.support_suffix != ""):
            # group by the results based on the selected attributes
            price = 0
            query_groups = defaultdict(list)
            s_results = s_results + o_results
            results_id = [item[0] for item in s_results]
            updated_id_list = []
            sum_value =  sum([item[-1] for item in o_results])
            n = len(o_results)

            s_results = sorted(s_results, key = lambda x: x[-1])
            o_results = sorted(o_results, key = lambda x: x[-1])
            s_values = [item[-1] for item in s_results]
            for i in range(n):
                effective_id_list = []
                a_i = max(x_l, (sum_value - (n - i - 1) * x_h)/(i + 1))
                b_i = min(x_h, (sum_value - (i) * x_l)/(n - i))
                # count the number of values in the range
                aID = o_results[i][0]
                cnt, left_idx, right_idx = count_and_idx(s_values, a_i, b_i)
                for k in range(left_idx, right_idx):
                    effective_id_list.append(s_results[k][0])
                
                # for tmp_item in item_groups:
                #     if(a_i <= tmp_item[-1] and tmp_item[-1] <= b_i):
                #         effecitve_states[item].append(tmp_item[0])
                his = price_history[aID]
                his = his & set(effective_id_list)
                tmp_price = len(price_history[aID]) - len(his)
                price += tmp_price/(support_size - 1) * tuple_price
                price_history[aID] = his
                updated_id_list.append(aID)
            for aID in price_history.keys():
                if(aID not in updated_id_list):
                    his = price_history[aID]
                    his = his - set(results_id)
                    tmp_price = len(price_history[aID]) - len(his)
                    price += tmp_price/(support_size - 1) * tuple_price
                    price_history[aID] = his
            return price

        if("group by" not in sql and self.support_suffix == ""):
            # group by the results based on the selected attributes
            price = 0
            query_groups = defaultdict(list)
            results_id = [item[0] for item in o_results]
            updated_id_list = []
            sum_value =  sum([item[-1] for item in o_results])
            n = len(o_results)
            o_results = sorted(o_results, key = lambda x: x[-1])
            s_values = [item[-1] for item in o_results]
            for i in range(n):
                effective_id_list = []
                a_i = max(x_l, (sum_value - (n - i - 1) * x_h)/(i + 1))
                b_i = min(x_h, (sum_value - (i) * x_l)/(n - i))
                # count the number of values in the range
                aID = o_results[i][0]
                cnt, left_idx, right_idx = count_and_idx(s_values, a_i, b_i)
                # print(a_i, b_i, cnt)
                # print(f"select count(*) from country where Population >= {a_i} and Population <= {b_i}")
                for k in range(left_idx, right_idx):
                    effective_id_list.append(o_results[k][0])
                his = price_history[aID]
                his = his & set(effective_id_list)
                tmp_price = len(price_history[aID]) - len(his)
                price += tmp_price/(support_size - 1) * tuple_price
                price_history[aID] = his
                updated_id_list.append(aID)
            for aID in price_history.keys():
                if(aID not in updated_id_list):
                    his = price_history[aID]
                    his = his - set(results_id)
                    tmp_price = len(price_history[aID]) - len(his)
                    price += tmp_price/(support_size - 1) * tuple_price
                    price_history[aID] = his
            return price

    def __price_avg_query_no_history__(self, sql):
        md = QueryMetaData(sql)
        if(len(md.tables) != 1):
            print("ARIA cannot directly price the average queries over joined tables")
            print("Please do the join first and compute the aggregations by yourself")
            return -1
        table = md.tables[0]
        price = 0
        support_size = self.support_size_list[table]
        table_size = self.table_size_list[table]
        tuple_price = self.tuple_price_list[table]
        # construct the new query without avg and group by clauses
        new_sql = sql.split("group")[0]
        new_sql = new_sql.replace("avg(", "")
        new_sql = new_sql.replace("sum(", "")
        # add the aID column into the sql query
        new_sql = new_sql.replace("select ", "select aID, ")
        new_sql = new_sql.replace(")", "")
        md = QueryMetaData(new_sql)
        # get the average column
        # print(new_sql, md.projections)
        att = md.projections[-1]
        x_l = self.field_domin[table+ "." + att][0]
        x_h = self.field_domin[table+ "." + att][1]
        # obtain the query answer 
        o_results = select(new_sql, database= self.db)
        # get the average value and group by the results
        if(self.support_suffix != ""):
            # new_sql = new_sql.replace(table, table + self.support_suffix)
            new_sql = new_sql.replace(table + " ", table + self.support_suffix + " ")
            new_sql = new_sql.replace(table + ",", table + self.support_suffix + ",")
            new_sql = new_sql.replace(table + ".", table + self.support_suffix + ".")
            s_results = select(new_sql, database= self.db)
        else:
            s_results = []
        # get the average value
        n = len(o_results)
        if(n == 0):
            # empty results
            price = 0
            tmp_price = len(s_results) + n
            price += tmp_price/(support_size - 1) * tuple_price * table_size
            return price
        # self.support_suffix = "1"
        if("group by" in sql and self.support_suffix == ""):
            # group by the results based on the selected attributes
            real_groups = defaultdict(list)
            results_id = [item[0] for item in o_results]
            for item in o_results:
                real_groups[tuple(item[1:-1])].append([item[0], item[-1]])
            price = 0
            sum_list = [sum([kk[-1] for kk in real_groups[item]])  for item in real_groups.keys()]
            n_list = [len(real_groups[item]) for item in real_groups.keys()]
            for j, item in enumerate(real_groups.keys()):
                item_groups = real_groups[item]
                item_groups = sorted(item_groups, key = lambda x: x[-1])
                s_values = [kk[-1] for kk in item_groups]
                results_groups = real_groups[item]
                sum_value = float(sum_list[j])
                n = n_list[j]
                for i in range(n):
                    a_i = max(x_l, (sum_value - (n - i - 1) * x_h)/(i + 1))
                    b_i = min(x_h, (sum_value - (i) * x_l)/(n - i))
                    cnt, left_idx, right_idx = count_and_idx(s_values, a_i, b_i)
                    tmp_price = support_size - cnt
                    
                    price += tmp_price/(support_size - 1) * tuple_price
                    # print(tmp_price, len(effective_id_list))
            tmp_price = len(o_results)   
            price += tmp_price/(support_size - 1) * tuple_price * (table_size - n)     
            return price
        if("group by" in sql and self.support_suffix != ""):
            # group by the results based on the selected attributes
            query_groups = defaultdict(list)
            results_id = []
            price = 0
            for item in s_results:
                query_groups[tuple(item[1:-1])].append([item[0], item[-1]])
                results_id.append(item[0])
            real_groups = defaultdict(list)
            for item in o_results:
                real_groups[tuple(item[1:-1])].append([item[0], item[-1]])
                query_groups[tuple(item[1:-1])].append([item[0], item[-1]])
                results_id.append(item[0])
            sum_list = [sum([kk[-1] for kk in real_groups[item]])  for item in real_groups.keys()]
            n_list = [len(real_groups[item]) for item in real_groups.keys()]
            for j, item in enumerate(real_groups.keys()):
                item_groups = query_groups[item]
                item_groups = sorted(item_groups, key = lambda x: x[-1])
                s_values = [kk[-1] for kk in item_groups]
                results_groups = real_groups[item]
                results_groups = sorted(results_groups, key = lambda x: x[-1])
                sum_value = float(sum_list[j])
                n = n_list[j]
                for i in range(n):
                    a_i = max(x_l, (sum_value - (n - i - 1) * x_h)/(i + 1))
                    b_i = min(x_h, (sum_value - (i) * x_l)/(n - i))
                    cnt, left_idx, right_idx = count_and_idx(s_values, a_i, b_i)
                    tmp_price = support_size - cnt
                    
                    price += tmp_price/(support_size - 1) * tuple_price
            tmp_price = len(s_results)  + len(o_results)
            price += tmp_price/(support_size - 1) * tuple_price * (table_size - n)        
            return price

        if("group by" not in sql and self.support_suffix != ""):
            # group by the results based on the selected attributes
            price = 0
            query_groups = defaultdict(list)
            s_results = s_results + o_results
            results_id = [item[0] for item in s_results]
            sum_value =  sum([item[-1] for item in o_results])
            n = len(o_results)
            s_results = sorted(s_results, key = lambda x: x[-1])
            o_results = sorted(o_results, key = lambda x: x[-1])
            s_values = [item[-1] for item in s_results]
            for i in range(n):
                a_i = max(x_l, (sum_value - (n - i - 1) * x_h)/(i + 1))
                b_i = min(x_h, (sum_value - (i) * x_l)/(n - i))
                cnt, left_idx, right_idx = count_and_idx(s_values, a_i, b_i)
                tmp_price = support_size - cnt
                price += tmp_price/(support_size - 1) * tuple_price
            tmp_price = len(s_results)   
            price += tmp_price/(support_size - 1) * tuple_price * (table_size - n)    
            return price

        if("group by" not in sql and self.support_suffix == ""):
            # group by the results based on the selected attributes
            price = 0
            query_groups = defaultdict(list)
            results_id = [item[0] for item in o_results]
            sum_value =  sum([item[-1] for item in o_results])
            n = len(o_results)
            o_results = sorted(o_results, key = lambda x: x[-1])
            s_values = [item[-1] for item in o_results]
            for i in range(n):
                a_i = max(x_l, (sum_value - (n - i - 1) * x_h)/(i + 1))
                b_i = min(x_h, (sum_value - (i) * x_l)/(n - i))
                # count the number of values in the range
                cnt, left_idx, right_idx = count_and_idx(s_values, a_i, b_i)
                tmp_price = support_size - cnt
                price += tmp_price/(support_size - 1) * tuple_price
            tmp_price = len(o_results)   
            price += tmp_price/(support_size - 1) * tuple_price * (table_size - n)   
            return price
    def price_SQL_query(self, sql):
        sql = sql.split(";")[0]
        sql  = sql + " "
        if(self.history_aware):
            if("distinct" in sql):
                if("*" in sql):
                    price = self.__price_distinct_star_history_aware__(sql)
                else:
                    price = self.__price_distinct_query_history_aware__(sql)
            elif("limit" in sql):
                if("*" in sql):
                    price = self.__price_limit_star_history_aware__(sql)
                else:
                    price = self.__price_limit_query_history_aware__(sql)
            elif("count(" in sql):
                price = self.__price_cnt_query_history_aware__(sql)
            elif("max(" in sql or "min(" in sql):
                price = self.__price_extreme_query_history_aware__(sql)
            elif("avg(" in sql or "sum(" in sql):
                price = self.__price_avg_query_history_aware__(sql)
            elif("*" in sql):
                price = self.__price_SPJ_star_history_aware__(sql)
            else:
                price = self.__price_SPJ_query_history_aware__(sql)
        else:
            if("distinct" in sql):
                if("*" in sql):
                    price = self.__price_distinct_star_no_history__(sql)
                else:
                    price = self.__price_distinct_query_no_history__(sql)
            elif("limit" in sql):
                if("*" in sql):
                    price = self.__price_limit_star_no_history__(sql)
                else:
                    price = self.__price_limit_query_no_history__(sql)
            elif("count(" in sql):
                price = self.__price_cnt_query_no_history__(sql)
            elif("max(" in sql or "min(" in sql):
                price = self.__price_extreme_query_no_history__(sql)
            elif("avg(" in sql or "sum(" in sql):
                price = self.__price_avg_query_no_history__(sql)
            elif("*" in sql):
                price = self.__price_SPJ_star_no_history__(sql)
            else:
                price = self.__price_SPJ_query_no_history__(sql)
        return price

