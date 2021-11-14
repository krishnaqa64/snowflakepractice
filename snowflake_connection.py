import pandas as pd
import numpy as np
import os
import snowflake.connector as sf
from snowflake.connector import DictCursor
from config import username,password ,account ,database ,schema ,warehouse1 ,warehouse2



#
#
# # def execute_query(query):
# #     connection = sf.connect(user=username, password=password, account=account)
# #     cursor=connection.cursor()
# #     cursor.execute(query)
# #     #cursor.close()
# #     for c in cursor:
# #         return c
#
#
# def execute_query(query,warehouse):
#     connection = sf.connect(user=username, password=password, account=account,database=database,schema=schema)
#     cursor=connection.cursor()
#     print(warehouse)
#     print(database)
#     print(query)
#     cursor.execute('use {}'.format(warehouse))
#     #cursor.execute('use {}'.format(database))
#     #cursor.execute(query)
#
#     #cursor.close()
#     #for c in cursor:
#         #return c
#
#
#
# #sql = 'use WAREHOUSE {}'.format(warehouse1)
# sql = 'select * from PUBLIC.TEMP_TABLE'
# print(execute_query(sql,warehouse1))
# # sql='use {}'.format(database)
# # print(execute_query(sql))
# # sql = 'select * from PUBLIC.TEMP_TABLE'
# # print(execute_query(sql))
#
# #
# # try:
# #     sql='use {}'.format(database)
# #     execute_query(conn1,sql)
# #     sql = 'use WAREHOUSE {}'.format(warehouse1)
# #     execute_query(conn1, sql)
# #
# #     # sql = 'alter WAREHOUSE {} resume'.format(warehouse1)
# #     # execute_query(conn1, sql)
# #
# #     # sql = "insert into PUBLIC.TEMP_TABLE values ('5','Donald','New')"
# #     # cursor = conn1.cursor(DictCursor)
# #     # cursor.execute(sql)
# #     # for c in cursor:
# #     #     print(c)
# #     # print("hello")
# #     # cursor.close
# #
# #     print("\n")
# #
# #     sql = 'select * from PUBLIC.TEMP_TABLE'
# #     cursor=conn1.cursor(DictCursor)
# #     cursor.execute(sql)
# #     for c in cursor:
# #         print(c)
# #     cursor.close



conn1=sf.connect(user=username,password=password,account=account)
def execute_query(connection,query):
    cursor=connection.cursor()
    cursor.execute(query)
    cursor.close()

try:
    sql='use {}'.format(database)
    execute_query(conn1,sql)
    sql = 'use WAREHOUSE {}'.format(warehouse1)
    execute_query(conn1, sql)

    # sql = 'alter WAREHOUSE {} resume'.format(warehouse1)
    # execute_query(conn1, sql)

    # sql = "insert into PUBLIC.TEMP_TABLE values ('5','Donald','New')"
    # cursor = conn1.cursor(DictCursor)
    # cursor.execute(sql)
    # for c in cursor:
    #     print(c)
    # print("hello")
    # cursor.close

    print("\n")

    sql = 'select * from PUBLIC.TEMP_TABLE'
    cursor=conn1.cursor(DictCursor)
    cursor.execute(sql)
    for c in cursor:
        print(c)
    cursor.close


except Exception as e:
    print(e)