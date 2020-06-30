import psycopg2
from time import time
import datetime
import pyodbc
import sqlanydb
import cx_Oracle  




def postgres():
    connection = psycopg2.connect(user = "postgres",
                                password = "zeeshan",
                                host = "127.0.0.1",
                                port = "5432",
                                database = "new_tpcds")
    cursor = connection.cursor()

    ### Generating Materialized and non-materialized views
    with open("Desktop/FYP/Postgres_Queries/mv_crv.txt","r") as file:
      mv_crv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/nv_crv.txt","r") as file:
      nv_crv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/mv_csv.txt","r") as file:
      mv_csv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/nv_csv.txt","r") as file:
      nv_csv = file.read().replace('\n','')

    with open("Desktop/FYP/Postgres_Queries/mv_itemv.txt","r") as file:
      mv_itemv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/nv_itemv.txt","r") as file:
      nv_itemv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/mv_promv.txt","r") as file:
      mv_promv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/nv_promv.txt","r") as file:
      nv_promv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/mv_ccv.txt","r") as file:
      mv_ccv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/nv_ccv.txt","r") as file:
      nv_ccv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/mv_srv.txt","r") as file:
      mv_srv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/nv_srv.txt","r") as file:
      nv_srv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/mv_ssv.txt","r") as file:
      mv_ssv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/nv_ssv.txt","r") as file:
      nv_ssv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/mv_storv.txt","r") as file:
      mv_storv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/nv_storv.txt","r") as file:
      nv_storv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/mv_websv.txt","r") as file:
      mv_websv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/nv_websv.txt","r") as file:
      nv_websv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/mv_webv.txt","r") as file:
      mv_webv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/nv_webv.txt","r") as file:
      nv_webv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/mv_wrhsv.txt","r") as file:
      mv_wrhsv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/nv_wrhsv.txt","r") as file:
      nv_wrhsv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/mv_wrv.txt","r") as file:
      mv_wrv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/nv_wrv.txt","r") as file:
      nv_wrv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/mv_wsv.txt","r") as file:
      mv_wsv = file.read().replace('\n','')
    
    with open("Desktop/FYP/Postgres_Queries/nv_wsv.txt","r") as file:
      nv_wsv = file.read().replace('\n','')
    
    
    

    cursor.execute(mv_crv)
    cursor.execute(nv_crv)
    cursor.execute(mv_csv)
    cursor.execute(nv_csv)
    cursor.execute(mv_itemv)
    cursor.execute(nv_itemv)
    cursor.execute(mv_promv)
    cursor.execute(nv_promv)
    cursor.execute(mv_ccv)
    cursor.execute(nv_ccv)
    cursor.execute(mv_srv)
    cursor.execute(nv_srv)
    cursor.execute(mv_ssv)
    cursor.execute(nv_ssv)
    cursor.execute(mv_storv)
    cursor.execute(nv_storv)
    cursor.execute(mv_websv)
    cursor.execute(nv_websv)
    cursor.execute(mv_webv)
    cursor.execute(nv_webv)
    cursor.execute(mv_wrhsv)
    cursor.execute(nv_wrhsv)
    cursor.execute(mv_wrv)
    cursor.execute(nv_wrv)
    cursor.execute(mv_wsv)
    cursor.execute(nv_wsv)

    #Storing sql queries
    with open("Desktop/FYP/Postgres_Queries/mv_csv.txt","r") as file_in:
          queries = []
    for line in file_in:
        queries.append(line)
    
    with open("Desktop/FYP/results.txt","r") as file_in:
        results = []
        for line in file_in:
            results.append(line)
    
    
    time_sql = []
    #calculating response time of all queries
    for m in range(26):
        for c in range(9):
            a = datetime.datetime.now()
            cursor.execute(queries[m])
            b = datetime.datetime.now()
            delta = b-a
            # print(delta)
            print (queries[m],int(delta.total_seconds()*10000))
            time_sql.append(int(delta.total_seconds()*10000))
        print("***********")
    print(time_sql)
    i = 4
    while i < len(time_sql):
        for d in range(26):
            print(results[d],time_sql[i])
            i+=9


def SQLServer():
    connection = pyodbc.connect('Driver={SQL Server};'
                      'Server=MUHAMMADZEE7B6B;'
                      'Database=ds;'
                      'Trusted_Connection=yes;')

    cursor = connection.cursor()
    with open("Desktop/FYP/SQL_Server_Queries/mv_crv.txt","r") as file:
        mv_crv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/nv_crv.txt","r") as file:
        nv_crv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/mv_csv.txt","r") as file:
        mv_csv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/nv_csv.txt","r") as file:
        nv_csv = file.read().replace('\n','')

    with open("Desktop/FYP/SQL_Server_Queries/mv_itemv.txt","r") as file:
        mv_itemv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/nv_itemv.txt","r") as file:
        nv_itemv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/mv_promv.txt","r") as file:
        mv_promv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/nv_promv.txt","r") as file:
        nv_promv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/mv_ccv.txt","r") as file:
        mv_ccv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/nv_ccv.txt","r") as file:
        nv_ccv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/mv_srv.txt","r") as file:
        mv_srv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/nv_srv.txt","r") as file:
        nv_srv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/mv_ssv.txt","r") as file:
        mv_ssv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/nv_ssv.txt","r") as file:
        nv_ssv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/mv_storv.txt","r") as file:
        mv_storv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/nv_storv.txt","r") as file:
        nv_storv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/mv_websv.txt","r") as file:
        mv_websv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/nv_websv.txt","r") as file:
        nv_websv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/mv_webv.txt","r") as file:
        mv_webv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/nv_webv.txt","r") as file:
        nv_webv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/mv_wrhsv.txt","r") as file:
        mv_wrhsv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/nv_wrhsv.txt","r") as file:
        nv_wrhsv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/mv_wrv.txt","r") as file:
        mv_wrv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/nv_wrv.txt","r") as file:
        nv_wrv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/mv_wsv.txt","r") as file:
        mv_wsv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Server_Queries/nv_wsv.txt","r") as file:
        nv_wsv = file.read().replace('\n','')

    cursor.execute(mv_crv)
    cursor.execute(nv_crv)
    cursor.execute(mv_csv)
    cursor.execute(nv_csv)
    cursor.execute(mv_itemv)
    cursor.execute(nv_itemv)
    cursor.execute(mv_promv)
    cursor.execute(nv_promv)
    cursor.execute(mv_ccv)
    cursor.execute(nv_ccv)
    cursor.execute(mv_srv)
    cursor.execute(nv_srv)
    cursor.execute(mv_ssv)
    cursor.execute(nv_ssv)
    cursor.execute(mv_storv)
    cursor.execute(nv_storv)
    cursor.execute(mv_websv)
    cursor.execute(nv_websv)
    cursor.execute(mv_webv)
    cursor.execute(nv_webv)
    cursor.execute(mv_wrhsv)
    cursor.execute(nv_wrhsv)
    cursor.execute(mv_wrv)
    cursor.execute(nv_wrv)
    cursor.execute(mv_wsv)
    cursor.execute(nv_wsv)
    #Storing sql queries
    with open("Desktop/FYP/sql_statements.txt","r") as file_in:
        queries = []
        for line in file_in:
            queries.append(line)
    with open("Desktop/FYP/results.txt","r") as file_in:
        results = []
        for line in file_in:
            results.append(line)
    
    
    time_sql = []
    #calculating response time of all queries
    for m in range(26):
        for c in range(9):
            a = datetime.datetime.now()
            cursor.execute(queries[m])
            b = datetime.datetime.now()
            delta = b-a
            # print(delta)
            print (queries[m],int(delta.total_seconds()*10000))
            time_sql.append(int(delta.total_seconds()*10000))
        print("***********")
    print(time_sql)
    i = 4
    while i < len(time_sql):
        for d in range(26):
            print(results[d],time_sql[i])
            i+=9


def SQLAnywhere():



    connection = sqlanydb.connect(uid='zeeshan', pwd='zeeshan', dbn='tpcds') 
    cursor = connection.cursor()

    with open("Desktop/FYP/SQL_Anywhere_Queries/mv_crv.txt","r") as file:
        mv_crv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/nv_crv.txt","r") as file:
        nv_crv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/mv_csv.txt","r") as file:
        mv_csv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/nv_csv.txt","r") as file:
        nv_csv = file.read().replace('\n','')

    with open("Desktop/FYP/SQL_Anywhere_Queries/mv_itemv.txt","r") as file:
        mv_itemv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/nv_itemv.txt","r") as file:
        nv_itemv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/mv_promv.txt","r") as file:
        mv_promv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/nv_promv.txt","r") as file:
        nv_promv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/mv_ccv.txt","r") as file:
        mv_ccv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/nv_ccv.txt","r") as file:
        nv_ccv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/mv_srv.txt","r") as file:
        mv_srv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/nv_srv.txt","r") as file:
        nv_srv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/mv_ssv.txt","r") as file:
        mv_ssv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/nv_ssv.txt","r") as file:
        nv_ssv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/mv_storv.txt","r") as file:
        mv_storv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/nv_storv.txt","r") as file:
        nv_storv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/mv_websv.txt","r") as file:
        mv_websv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/nv_websv.txt","r") as file:
        nv_websv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/mv_webv.txt","r") as file:
        mv_webv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/nv_webv.txt","r") as file:
        nv_webv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/mv_wrhsv.txt","r") as file:
        mv_wrhsv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/nv_wrhsv.txt","r") as file:
        nv_wrhsv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/mv_wrv.txt","r") as file:
        mv_wrv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/nv_wrv.txt","r") as file:
        nv_wrv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/mv_wsv.txt","r") as file:
        mv_wsv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Anywhere_Queries/nv_wsv.txt","r") as file:
        nv_wsv = file.read().replace('\n','')

    cursor.execute(mv_crv)
    cursor.execute(nv_crv)
    cursor.execute(mv_csv)
    cursor.execute(nv_csv)
    cursor.execute(mv_itemv)
    cursor.execute(nv_itemv)
    cursor.execute(mv_promv)
    cursor.execute(nv_promv)
    cursor.execute(mv_ccv)
    cursor.execute(nv_ccv)
    cursor.execute(mv_srv)
    cursor.execute(nv_srv)
    cursor.execute(mv_ssv)
    cursor.execute(nv_ssv)
    cursor.execute(mv_storv)
    cursor.execute(nv_storv)
    cursor.execute(mv_websv)
    cursor.execute(nv_websv)
    cursor.execute(mv_webv)
    cursor.execute(nv_webv)
    cursor.execute(mv_wrhsv)
    cursor.execute(nv_wrhsv)
    cursor.execute(mv_wrv)
    cursor.execute(nv_wrv)
    cursor.execute(mv_wsv)
    cursor.execute(nv_wsv)

    #Storing sql queries
    with open("Desktop/FYP/sql_statements.txt","r") as file_in:
        queries = []
        for line in file_in:
            queries.append(line)

    with open("Desktop/FYP/results.txt","r") as file_in:
        results = []
        for line in file_in:
            results.append(line)
    
    
    
    time_sql = []
    #calculating response time of all queries
    for m in range(26):
        for c in range(9):
            a = datetime.datetime.now()
            cursor.execute(queries[m])
            b = datetime.datetime.now()
            delta = b-a
            # print(delta)
            print (queries[m],int(delta.total_seconds()*10000))
            time_sql.append(int(delta.total_seconds()*10000))
        print("***********")
    print(time_sql)
    i = 4
    while i < len(time_sql):
        for d in range(26):
            print(results[d],time_sql[i])
            i+=9

def OracleSQL():
    connection = cx_Oracle.connect('zeeshan/tpcds@localhost') 

    cursor = connection.cursor()




    with open("Desktop/FYP/SQL_Oracle_Queries/mv_crv.txt","r") as file:
        mv_crv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/nv_crv.txt","r") as file:
        nv_crv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/mv_csv.txt","r") as file:
        mv_csv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/nv_csv.txt","r") as file:
        nv_csv = file.read().replace('\n','')

    with open("Desktop/FYP/SQL_Oracle_Queries/mv_itemv.txt","r") as file:
        mv_itemv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/nv_itemv.txt","r") as file:
        nv_itemv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/mv_promv.txt","r") as file:
        mv_promv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/nv_promv.txt","r") as file:
        nv_promv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/mv_ccv.txt","r") as file:
        mv_ccv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/nv_ccv.txt","r") as file:
        nv_ccv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/mv_srv.txt","r") as file:
        mv_srv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/nv_srv.txt","r") as file:
        nv_srv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/mv_ssv.txt","r") as file:
        mv_ssv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/nv_ssv.txt","r") as file:
        nv_ssv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/mv_storv.txt","r") as file:
        mv_storv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/nv_storv.txt","r") as file:
        nv_storv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/mv_websv.txt","r") as file:
        mv_websv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/nv_websv.txt","r") as file:
        nv_websv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/mv_webv.txt","r") as file:
        mv_webv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/nv_webv.txt","r") as file:
        nv_webv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/mv_wrhsv.txt","r") as file:
        mv_wrhsv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/nv_wrhsv.txt","r") as file:
        nv_wrhsv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/mv_wrv.txt","r") as file:
        mv_wrv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/nv_wrv.txt","r") as file:
        nv_wrv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/mv_wsv.txt","r") as file:
        mv_wsv = file.read().replace('\n','')
    
    with open("Desktop/FYP/SQL_Oracle_Queries/nv_wsv.txt","r") as file:
        nv_wsv = file.read().replace('\n','')

    cursor.execute(mv_crv)
    cursor.execute(nv_crv)
    cursor.execute(mv_csv)
    cursor.execute(nv_csv)
    cursor.execute(mv_itemv)
    cursor.execute(nv_itemv)
    cursor.execute(mv_promv)
    cursor.execute(nv_promv)
    cursor.execute(mv_ccv)
    cursor.execute(nv_ccv)
    cursor.execute(mv_srv)
    cursor.execute(nv_srv)
    cursor.execute(mv_ssv)
    cursor.execute(nv_ssv)
    cursor.execute(mv_storv)
    cursor.execute(nv_storv)
    cursor.execute(mv_websv)
    cursor.execute(nv_websv)
    cursor.execute(mv_webv)
    cursor.execute(nv_webv)
    cursor.execute(mv_wrhsv)
    cursor.execute(nv_wrhsv)
    cursor.execute(mv_wrv)
    cursor.execute(nv_wrv)
    cursor.execute(mv_wsv)
    cursor.execute(nv_wsv)

    #Storing sql queries
    with open("Desktop/FYP/sql_statements.txt","r") as file_in:
        queries = []
        for line in file_in:
            queries.append(line)
    
    with open("Desktop/FYP/results.txt","r") as file_in:
        results = []
        for line in file_in:
            results.append(line)
    
    
    time_sql = []
    #calculating response time of all queries
    for m in range(26):
        for c in range(9):
            a = datetime.datetime.now()
            cursor.execute(queries[m])
            b = datetime.datetime.now()
            delta = b-a
            # print(delta)
            print (queries[m],int(delta.total_seconds()*10000))
            time_sql.append(int(delta.total_seconds()*10000))
        print("***********")
    print(time_sql)
    i = 4
    while i < len(time_sql):
        for d in range(26):
            print(results[d],time_sql[i])
            i+=9



print("1: Postgres")
print("2: SQL Server")
print("3: SQL Anywhere")
print("4: Oracle sql")
print("Please enter which db would you like to use?")
db_type = int(input())

if db_type == 1:
    postgres()
if db_type == 2:
    SQLServer()
if db_type == 3:
    SQLAnywhere()
if db_type == 4:
    OracleSQL()






