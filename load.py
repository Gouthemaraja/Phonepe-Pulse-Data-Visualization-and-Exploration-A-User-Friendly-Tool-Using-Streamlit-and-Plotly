import pandas as pd 
import extraction as ex
import mysql.connector as sql
from mysql.connector.errors import Error

def connection_open():

    try:
        connection,mycursor = mysql('root',"root","localhost","phonepe_pulse")
        return connection,mycursor
    except:
        e = mysql('roo',"root","localhost","phonepe_pulse")
        print("Error is ",e)


# Create a MySQL connection

def mysql(username,password,host,databasename):
    db_username = username
    db_password =password
    db_host = host
    db_name = databasename
    try:
        connection = sql.connect(
            host=db_host,
            database=db_name,
            user=db_username,
            password=db_password
        )
        mycursor = connection.cursor(buffered=True)
        print("connected")
        return connection,mycursor
    except Error as e:
        return e

# closing the connections

def closing_connection(connection,mycursor):
    mycursor = mycursor 
    connection = connection
    mycursor.close()
    connection.close()
    if connection.is_closed():
        print("sql connection is closed")
    else:
        print("not closed")


def load_year_quater():
    dic = ex.dic
    df = ex.agg_trans_india(dic)
    year = df["qyear"].unique()
    # print(year)
    quater = df["quater"].unique()
    # print(quater)
    try:
        for x in year:
            tup = ()
            tup = tup + (int(x),)
            connection,cursor = connection_open()
            query = f'INSERT INTO qyear values (%s)'
            try:
                cursor.execute(query,tup)
                connection.commit()
            except Error as e:
                print(e)
            closing_connection(connection,cursor)
    except Error as e:
        print(e)
    try:
        for x in quater:
            tup = ()
            tup = tup + ((x),)
            connection,cursor = connection_open()
            query = f'INSERT INTO quater values (%s)'
            try:
                cursor.execute(query,tup)
                connection.commit()
            except Error as e:
                print(e)  
            closing_connection(connection,cursor)
    except Error as e:
        print(e)
        



def load_aggre_trans_india():
    dic = ex.dic
    df = ex.agg_trans_india(dic)
    values = [tuple(row) for row in df.values]
    # # # print(values)
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(df.columns))
    # # print(placeholders)
    load_query = f'INSERT INTO aggre_trans_india VALUES ({placeholders})'
    try:
        cursor.executemany(load_query,values)
        connection.commit()
    except Error as e:
        print(e)
    closing_connection(connection,cursor)

def load_aggre_trans_india_states():
    dic_state = ex.dic_state
    df = ex.agg_trans_india_state(dic_state)
    values = [tuple(row) for row in df.values]
    # # print(values)
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(df.columns))
    # print(placeholders)
    load_query = f'INSERT INTO aggre_trans_india_states VALUES ({placeholders})'
    try:
        cursor.executemany(load_query,values)
        connection.commit()
    except Error as e:
        print(e)
    closing_connection(connection,cursor)

def load_aggre_users_india():
    dic_user_india = ex.dic_user_india
    df = ex.agg_user_india(dic_user_india)
    values = [tuple(row) for row in df.values]
    # # print(values)
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(df.columns))
    # print(placeholders)
    load_query = f'INSERT INTO aggre_users_india VALUES ({placeholders})'
    try:
        cursor.executemany(load_query,values)
        connection.commit()
    except Error as e:
        print(e)
    closing_connection(connection,cursor)

def load_aggre_user_india_state():
    dic_user_india_state = ex.dic_user_india_state
    df = ex.agg_user_india_state(dic_user_india_state)
    values = [tuple(row) for row in df.values]
    # # print(values)
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(df.columns))
    # print(placeholders)
    load_query = f'INSERT INTO aggre_user_india_states VALUES ({placeholders})'
    try:
        cursor.executemany(load_query,values)
        connection.commit()
    except Error as e:
        print(e)
    closing_connection(connection,cursor)

def load_map_trans_india():
    dic_map_trans_india = ex.dic_map_trans_india
    df = ex.map_trans_india(dic_map_trans_india)
    values = [tuple(row) for row in df.values]
    # # print(values)
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(df.columns))
    # print(placeholders)
    load_query = f'INSERT INTO map_trans_india VALUES ({placeholders})'
    try:
        cursor.executemany(load_query,values)
        connection.commit()
    except Error as e:
        print(e)
    closing_connection(connection,cursor)


def load_map_trans_india_state():
    dic_map_trans_india_state = ex.dic_map_trans_india_state
    df = ex.map_trans_india_states(dic_map_trans_india_state)
    values = [tuple(row) for row in df.values]
    # # print(values)
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(df.columns))
    # print(placeholders)
    load_query = f'INSERT INTO map_trans_india_state VALUES ({placeholders})'
    try:
        cursor.executemany(load_query,values)
        connection.commit()
    except Error as e:
        print(e)
    closing_connection(connection,cursor)

def load_map_user_india():
    dic_map_user_india = ex.dic_map_user_india
    df = ex.map_user_india(dic_map_user_india)
    values = [tuple(row) for row in df.values]
    # # print(values)
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(df.columns))
    # print(placeholders)
    load_query = f'INSERT INTO map_user_india VALUES ({placeholders})'
    try:
        cursor.executemany(load_query,values)
        connection.commit()
    except Error as e:
        print(e)
    closing_connection(connection,cursor)

def load_map_user_india_state():
    dic_map_user_india_state = ex.dic_map_user_india_state
    df = ex.map_user_india_states(dic_map_user_india_state)
    values = [tuple(row) for row in df.values]
    # # print(values)
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(df.columns))
    # print(placeholders)
    load_query = f'INSERT INTO map_user_india_state VALUES ({placeholders})'
    try:
        cursor.executemany(load_query,values)
        connection.commit()
    except Error as e:
        print(e)
    closing_connection(connection,cursor)


def load_top_trans_india():
    dic_top_trans_india = ex.dic_top_trans_india
    dfs,dfd,dfpc= ex.top_trans_india(dic_top_trans_india)
    values1 = [tuple(row) for row in dfs.values]
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(dfs.columns))
    # print(placeholders)
    load_query = f'INSERT INTO top_trans_india_by_state VALUES ({placeholders})'
    try:
        cursor.executemany(load_query,values1)
        connection.commit()
    except Error as e:
        print(e)

    closing_connection(connection,cursor)
    values2 = [tuple(row) for row in dfd.values]
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(dfd.columns))
    # print(placeholders)
    load_query1 = f'INSERT INTO top_trans_india_by_district VALUES ({placeholders})'
    try:
        cursor.executemany(load_query1,values2)
        connection.commit()
    except Error as e:
        print(e)

    closing_connection(connection,cursor)
    values3 = [tuple(row) for row in dfpc.values]
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(dfpc.columns))
    # print(placeholders)
    load_query3 = f'INSERT INTO top_trans_india_by_pincode VALUES ({placeholders})'
    try:
        cursor.executemany(load_query3,values3)
        connection.commit()
    except Error as e:
        print(e)
    closing_connection(connection,cursor)

def load_top_trans_india_state():
    dic_top_trans_india_state = ex.dic_top_trans_india_state
    dfsd,dfspc = ex.top_trans_india_state(dic_top_trans_india_state)
    values1 = [tuple(row) for row in dfsd.values]
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(dfsd.columns))
    # print(placeholders)
    load_query = f'INSERT INTO top_trans_india_state_by_district VALUES ({placeholders})'
    try:
        cursor.executemany(load_query,values1)
        connection.commit()
    except Error as e:
        print(e)

    closing_connection(connection,cursor)
    
    values2 = [tuple(row) for row in dfspc.values]
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(dfspc.columns))
    # print(placeholders)
    #print(values2)
    load_query = f'INSERT INTO top_trans_india_state_by_pincode VALUES ({placeholders})'
    try:
        cursor.executemany(load_query,values2)
        connection.commit()
    except Error as e:
        print(e)

    closing_connection(connection,cursor)

def load_top_user_india():
    dic_top_user_india = ex.dic_top_user_india
    dfs,dfd,dfpc = ex.top_user_india(dic_top_user_india)
    values1 = [tuple(row) for row in dfs.values]
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(dfs.columns))
    # print(placeholders)
    load_query = f'INSERT INTO top_user_india_by_state VALUES ({placeholders})'
    try:
        cursor.executemany(load_query,values1)
        connection.commit()
    except Error as e:
        print(e)
    closing_connection(connection,cursor)

    values2 = [tuple(row) for row in dfd.values]
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(dfd.columns))
    # print(placeholders)
    load_query1 = f'INSERT INTO top_user_india_by_district VALUES ({placeholders})'
    try:
        cursor.executemany(load_query1,values2)
        connection.commit()
    except Error as e:
        print(e)

    closing_connection(connection,cursor)

    values3 = [tuple(row) for row in dfpc.values]
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(dfpc.columns))
    # print(placeholders)
    load_query3 = f'INSERT INTO top_user_india_by_pincode VALUES ({placeholders})'
    try:
        cursor.executemany(load_query3,values3)
        connection.commit()
    except Error as e:
        print(e)
    closing_connection(connection,cursor)


def load_top_user_by_state():
    dic_top_user_india_state = ex.dic_top_user_india_state
    dfsd,dfspc = ex.top_user_india_state(dic_top_user_india_state)
    values1 = [tuple(row) for row in dfsd.values]
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(dfsd.columns))
    # print(placeholders)
    load_query = f'INSERT INTO top_user_india_state_by_district VALUES ({placeholders})'
    try:
        cursor.executemany(load_query,values1)
        connection.commit()
    except Error as e:
        print(e)

    closing_connection(connection,cursor)

    values2 = [tuple(row) for row in dfspc.values]
    connection,cursor = connection_open()
    placeholders = ', '.join(['%s'] * len(dfspc.columns))
    # print(placeholders)
    #print(values2)
    load_query = f'INSERT INTO top_user_india_state_by_pincode VALUES ({placeholders})'
    try:
        cursor.executemany(load_query,values2)
        connection.commit()
    except Error as e:
        print(e)

    closing_connection(connection,cursor)

    


if __name__ == "__main__":
    load_year_quater()
    load_aggre_trans_india()
    
    load_aggre_trans_india_states()
    load_aggre_users_india()
    load_aggre_user_india_state()
    load_map_trans_india()
    load_map_trans_india_state()
    load_map_user_india()
    load_map_user_india_state()
    load_top_trans_india()
    load_top_trans_india_state()
    load_top_user_india()
    load_top_user_by_state()