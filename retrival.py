import mysql.connector as sql
from mysql.connector.errors import Error
import pandas as pd

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


def retrive_data():
    connection,mycursor = connection_open()
    query = """SELECT * FROM phonepe_pulse.aggre_trans_india_states"""
    mycursor.execute(query)

    # Fetch the results
    result = mycursor.fetchall()
    
    column_names = [desc[0] for desc in mycursor.description]
    
    df = pd.DataFrame(result,columns=column_names)

    closing_connection(connection,mycursor)
    return df


def retrive_trans_data(year,quater,state):
    year = int(year)
    quater = quater
    state = state
    # print(state)
    connection,mycursor = connection_open()
    query = f"SELECT state, payment_category,count as count_Cr, amount as amount_Cr FROM phonepe_pulse.aggre_trans_india_states where qyear = %s and quater = %s and state = %s"
    mycursor.execute(query,(year,quater,state))

    # Fetch the results
    result = mycursor.fetchall()
    
    column_names = [desc[0] for desc in mycursor.description]
    
    df = pd.DataFrame(result,columns=column_names)

    closing_connection(connection,mycursor)
    return df

def retrive_user_data(year,quater,state):
    year = int(year)
    quater = quater
    state = state
    connection,mycursor = connection_open()
    query = f"SELECT  state, registeredUsers,appOpens FROM  phonepe_pulse.aggre_user_india_states where qyear = %s and quater = %s and state = %s limit 1"
    mycursor.execute(query,(year,quater,state))

    # Fetch the results
    result = mycursor.fetchall()
    
    column_names = [desc[0] for desc in mycursor.description]
    
    df = pd.DataFrame(result,columns=column_names)

    closing_connection(connection,mycursor)
    return df

def retrive_brand_users(year,quater):
    year = int(year)
    quater = quater
    connection,mycursor = connection_open()
    query = f"SELECT  brand,count FROM phonepe_pulse.aggre_users_india where qyear = %s and quater = %s"
    mycursor.execute(query,(year ,quater))
    result = mycursor.fetchall()
    # print(result)
    column_names = [desc[0] for desc in mycursor.description]
    df = pd.DataFrame(result,columns=column_names)
    closing_connection(connection,mycursor)
    return df


def retrive_trans_dictrict(year,quater,state):
    year = int(year)
    quater = quater
    state = state
    
    connection,mycursor = connection_open()
    query = f"SELECT zone,count,amount FROM phonepe_pulse.map_trans_india_state where \
    qyear = %s and quater = %s and state = %s"
    mycursor.execute(query,(year ,quater,state))
    result = mycursor.fetchall()
    # print(result)
    column_names = [desc[0] for desc in mycursor.description]
    df = pd.DataFrame(result,columns=column_names)
    closing_connection(connection,mycursor)
    return df

def retrive_user_dictrict(year,quater,state):
    year = int(year)
    quater = quater
    state = state
    connection,mycursor = connection_open()
    query = f"SELECT zone,registeredUsers,appOpens FROM phonepe_pulse.map_user_india_state\
    where qyear = %s and quater = %s and state = %s"
    mycursor.execute(query,(year ,quater,state))
    result = mycursor.fetchall()
    # print(result)
    column_names = [desc[0] for desc in mycursor.description]
    df = pd.DataFrame(result,columns=column_names)
    closing_connection(connection,mycursor)
    
    return df


def retrive_top_district_trans(year,quater):
    year = int(year)
    quater = quater

    connection,mycursor = connection_open()
    query = f"SELECT district,amount,count FROM phonepe_pulse.top_trans_india_by_district\
    where qyear = %s and quater = %s order by amount desc"
    mycursor.execute(query,(year ,quater))
    result = mycursor.fetchall()
    # print(result)
    column_names = [desc[0] for desc in mycursor.description]
    df = pd.DataFrame(result,columns=column_names)
    closing_connection(connection,mycursor)
    
    return df


def retrive_top_pincode_trans(year,quater):
    year = int(year)
    quater = quater

    connection,mycursor = connection_open()
    query = f"SELECT pincode,amount,count FROM phonepe_pulse.top_trans_india_by_pincode\
    where qyear = %s and quater = %s order by amount desc"
    mycursor.execute(query,(year ,quater))
    result = mycursor.fetchall()
    # print(result)
    column_names = [desc[0] for desc in mycursor.description]
    df = pd.DataFrame(result,columns=column_names)
    closing_connection(connection,mycursor)
    return df


def retrive_top_state_trans(year,quater):
    year = int(year)
    quater = quater

    connection,mycursor = connection_open()
    query = f"SELECT state,amount,count FROM phonepe_pulse.top_trans_india_by_state\
    where qyear = %s and quater = %s order by amount desc"
    mycursor.execute(query,(year ,quater))
    result = mycursor.fetchall()
    # print(result)
    column_names = [desc[0] for desc in mycursor.description]
    df = pd.DataFrame(result,columns=column_names)
    closing_connection(connection,mycursor)
    return df
