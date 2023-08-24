import pandas as pd 
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

def closing_connection(mycursor,connection):
    mycursor = mycursor 
    connection = connection
    mycursor.close()
    connection.close()
    if connection.is_closed():
        print("sql connection is closed")
    else:
        print("not closed")

# Creating diminsion tables

def creating_dim_tables():
    connection,mycursor = connection_open()
    mycursor = mycursor 
    connection = connection 
    try:
        query_year = """create table qyear (
                        year int primary key 
                        )"""
        mycursor.execute(query_year)
       
    except Error as e:
        print("error is",e)
    try:
        query_quater =  """create table quater(
                        quater varchar(2) primary key
                        )"""
        mycursor.execute(query_quater)
    except Error as e:
        print("error is",e)

    closing_connection(mycursor,connection)

# Creating transaction tables

def creating_transaction_tables():
    connection,mycursor = connection_open()
    mycursor = mycursor 
    connection = connection 
    #table 1
    try:
        aggre_trans_india = """CREATE TABLE aggre_trans_india (
        payment_category varchar(50),
        qyear int,
        quater varchar(2),
        count BIGINT,
        amount DECIMAL(40, 11),
        PRIMARY KEY (payment_category, qyear, quater),
        FOREIGN KEY (qyear) REFERENCES qyear(year), -- Assuming qyear is a table with qyear as the primary key
        FOREIGN KEY (quater) REFERENCES quater(quater) -- Assuming quater is a table with quater as the primary key
        )"""
        mycursor.execute(aggre_trans_india)
    except Error as e:
        print("error is",e)
    #table 2
    try:
        aggre_trans_india_states = """CREATE TABLE aggre_trans_india_states (
            state varchar(50),
            payment_category varchar(50),
            qyear int,
            quater varchar(2),
            count BIGINT,
            amount DECIMAL(40, 11),
            PRIMARY KEY (payment_category, qyear, quater,state),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
            )"""
        mycursor.execute(aggre_trans_india_states)
    except Error as e:
        print("error is",e)
    #table 3
    try:
        aggre_users_india = """ CREATE TABLE aggre_users_india (
            registeredUsers	bigint,
            appOpens bigint,
            brand varchar(20),
            count BIGINT,
            percentage DECIMAL(10,5),
            qyear int,
            quater varchar(2),
            PRIMARY KEY (brand, qyear, quater),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
            )"""
        mycursor.execute(aggre_users_india)
    except Error as e:
        print("error is",e)
    #table 4
    try:
        aggre_user_india_state = """ CREATE TABLE aggre_user_india_states(
            state varchar(50),
            registeredUsers	bigint,
            appOpens bigint,
            brand varchar(20),
            count BIGINT,
            percentage DECIMAL(10,5),
            qyear int,
            quater varchar(2),
            PRIMARY KEY (state,brand, qyear, quater),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
            )"""
        mycursor.execute(aggre_user_india_state)
    except Error as e:
        print("error is",e)

    #table 5
    try:
        map_trans_india = """CREATE TABLE map_trans_india(
            state varchar(50),
            count BIGINT,
            amount DECIMAL(40, 11),
            qyear int,
            quater varchar(2),
            PRIMARY KEY (state,qyear, quater),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
            )"""

        mycursor.execute(map_trans_india)
    except Error as e:
        print("error is",e)

    #table 6
    try:
        map_trans_india_state = """CREATE TABLE map_trans_india_state(
            state varchar(50),
            zone varchar(50),
            count BIGINT,
            amount DECIMAL(40, 11),
            qyear int,
            quater varchar(2),
            PRIMARY KEY (state,qyear,quater,zone),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
            )"""
        mycursor.execute(map_trans_india_state)
    except Error as e:
        print("error is",e)

    #table 7
    try:
        map_user_india = """CREATE TABLE map_user_india(
            state varchar(50),
            registeredUsers	bigint,
            appOpens bigint,
            qyear int,
            quater varchar(2),
            PRIMARY KEY (state,qyear,quater),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
        )"""

        mycursor.execute(map_user_india)
    except Error as e:
        print("error is",e)
    #table 8
    try:
        map_user_india_state = """CREATE TABLE map_user_india_state(
            state varchar(50),
            zone varchar(50),
            registeredUsers	bigint,
            appOpens bigint,
            qyear int,
            quater varchar(2),
            PRIMARY KEY (state,qyear,quater,zone),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
        )"""

        mycursor.execute(map_user_india_state)
    except Error as e:
        print("error is",e)
    #table 9
    try:
        top_trans_india_by_district = """ CREATE TABLE top_trans_india_by_district(
            district varchar(50),
            count BIGINT,
            amount DECIMAL(40, 11),
            qyear int,
            quater varchar(2),
            PRIMARY KEY (district,qyear,quater),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
            )"""
        mycursor.execute(top_trans_india_by_district)
    except Error as e:
        print("error is",e)
    #table 10
    try:
        top_trans_india_by_pincode = """ CREATE TABLE top_trans_india_by_pincode(
            pincode varchar(50),
            count BIGINT,
            amount DECIMAL(40, 11),
            qyear int,
            quater varchar(2),
            PRIMARY KEY (pincode,qyear,quater),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
            )"""

        mycursor.execute(top_trans_india_by_pincode)
    except Error as e:
        print("error is",e)
    
    #table 11
    try:
        top_trans_india_by_state = """ CREATE TABLE top_trans_india_by_state(
            state varchar(50),
            count BIGINT,
            amount DECIMAL(40, 11),
            qyear int,
            quater varchar(2),
            PRIMARY KEY (state,qyear,quater),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
            )"""

        mycursor.execute(top_trans_india_by_state)
    except Error as e:
        print("error is",e)
    #table 12
    try:
        top_trans_india_state_by_district = """ CREATE TABLE top_trans_india_state_by_district(
            state varchar(50),
            district varchar(50),
            count BIGINT,
            amount DECIMAL(40, 11),
            qyear int,
            quater varchar(2),
            PRIMARY KEY (district,qyear,quater,state),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
            )"""

        mycursor.execute(top_trans_india_state_by_district)
    except Error as e:
        print("error is",e)
    #table 13
    try:
        top_trans_india_state_by_pincode = """ CREATE TABLE top_trans_india_state_by_pincode(
            state varchar(50),
            pincode varchar(50),
            count BIGINT,
            amount DECIMAL(40, 11),
            qyear int,
            quater varchar(2),
            PRIMARY KEY (pincode,qyear,quater,state),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
            )"""

        mycursor.execute(top_trans_india_state_by_pincode)
    except Error as e:
        print("error is",e)
    #table 14
    try:
        top_user_india_by_state = """CREATE TABLE top_user_india_by_state(
            state varchar(50),	
            registeredUsers	bigint,
            qyear int,
            quater varchar(2),
            PRIMARY KEY (qyear,quater,state),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
            )"""


        mycursor.execute(top_user_india_by_state)
    except Error as e:
        print("error is",e)
    #table 15
    try:

        top_user_india_by_pincode = """CREATE TABLE top_user_india_by_pincode(
            pincode varchar(50),	
            registeredUsers	bigint,
            qyear int,
            quater varchar(2),
            PRIMARY KEY (qyear,quater,pincode),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
            )"""


        mycursor.execute(top_user_india_by_pincode)
    except Error as e:
        print("error is",e)

    #table 16

    try:

        top_user_india_by_district = """CREATE TABLE top_user_india_by_district(
            district varchar(50),	
            registeredUsers	bigint,
            qyear int,
            quater varchar(2),
            PRIMARY KEY (qyear,quater,district),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
            )"""


        mycursor.execute(top_user_india_by_district)
    except Error as e:
        print("error is",e)

    #table 17

    try:
        top_user_india_state_by_district = """ CREATE TABLE top_user_india_state_by_district(
            state varchar(50),
            district varchar(50),
            registeredUsers	bigint,
            qyear int,
            quater varchar(2),
            PRIMARY KEY (qyear,quater,district,state),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
            )"""

        mycursor.execute(top_user_india_state_by_district)
    except Error as e:
        print("error is",e)

    #table 18

    try:
        top_user_india_state_by_pincode = """ CREATE TABLE top_user_india_state_by_pincode(
            state varchar(50),
            pincode varchar(50),
            registeredUsers	bigint,
            qyear int,
            quater varchar(2),
            PRIMARY KEY (qyear,quater,pincode,state),
            FOREIGN KEY (qyear) REFERENCES qyear(year), 
            FOREIGN KEY (quater) REFERENCES quater(quater)
            )"""
        
        mycursor.execute(top_user_india_state_by_pincode)
    except Error as e:
        print("error is",e)

    closing_connection(mycursor,connection)

if __name__ == "__main__":
    creating_dim_tables()
    creating_transaction_tables()