import pyodbc
from pymongo import MongoClient
import copy

show = True # Print on screen to know process being done during pipeline process

def SqlServer_Initialise():
    #sql initializitaion using pyodbc
    return pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                      "Server=34.132.216.173;"
                      "Database=TestDB;"
                      "uid=sa;pwd=Newuser123@")


def Mongo_Initialise():    
    return MongoClient('mongodb://AdminCherry:123456@34.171.176.28:27017')['staff'] 
 
        

def fetch_sql_query(sql, cursor, query_types):
    #Function that will be used to carry out queries
    if query_types == "fetchall":      
        cursor.execute(sql)
        return cursor.fetchall()
    else:
        print("fetchall only try again ")

#data from db provided
def extract_sqldata(sql_cursor):     
    staff = fetch_sql_query('select * from staff', sql_cursor, 'fetchall')
    #Inventory = fetch_sql_query('select * from Inventory', sql_cursor, 'fetchall')
    #staff_hobbies = fetch_sql_query('select * from staff_hobbies', sql_cursor, 'fetchall')       
    return staff


def transform_sql_to_mongo(collection,sqltable):
    collection_test = []
    sqltable_data = { }
    if sqltable == "staff":
        for item in collection:            
            sqltable_data['name'] = item[1]
            sqltable_data['age'] = item[2]
            sqltable_data['city'] = item[3]
            sqltable_data['married'] = item[4]
            collection_test.append(copy.copy(sqltable_data)) #https://www.python-course.eu/deep_copy.php reference
            #appeneded
        return collection_test


#Loads transfromed sqldata into mongo db( L of ETL)
def load_data(mongo_collection, collection_test):       
    return mongo_collection.insert(collection_test)


def main():
    #starts pipeline
    if show:        
        print('Sql Server Connection Initialision')
    sql_server = SqlServer_Initialise()

    if show:
        print('Sql Server connected')
        print('Starting data pipeline stage 1 : Extracting data from SQL Server')
    sql_server_cursor = sql_server.cursor()
    sql_server_data = extract_sqldata(sql_server_cursor)

    if show:
        print('Stage 1 completed! Data successfully extracted from SQL Server')
        print('Starting stage 2: Transforming all data from SQL Server to MongoDB')        
    staff_collection = transform_sql_to_mongo(sql_server_data, "staff")


    if show:       
        print('Data successfully transformed')
        print('Intialising MongoDB connection')
    mongo = Mongo_Initialise()

    if show:
        print('MongoDB connection successfully')
        print('Loading transformed data into mongo')
    result = load_data(mongo['staff'], staff_collection)


    if show:
        print('Stage 3 completed! Data successfully loaded')
        print('Closing SQL server connection')
    sql_server.close()
    if show:
        print('SQL server connection closed successfully')
        

main()