import sys
import mysql.connector
from mysql.connector import Error
import csv
import os

# =======================================
# CS122A Project 
# =======================================
# Niko:
#   - Q1: import_data
#   - Q2: insertAgentClient
#   - Q9: printNL2SQLresult (+ NL2SQL CSV creation)
#
# Nick:
#   - Q3: addCustomizedModel
#   - Q4: deleteBaseModel
#   - Q7: topNDurationConfig
#
# Tony:
#   - Q5: listInternetService
#   - Q6: countCustomizedModel
#   - Q8: listBaseModelKeyWord
#   - Testing + README/documentation
# =======================================

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'user': 'test',
    'password': 'password',
    'database': 'cs122a'
}

# Core Database Utilities

def get_db_connection():
    """Establish and return a database connection."""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None

# =======================================
# Q1 — Import Data
# =======================================
def import_data(folder_name):
    """
    Delete existing tables, create new tables (DDL), and import CSV data.
    """
    connection = get_db_connection()
    if not connection:
        print("Fail")
        return
    
    try:
        cursor = connection.cursor()
        
        # Drop existing tables (in correct order to handle foreign keys)
        drop_tables = [
            "SET FOREIGN_KEY_CHECKS = 0", 
            "DROP TABLE IF EXISTS ModelConfiguration",
            "DROP TABLE IF EXISTS CustomizedModel",
            "DROP TABLE IF EXISTS BaseModelUtilization",
            "DROP TABLE IF EXISTS BaseModel",
            "DROP TABLE IF EXISTS LLMService",
            "DROP TABLE IF EXISTS DataStorageService",
            "DROP TABLE IF EXISTS InternetService",
            "DROP TABLE IF EXISTS PaymentMethod", 
            "DROP TABLE IF EXISTS Client_Interests", 
            "DROP TABLE IF EXISTS AgentClient",
            "DROP TABLE IF EXISTS AgentCreator",
            "DROP TABLE IF EXISTS User",
            "SET FOREIGN_KEY_CHECKS = 1" 
        ]
        
        for drop_query in drop_tables:
            cursor.execute(drop_query)
        
        # CREATE TABLES - Based on the provided correct DDL
        create_tables = [
            """CREATE TABLE User (
                uid INT PRIMARY KEY,
                username VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL UNIQUE 
            )""",
            """CREATE TABLE AgentCreator (
                uid INT PRIMARY KEY,
                payout_account VARCHAR(255) NOT NULL,
                bio TEXT,
                FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
            )""",
            """CREATE TABLE AgentClient (
                uid INT PRIMARY KEY,
                interests TEXT,
                card_holder_name VARCHAR(255) NOT NULL,
                expiration_date DATE NOT NULL,
                card_number BIGINT NOT NULL,
                cvv INT NOT NULL,
                zip INT NOT NULL,
                FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
            )""",
            """CREATE TABLE Client_Interests (
                uid INT,
                interest VARCHAR(255),
                PRIMARY KEY (uid, interest), 
                FOREIGN KEY (uid) REFERENCES AgentClient(uid) ON DELETE CASCADE
            )""",
            """CREATE TABLE InternetService (
                sid INT PRIMARY KEY,
                endpoint VARCHAR(500) NOT NULL,
                provider VARCHAR(255) NOT NULL
            )""",
            """CREATE TABLE LLMService (
                sid INT PRIMARY KEY,
                domain VARCHAR(255),
                FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
            )""",
            """CREATE TABLE DataStorageService (
                sid INT PRIMARY KEY,
                type VARCHAR(255),
                FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
            )""",
            """CREATE TABLE BaseModel (
                bmid INT PRIMARY KEY,
                uid INT NOT NULL, 
                description TEXT,
                FOREIGN KEY (uid) REFERENCES AgentCreator(uid) ON DELETE CASCADE
            )""",
            """CREATE TABLE BaseModelUtilization (
                bmid INT,
                sid INT,
                version INT NOT NULL,
                PRIMARY KEY (bmid, sid),
                FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE,
                FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
            )""",
            """CREATE TABLE CustomizedModel (
                bmid INT NOT NULL,
                mid INT PRIMARY KEY,
                FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE
            )""",
            """CREATE TABLE ModelConfiguration (
                cid INT PRIMARY KEY,
                uid INT NOT NULL,
                mid INT NOT NULL,
                label VARCHAR(255),
                content TEXT,
                duration INT,
                FOREIGN KEY (uid) REFERENCES AgentClient(uid) ON DELETE CASCADE,
                FOREIGN KEY (mid) REFERENCES CustomizedModel(mid) ON DELETE CASCADE
            )"""
        ]
        
        for create_query in create_tables:
            cursor.execute(create_query)
        
        connection.commit()
        
        # Import CSV files
        csv_tables = [
            ('User.csv', 'User'),
            ('AgentCreator.csv', 'AgentCreator'),
            ('AgentClient.csv', 'AgentClient'), 
            ('Client_Interests.csv', 'Client_Interests'), 
            ('InternetService.csv', 'InternetService'),
            ('LLMService.csv', 'LLMService'),
            ('DataStorageService.csv', 'DataStorageService'),
            ('BaseModel.csv', 'BaseModel'),
            ('BaseModelUtilization.csv', 'BaseModelUtilization'),
            ('CustomizedModel.csv', 'CustomizedModel'),
            ('ModelConfiguration.csv', 'ModelConfiguration'),
        ]
        
        for csv_file, table_name in csv_tables:
            file_path = os.path.join(folder_name, csv_file)
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    csv_reader = csv.reader(f)
                    next(csv_reader, None) # Skip header row
                    for row in csv_reader:
                        # Convert 'NULL' strings to None
                        row = [None if val in ('NULL', '') else val for val in row]
                        
                        placeholders = ','.join(['%s'] * len(row))
                        insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                        
                        try:
                            cursor.execute(insert_query, row)
                        except Error as insert_e:
                            print(f"Error inserting into {table_name} with data {row}: {insert_e}")
                            raise 
                            
        connection.commit()
        cursor.close()
        connection.close()
        print("Success")
        
    except Error as e:
        print(f"Error during import: {e}")
        print("Fail")
        if connection and connection.is_connected():
            connection.rollback()
            connection.close()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        print("Fail")

# =======================================
# Q2 — insertAgentClient
# =======================================
def insertAgentClient(uid, username, email, card_holder_name, expiration_date, card_number, cvv, zip):
    # TODO: implement actual logic
    print("Success placeholder for Q2")
    return


# =======================================
# Q3 — addCustomizedModel
# =======================================
def addCustomizedModel(bmid, mid):
    # TODO: implement actual logic
    print("Success placeholder for Q3")
    return


# =======================================
# Q4 — deleteBaseModel
# =======================================
def deleteBaseModel(bmid):
    # TODO: implement actual logic
    print("Success placeholder for Q4")
    return


# =======================================
# Q5 — listInternetService - TONY
# =======================================
def listInternetService(bmid):
    # TODO: implement Q5 logic
    print("Success placeholder for Q5")
    return


# =======================================
# Q6 — countCustomizedModel - TONY
# =======================================
def countCustomizedModel():
    # TODO: implement Q6
    print("Success placeholder for Q6")
    return


# =======================================
# Q7 — topNDurationConfig
# =======================================
def topNDurationConfig(uid, N):
    # TODO: implement Q7 logic
    print("Success placeholder for Q7")
    return


# =======================================
# Q8 — listBaseModelKeyWord - TONY
# =======================================
def listBaseModelKeyWord(keyword):
    # TODO: implement Q8 logic
    print("Success placeholder for Q8")
    return


# =======================================
# Q9 — printNL2SQLresult
# =======================================
def printNL2SQLresult(filename):
    # TODO: implement Q9
    print("Success placeholder for Q9")
    return


# Main Entry Point (Only recognizes 'import')
def main():
    if len(sys.argv) < 2:
        print("Usage: python3 project.py <function_name> [params...]")
        return
    
    function_name = sys.argv[1]
    args = sys.argv[2:]
    
    # Route to appropriate function
    try:
        if function_name == "import":
            if len(args) < 1:
                print("Usage: python3 project.py import [folderName:str]")
                return
            import_data(args[0])
        
        else:
            print(f"Unknown function: {function_name}")
            
    except IndexError:
        print("Error: Missing arguments for function.")
    except ValueError:
        print("Error: Invalid argument type (expected int or date).")

if __name__ == "__main__":
    main()
