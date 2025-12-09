import sys
import mysql.connector
from mysql.connector import Error
import csv
import os
import re

# Database credentials
DB_CONFIG = {
    'host': 'localhost',
    'user': 'test',
    'password': 'password',
    'database': 'cs122a'
}

def get_db_connection():
    """Get a database connection."""
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None

def execute_query(connection, query, params=None, fetch=False):
    """Run a query and handle the result."""
    try:
        cursor = connection.cursor()
        cursor.execute(query, params) if params else cursor.execute(query)
        
        if fetch:
            result = cursor.fetchall()
            cursor.close()
            return result
        else:
            connection.commit()
            cursor.close()
            return True
    except Error:
        connection.rollback()
        return False if not fetch else None

# Q1: Set up the database from scratch
def import_data(folder_name):
    """Drop existing tables, create fresh schema, load CSV data."""
    connection = get_db_connection()
    if not connection:
        print("Fail")
        return
    
    try:
        cursor = connection.cursor()
        
        # Nuke everything
        drop_tables = [
            "SET FOREIGN_KEY_CHECKS = 0", 
            "DROP TABLE IF EXISTS ModelConfiguration",
            "DROP TABLE IF EXISTS CustomizedModel",
            "DROP TABLE IF EXISTS BaseModelUtilization",
            "DROP TABLE IF EXISTS BaseModel",
            "DROP TABLE IF EXISTS LLMService",
            "DROP TABLE IF EXISTS DataStorageService",
            "DROP TABLE IF EXISTS InternetService",
            "DROP TABLE IF EXISTS Client_Interests", 
            "DROP TABLE IF EXISTS AgentClient",
            "DROP TABLE IF EXISTS AgentCreator",
            "DROP TABLE IF EXISTS User",
            "SET FOREIGN_KEY_CHECKS = 1" 
        ]
        
        for drop_query in drop_tables:
            cursor.execute(drop_query)
        
        # Build the schema
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
        
        # Load the data
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
                    next(csv_reader, None)
                    for row in csv_reader:
                        row = [None if val in ('NULL', '') else val for val in row]
                        placeholders = ','.join(['%s'] * len(row))
                        insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
                        cursor.execute(insert_query, row)
                            
        connection.commit()
        cursor.close()
        connection.close()
        print("Success")
        
    except Exception as e:
        print("Fail")
        if connection and connection.is_connected():
            connection.rollback()
            connection.close()

# Q2: Add a new client with payment info and interests
def insert_agent_client(uid, username, email, card_number, card_holder, 
                        expiration_date, cvv, zip_code, interests):
    """Create a new agent client with their payment details."""
    connection = get_db_connection()
    if not connection:
        print("Fail")
        return
    
    try:
        cursor = connection.cursor()
        
        # Add user
        user_query = "INSERT INTO User (uid, username, email) VALUES (%s, %s, %s)"
        cursor.execute(user_query, (uid, username, email))
        
        # Add client with payment info
        client_query = """INSERT INTO AgentClient 
                         (uid, card_number, card_holder_name, expiration_date, cvv, zip, interests) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        cursor.execute(client_query, (uid, card_number, card_holder, expiration_date, cvv, zip_code, interests))
        
        # Break out interests into separate table
        if interests:
            interest_list = [i.strip() for i in re.split(r'[;,]', interests) if i.strip()]
            interest_query = "INSERT INTO Client_Interests (uid, interest) VALUES (%s, %s)"
            for interest in interest_list:
                cursor.execute(interest_query, (uid, interest))
        
        connection.commit()
        cursor.close()
        connection.close()
        print("Success")
        
    except Error:
        print("Fail")
        if connection and connection.is_connected():
            connection.rollback()
            connection.close()

# Q3: Create a customized version of a base model
def add_customized_model(mid, bmid):
    """Add a new customized model based on an existing base model."""
    connection = get_db_connection()
    if not connection:
        print("Fail")
        return
    
    try:
        cursor = connection.cursor()
        query = "INSERT INTO CustomizedModel (mid, bmid) VALUES (%s, %s)"
        cursor.execute(query, (mid, bmid))
        connection.commit()
        cursor.close()
        connection.close()
        print("Success")
        
    except Error:
        print("Fail")
        if connection and connection.is_connected():
            connection.rollback()
            connection.close()

# Q4: Remove a base model (cascades to customized models)
def delete_base_model(bmid):
    """Delete a base model and everything that depends on it."""
    connection = get_db_connection()
    if not connection:
        print("Fail")
        return
    
    try:
        cursor = connection.cursor()
        cursor.execute("DELETE FROM BaseModel WHERE bmid = %s", (bmid,))
        connection.commit()
        cursor.close()
        connection.close()
        print("Success")
        
    except Error:
        print("Fail")
        if connection and connection.is_connected():
            connection.rollback()
            connection.close()

# Q5: Show all services used by a base model
def list_internet_service(bmid):
    """List internet services that a base model uses."""
    connection = get_db_connection()
    if not connection:
        return
    
    try:
        query = """
            SELECT i.sid, i.endpoint, i.provider
            FROM InternetService i
            JOIN BaseModelUtilization b ON i.sid = b.sid
            WHERE b.bmid = %s
            ORDER BY i.provider ASC
        """
        
        result = execute_query(connection, query, (bmid,), fetch=True)
        connection.close()
        
        if result:
            for row in result:
                print(','.join(str(col) for col in row))
        
    except Error:
        if connection and connection.is_connected():
            connection.close()

# Q6: Count how many customized models exist for each base model
def count_customized_model(*bmids):
    """Count customized models for given base model IDs."""
    connection = get_db_connection()
    if not connection:
        return
    
    try:
        placeholders = ','.join(['%s'] * len(bmids))
        query = f"""
            SELECT b.bmid, b.description, COUNT(c.mid) as customizedModelCount
            FROM BaseModel b
            LEFT JOIN CustomizedModel c ON b.bmid = c.bmid
            WHERE b.bmid IN ({placeholders})
            GROUP BY b.bmid, b.description
            ORDER BY b.bmid ASC
        """
        
        result = execute_query(connection, query, bmids, fetch=True)
        connection.close()
        
        if result:
            for row in result:
                print(','.join(str(col) for col in row))
        
    except Error:
        if connection and connection.is_connected():
            connection.close()

# Q7: Get the longest-running configs for a client
def top_n_duration_config(uid, n):
    """Find the N longest duration configurations for a client."""
    connection = get_db_connection()
    if not connection:
        return
    
    try:
        query = """
            SELECT uid, cid, label, content, duration
            FROM ModelConfiguration
            WHERE uid = %s
            ORDER BY duration DESC
            LIMIT %s
        """
        
        result = execute_query(connection, query, (uid, n), fetch=True)
        connection.close()
        
        if result:
            for row in result:
                print(','.join(str(col) for col in row))
        
    except Error:
        if connection and connection.is_connected():
            connection.close()

# Q8: Find base models using LLM services with a specific keyword
def list_base_model_keyword(keyword):
    """List base models using LLM services whose domain matches the keyword."""
    connection = get_db_connection()
    if not connection:
        return
    
    try:
        query = """
            SELECT DISTINCT b.bmid, i.sid, i.provider, l.domain
            FROM BaseModel b
            JOIN BaseModelUtilization u ON b.bmid = u.bmid
            JOIN InternetService i ON u.sid = i.sid
            JOIN LLMService l ON i.sid = l.sid 
            WHERE l.domain LIKE %s
            ORDER BY b.bmid ASC
            LIMIT 5
        """
        
        keyword_pattern = f"%{keyword}%"
        result = execute_query(connection, query, (keyword_pattern,), fetch=True)
        connection.close()
        
        if result:
            for row in result:
                print(','.join(str(col) for col in row))
        
    except Error:
        if connection and connection.is_connected():
            connection.close()

# Q9: Dump the NL2SQL results
def print_nl2sql_result():
    """Print the NL2SQL experiment results from CSV."""
    csv_file = 'nl2sql_results.csv'
    
    try:
        if not os.path.exists(csv_file):
            print(f"Error: {csv_file} not found")
            return
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                print(','.join(row))
        
    except Exception as e:
        print(f"Error reading CSV: {e}")

# Main dispatcher
def main():
    if len(sys.argv) < 2:
        print("Usage: python3 project.py <function_name> [params...]")
        return
    
    function_name = sys.argv[1]
    args = sys.argv[2:]
    
    try:
        if function_name == "import":
            if len(args) < 1:
                print("Usage: python3 project.py import [folderName:str]")
                return
            import_data(args[0])
        
        elif function_name == "insertAgentClient":
            if len(args) < 9:
                print("Usage: python3 project.py insertAgentClient [uid:int] [username:str] ... [interests:str]")
                return
            insert_agent_client(
                int(args[0]), args[1], args[2], int(args[3]), 
                args[4], args[5], int(args[6]), int(args[7]), args[8]
            )
        
        elif function_name == "addCustomizedModel":
            add_customized_model(int(args[0]), int(args[1]))
        
        elif function_name == "deleteBaseModel":
            delete_base_model(int(args[0]))
        
        elif function_name == "listInternetService":
            list_internet_service(int(args[0]))
        
        elif function_name == "countCustomizedModel":
            bmids = [int(arg) for arg in args]
            count_customized_model(*bmids)
        
        elif function_name == "topNDurationConfig":
            top_n_duration_config(int(args[0]), int(args[1]))
        
        elif function_name == "listBaseModelKeyWord":
            list_base_model_keyword(args[0])
        
        elif function_name == "printNL2SQLresult":
            print_nl2sql_result()
        
        else:
            print(f"Unknown function: {function_name}")
            
    except IndexError:
        print("Error: Missing arguments for function.")
    except ValueError:
        print("Error: Invalid argument type (expected int or date).")

if __name__ == "__main__":
    main()