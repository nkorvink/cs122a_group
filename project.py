"""
CS122A Final Project - Agent Platform Database CLI
"""

import sys
import mysql.connector
from mysql.connector import Error
import csv
import os
import re

# =======================================
# Database Configuration
# =======================================

DB_CONFIG = {
    'host': 'localhost',
    'user': 'test',        # change if your MySQL user is different
    'password': 'password',  # change if your MySQL password is different
    'database': 'cs122a'
}

def get_db_connection():
    """
    Establish and return a database connection using DB_CONFIG.

    Returns:
        mysql.connector.connection.MySQLConnection | None:
            A live connection object if successful, otherwise None.
    """
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None

def execute_query(connection, query, params=None, fetch=False):
    """
    Helper to execute a single SQL query.

    Args:
        connection: An open MySQL connection.
        query (str): SQL query string with %s placeholders.
        params (tuple | list | None): Values to bind to placeholders.
        fetch (bool): If True, returns all fetched rows.
                      If False, commits and returns True/False.

    Returns:
        - If fetch == True:
            list[tuple] | None: Result rows, or None on error.
        - If fetch == False:
            bool: True on success, False on error.
    """
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

# =======================================
# Q1: import_data
# CLI name: "import"
# Usage: python3 cs122a_wip.py import <folderName>
# Output: "Success" or "Fail"
# =======================================

def import_data(folder_name):
    """
    Drop existing tables, recreate the schema, and load data from CSV files.

    Args:
        folder_name (str): Path to the folder containing all required CSVs.

    Side effects:
        - Modifies the database schema and data.
        - Prints "Success" or "Fail".
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
            "DROP TABLE IF EXISTS Client_Interests",
            "DROP TABLE IF EXISTS AgentClient",
            "DROP TABLE IF EXISTS AgentCreator",
            "DROP TABLE IF EXISTS User",
            "SET FOREIGN_KEY_CHECKS = 1"
        ]

        for drop_query in drop_tables:
            cursor.execute(drop_query)

        # Create tables based on the project schema
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

        # Load CSV data into tables
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
                    header = next(csv_reader, None)
                    if not header:
                        continue

                    # Normalize header names (strip spaces)
                    header = [h.strip() for h in header]

                    columns = ','.join(header)
                    placeholders = ','.join(['%s'] * len(header))
                    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

                    for row in csv_reader:
                        # Convert 'NULL' or empty strings to None
                        row = [None if val in ('NULL', '') else val for val in row]
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

# =======================================
# Q2: insertAgentClient
# CLI name: "insertAgentClient"
# Usage:
#   python3 cs122a_wip.py insertAgentClient uid username email cardNumber cardHolder expirationDate cvv zip interests
# Output: "Success" or "Fail"
# =======================================

def insert_agent_client(uid, username, email, card_number, card_holder,
                        expiration_date, cvv, zip_code, interests):
    """
    Insert a new agent client, including user info, payment info, and interests.

    Args:
        uid (int): User/Client ID.
        username (str): Username.
        email (str): Email address.
        card_number (int): Credit card number.
        card_holder (str): Card holder's full name.
        expiration_date (str): Expiration date (YYYY-MM-DD).
        cvv (int): CVV code.
        zip_code (int): ZIP/postal code.
        interests (str): Comma- or semicolon-separated list of interests.

    Side effects:
        - Inserts into User, AgentClient, and Client_Interests.
        - Prints "Success" or "Fail".
    """
    connection = get_db_connection()
    if not connection:
        print("Fail")
        return

    try:
        cursor = connection.cursor()

        # Insert into User
        user_query = "INSERT INTO User (uid, username, email) VALUES (%s, %s, %s)"
        try:
            cursor.execute(user_query, (uid, username, email))
        except:
            pass
        # Insert into AgentClient
        client_query = """
            INSERT INTO AgentClient
                         (uid, interests, card_holder_name, expiration_date, card_number, cvv, zip)
                         VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(
            client_query,
            (uid, interests, card_holder, expiration_date, card_number, cvv, zip_code)
        )

        # Insert interests into Client_Interests one by one
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

# =======================================
# Q3: addCustomizedModel
# CLI name: "addCustomizedModel"
# Usage: python3 cs122a_wip.py addCustomizedModel mid bmid
# Output: "Success" or "Fail"
# =======================================

def add_customized_model(mid, bmid):
    """
    Create a new customized model linked to an existing base model.

    Args:
        mid (int): Customized model ID.
        bmid (int): Base model ID.

    Side effects:
        - Inserts into CustomizedModel.
        - Prints "Success" or "Fail".
    """
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

# =======================================
# Q4: deleteBaseModel
# CLI name: "deleteBaseModel"
# Usage: python3 cs122a_wip.py deleteBaseModel bmid
# Output: "Success" or "Fail"
# =======================================

def delete_base_model(bmid):
    """
    Delete a base model. CASCADE rules handle related rows.

    Args:
        bmid (int): Base model ID.

    Side effects:
        - Deletes from BaseModel (and dependent rows).
        - Prints "Success" or "Fail".
    """
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
        rows = cursor.rowcount
        if rows == 0:
            return
        else:
            print("Success")

    except Error:
        print("Fail")
        if connection and connection.is_connected():
            connection.rollback()
            connection.close()

# =======================================
# Q5: listInternetService
# CLI name: "listInternetService"
# Usage: python3 cs122a_wip.py listInternetService bmid
# Output: CSV rows: sid,endpoint,provider
# =======================================

def list_internet_service(bmid):
    """
    List internet services utilized by a given base model.

    Args:
        bmid (int): Base model ID.

    Output:
        Prints each matching row as CSV:
        sid,endpoint,provider
    """
    connection = get_db_connection()
    if not connection:
        return

    try:
        query = """
            SELECT i.sid, i.endpoint, i.provider
            FROM InternetService i
            JOIN BaseModelUtilization b ON i.sid = b.sid
            WHERE b.bmid = %s
            ORDER BY i.sid ASC
        """

        result = execute_query(connection, query, (bmid,), fetch=True)
        connection.close()

        for row in result or []:
            print(','.join(str(col) for col in row))

    finally:
        if connection and connection.is_connected():
            connection.close()

# =======================================
# Q6: countCustomizedModel
# CLI name: "countCustomizedModel"
# Usage: python3 cs122a_wip.py countCustomizedModel bmid1 [bmid2 ...]
# Output: CSV rows: bmid,description,customizedModelCount
# =======================================

def count_customized_model(*bmids):
    """
    Count how many customized models exist for each given base model ID.

    Args:
        *bmids (int): One or more base model IDs.

    Output:
        Prints each row as CSV:
        bmid,description,customizedModelCount
    """
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

        for row in result or []:
            print(','.join(str(col) for col in row))

    except Error:
        if connection and connection.is_connected():
            connection.close()

# =======================================
# Q7: topNDurationConfig
# CLI name: "topNDurationConfig"
# Usage: python3 cs122a_wip.py topNDurationConfig uid N
# Output: CSV rows: uid,cid,label,content,duration
# =======================================

def top_n_duration_config(uid, n):
    """
    Return the top N configurations with the longest duration for a client.

    Args:
        uid (int): Client user ID.
        n (int): Number of rows to return (top N).

    Output:
        Prints each row as CSV:
        uid,cid,label,content,duration
    """
    connection = get_db_connection()
    if not connection:
        return

    try:
        query = """
            SELECT uid, cid, label, content, duration
            FROM ModelConfiguration
            WHERE uid = %s
            ORDER BY duration DESC, cid ASC
            LIMIT %s
        """

        result = execute_query(connection, query, (uid, n), fetch=True)
        connection.close()

        for row in result or []:
            print(','.join(str(col) for col in row))

    except Error:
        if connection and connection.is_connected():
            connection.close()

# =======================================
# Q8: listBaseModelKeyWord
# CLI name: "listBaseModelKeyWord"
# Usage: python3 cs122a_wip.py listBaseModelKeyWord keyword
# Output: CSV rows: bmid,sid,provider,domain (max 5 rows)
# =======================================

def list_base_model_keyword(keyword):
    """
    List base models that use LLM services whose domain contains a keyword.

    Args:
        keyword (str): Substring to match in LLMService.domain.

    Output:
        Prints up to 5 rows as CSV:
        bmid,sid,provider,domain
    """
    connection = get_db_connection()
    if not connection:
        return

    try:
        query = """
            SELECT DISTINCT b.bmid, i.sid, i.provider, l.domain
            FROM BaseModel b
            JOIN BaseModelUtilization u ON b.bmid = u.bmid
            JOIN InternetService i ON u.sid = i.sid
            LEFT JOIN LLMService l ON i.sid = l.sid
            WHERE l.domain IS NOT NULL
            AND l.domain LIKE %s
            ORDER BY b.bmid ASC
            LIMIT 5
        """

        keyword_pattern = f"%{keyword}%"
        result = execute_query(connection, query, (keyword_pattern,), fetch=True)
        connection.close()

        for row in result or []:
            print(','.join(str(col) for col in row))

    except Error:
        if connection and connection.is_connected():
            connection.close()

# =======================================
# Q9: printNL2SQLresult
# CLI name: "printNL2SQLresult"
# Usage: python3 cs122a_wip.py printNL2SQLresult
# Output: CSV rows directly from nl2sql_results.csv
# =======================================

def print_nl2sql_result():
    """
    Print the NL2SQL experiment results from a CSV file.

    The file is expected to be named 'nl2sql_results.csv'
    and located in the same directory as this script.

    Output:
        Prints each CSV row as-is.
    """
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

# =======================================
# Main Dispatcher
# =======================================

def main():
    """
    Parse command-line arguments and dispatch to the appropriate function.

    Usage:
        python3 cs122a_wip.py <functionName> [params...]

    Supported functionName values:
        - import
        - insertAgentClient
        - addCustomizedModel
        - deleteBaseModel
        - listInternetService
        - countCustomizedModel
        - topNDurationConfig
        - listBaseModelKeyWord
        - printNL2SQLresult
    """
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
                int(args[0]),  # uid
                args[1],       # username
                args[2],       # email
                int(args[3]),  # card_number
                args[4],       # card_holder
                args[5],       # expiration_date
                int(args[6]),  # cvv
                int(args[7]),  # zip
                args[8]        # interests
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
