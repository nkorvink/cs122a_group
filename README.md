📘 CS122A Final Project — Agent Platform Database
👥 Team Members & Responsibilities
Niko
Q1: import_data
Q2: insertAgentClient
Q9: printNL2SQLresult

Nick
Q3: addCustomizedModel
Q4: deleteBaseModel
Q7: topNDurationConfig
Testing

Tony
Q5: listInternetService
Q6: countCustomizedModel
Q8: listBaseModelKeyWord
Documentation

▶️ Running the Program
Every function is called from the command line:
python3 cs122a_wip.py <functionName> [arguments...]
Example:
python3 cs122a_wip.py import dataFolder

📦 Function Overview
Q1 — import_data(folderName): Recreates all tables and loads CSV files.
Q2–Q4: Insert, add, and delete operations for AgentClient / CustomizedModel / BaseModel.
Q5 — listInternetService(bmid): Lists the internet services used by a BaseModel.
Q6 — countCustomizedModel(): Counts customized models per BaseModel.
Q7 — topNDurationConfig(uid, N): Lists the top N longest duration configurations for a client.
Q8 — listBaseModelKeyWord(keyword): Finds BaseModels whose LLM domain contains a keyword.
Q9 — printNL2SQLresult(file): Outputs the results of the NL2SQL evaluation.

🛠 Setup
Install MySQL and create the database:
CREATE DATABASE cs122a;

Install Python dependency:
pip install mysql-connector-python

Update DB credentials in the script if needed:
DB_CONFIG = {
    'host': 'localhost',
    'user': 'test',
    'password': 'password',
    'database': 'cs122a'
}

📂 CSV Requirements
The folder passed to import_data must contain all required CSVs (User.csv, BaseModel.csv, etc.).

✔️ Notes
Functions output either "Success" / "Fail" or CSV-formatted rows.
Additional documentation will be added as functions are implemented.
