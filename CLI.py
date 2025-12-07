import mysql.connector
import csv
import os
import sys

def import_data(cursor, mydb,folder_name):
    # Drop and create database
    cursor.execute("DROP DATABASE IF EXISTS cs122a_hw2")
    cursor.execute("CREATE DATABASE cs122a_hw2")
    cursor.execute("USE cs122a_hw2")

    # Q1: User and Agent Creator/Client
    cursor.execute("""
    CREATE TABLE User (
        uid INT,
        email TEXT NOT NULL,
        username TEXT NOT NULL,
        PRIMARY KEY (uid)
    )
    """)

    cursor.execute("""
    CREATE TABLE AgentCreator (
        uid INT,
        bio TEXT,
        payout TEXT,
        PRIMARY KEY (uid),
        FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
    )
    """)

    cursor.execute("""
    CREATE TABLE AgentClient (
        uid INT,
        interests TEXT NOT NULL,
        cardholder TEXT NOT NULL,
        expire DATE NOT NULL,
        cardno INT NOT NULL,
        cvv INT NOT NULL,
        zip INT NOT NULL,
        PRIMARY KEY (uid),
        FOREIGN KEY (uid) REFERENCES User(uid) ON DELETE CASCADE
    )
    """)

    # Q2: Base and Customized Model
    cursor.execute("""
    CREATE TABLE BaseModel (
        bmid INT,
        creator_uid INT NOT NULL,
        description TEXT NOT NULL,
        PRIMARY KEY (bmid),
        FOREIGN KEY (creator_uid) REFERENCES AgentCreator(uid) ON DELETE CASCADE
    )
    """)

    cursor.execute("""
    CREATE TABLE CustomizedModel (
        bmid INT,
        mid INT NOT NULL,
        PRIMARY KEY (bmid, mid),
        FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE
    )
    """)

    # Q3: Configurations
    cursor.execute("""
    CREATE TABLE Configuration (
        cid INT,
        client_uid INT NOT NULL,
        content TEXT NOT NULL,
        labels TEXT NOT NULL,
        PRIMARY KEY (cid),
        FOREIGN KEY (client_uid) REFERENCES AgentClient(uid) ON DELETE CASCADE
    )
    """)

    # Q4: Internet Services: LLM/Data Storage
    cursor.execute("""
    CREATE TABLE InternetService (
        sid INT,
        provider TEXT NOT NULL,
        endpoints TEXT NOT NULL,
        PRIMARY KEY (sid)
    )
    """)

    cursor.execute("""
    CREATE TABLE LLMService (
        sid INT,
        domain TEXT,
        PRIMARY KEY (sid),
        FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
    )
    """)

    cursor.execute("""
    CREATE TABLE DataStorage (
        sid INT,
        type TEXT,
        PRIMARY KEY (sid),
        FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
    )
    """)

    # Q5: Relationships
    cursor.execute("""
    CREATE TABLE ModelServices (
        bmid INT NOT NULL,
        sid INT NOT NULL,
        version INT NOT NULL,
        PRIMARY KEY (bmid, sid),
        FOREIGN KEY (bmid) REFERENCES BaseModel(bmid) ON DELETE CASCADE,
        FOREIGN KEY (sid) REFERENCES InternetService(sid) ON DELETE CASCADE
    )
    """)

    cursor.execute("""
    CREATE TABLE ModelConfigurations (
        bmid INT NOT NULL,
        mid INT NOT NULL,
        cid INT NOT NULL,
        duration INT NOT NULL,
        PRIMARY KEY (bmid, mid, cid),
        FOREIGN KEY (bmid, mid) REFERENCES CustomizedModel(bmid, mid) ON DELETE CASCADE,
        FOREIGN KEY (cid) REFERENCES Configuration(cid) ON DELETE CASCADE
    )
    """)

    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.join(script_dir, folder_name)
    #needs to be in an order
    #can't just read sequentially from folder
    file_read("User.csv",folder_path,cursor)
    file_read("AgentCreator.csv",folder_path,cursor)
    file_read("AgentClient.csv",folder_path,cursor)
    file_read("BaseModel.csv",folder_path,cursor)
    file_read("CustomizedModel.csv",folder_path,cursor)
    file_read("Configuration.csv",folder_path,cursor)
    file_read("InternetService.csv",folder_path,cursor)
    file_read("LLMService.csv",folder_path,cursor)
    file_read("DataStorage.csv",folder_path,cursor)
    file_read("ModelServices.csv",folder_path,cursor)
    file_read("ModelConfigurations.csv",folder_path,cursor)
    mydb.commit()

def file_read(file_name,folder_path,cursor):
    file_path = os.path.join(folder_path, file_name)
    table_name = os.path.splitext(file_name)[0]  #use file name as table name

    # Open CSV and read
    with open(file_path, "r", newline="", encoding="utf-8") as csv_file:
        csv_reader = csv.reader(csv_file)
        columns = [col.strip() for col in next(csv_reader)]  
        placeholders = ", ".join(["%s"] * len(columns))
        col_names = ", ".join(columns)
        sql = f"INSERT INTO `{table_name}` ({col_names}) VALUES ({placeholders})"
        for row in csv_reader:
            row = [val.strip() for val in row]  # remove extra whitespace
            cursor.execute(sql, row)  # insert row




def main():
    mydb = mysql.connector.connect(
        
        host="host",
        user="user",
        password="password",
        database="cs122a_hw2"
    )

    cursor = mydb.cursor()

    #CLI interface
    #put your function and needed arguments in here
    if(sys.argv[1]=="import"):
       import_data(cursor,mydb,sys.argv[2])
 


    cursor.close()
    mydb.close()


if __name__ == "__main__":
    main()
