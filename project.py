import mysql.connector
import csv
import os
import sys

def import_data(cursor, mydb,folder_name):
    # Drop and create database
    cursor.execute("DROP DATABASE IF EXISTS cs122a")
    cursor.execute("CREATE DATABASE cs122a")
    cursor.execute("USE cs122a")

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
        cardno BIGINT NOT NULL,
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
    try:
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
    except mysql.connector.Error as e:
        mydb.rollback()
        print("Fail")
    else:
        mydb.commit()
        print("Success")

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

def insert_agent_client(mydb, cursor, uid, username, email, cardno, cardholder, expire, cvv, zip_code, interests):
    try:
        # Insert into AgentClient table
        cursor.execute(
            """
            INSERT INTO AgentClient
            (uid, interests, cardholder, expire, cardno, cvv, zip)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (uid, interests, cardholder, expire, cardno, cvv, zip_code)
        )
        mydb.commit()
        print("Success")
    
    except mysql.connector.Error as e:
        print("Fail")



def add_customized_model(mydb, cursor, mid, bmid):
    try:
        query = "INSERT INTO CustomizedModel (bmid, mid) VALUES (%s, %s)"
        cursor.execute(query, (bmid, mid))
        mydb.commit()
        print("Success")
    except mysql.connector.Error as e:
        print("Fail")

def delete_base_model(mydb, cursor, bmid):
    try:
        cursor.execute("DELETE FROM BaseModel WHERE bmid = %s", (bmid,))

        if cursor.rowcount == 0:
            mydb.rollback()
            print("Fail")
            return

        mydb.commit()
        print("Success")

    except mysql.connector.Error:
        mydb.rollback()
        print("Fail")

def listInternetService(cursor, bmid):
    cursor.execute(
        """
        SELECT I.sid, I.endpoints, I.provider
        FROM InternetService I
        JOIN ModelServices MS ON I.sid = MS.sid
        WHERE MS.bmid = %s
        ORDER BY I.provider ASC;
        """,
        (bmid,)
    )
    rows = cursor.fetchall()
    for row in rows:
        print(",".join(str(x) for x in row))

def countCustomizedModel(bmodels, cursor):
    # Prepare placeholders for variable number of BMIDs
    placeholders = ", ".join(["%s"] * len(bmodels))
    sql = f"""
        SELECT BM.bmid, BM.description, COUNT(CM.mid) AS num_customized
        FROM BaseModel BM
        LEFT JOIN CustomizedModel CM ON BM.bmid = CM.bmid
        WHERE BM.bmid IN ({placeholders})
        GROUP BY BM.bmid, BM.description
        ORDER BY BM.bmid ASC;
    """
    
    cursor.execute(sql, bmodels)
    rows = cursor.fetchall()
    
    for row in rows:
        print(",".join(str(x) for x in row))
        
def findTopLongestDuration(cursor, client_uid, n: int):
    query = """
        SELECT c.client_uid, c.cid, c.labels, c.content, MAX(mc.duration) AS duration
        FROM Configuration c
        JOIN ModelConfigurations mc ON c.cid = mc.cid
        WHERE c.client_uid = %s
        GROUP BY c.client_uid, c.cid, c.labels, c.content
        ORDER BY duration DESC
        LIMIT %s
    """
    cursor.execute(query, (client_uid, n))
    rows = cursor.fetchall()
    for row in rows:
        print(",".join(str(x) for x in row))

def listBaseModelKeyWord(cursor, keyword):
    key = f"%{keyword}%"
    cursor.execute(
        """
        SELECT BM.bmid, MS.sid, I.provider, LLM.domain
        FROM BaseModel BM
        JOIN ModelServices MS ON BM.bmid = MS.bmid
        JOIN LLMService LLM ON MS.sid = LLM.sid
        JOIN InternetService I ON MS.sid = I.sid
        WHERE LLM.domain LIKE %s
        ORDER BY BM.bmid ASC
        LIMIT 5;
        """,
        (key,)
    )
    rows = cursor.fetchall()
    for row in rows:
        print(",".join(str(x) for x in row))

def printNL2SQLResult():
    with open('NL2SQL.csv', newline='') as csvfile:
        data = csv.reader(csvfile)
        for row in data:
            print("{" + ",\n".join("Success" if x == "True" else ("Fail" if x == "False" else str(x)) for x in row) + "}")

def main():
    mydb = mysql.connector.connect(
        
        host="localhost",
        user="test",
        password="password",
        database="cs122a"
    )

    cursor = mydb.cursor()

    #CLI interface
    #put your function and needed arguments in here
    if(sys.argv[1]=="import"):
       import_data(cursor,mydb,sys.argv[2])
    elif sys.argv[1] == "insertAgentClient":
        uid = int(sys.argv[2])
        username = sys.argv[3]
        email = sys.argv[4]
        cardno = int(sys.argv[5])
        cardholder = sys.argv[6]
        expire = sys.argv[7]
        cvv = int(sys.argv[8])
        zip_code = int(sys.argv[9])
        interests = sys.argv[10]
        insert_agent_client(mydb, cursor, uid, username, email, cardno, cardholder, expire, cvv, zip_code, interests)
    elif sys.argv[1] == "addCustomizedModel":
        mid = int(sys.argv[2])
        bmid = int(sys.argv[3])
        add_customized_model(mydb, cursor, mid, bmid)
    elif sys.argv[1] == "deleteBaseModel":
        bmid = int(sys.argv[2])
        delete_base_model(mydb, cursor, bmid)
    elif sys.argv[1] == "listInternetService":
        bmid = int(sys.argv[2])
        listInternetService(cursor, bmid)
    elif sys.argv[1] == "countCustomizedModel":
        input_bmids = sys.argv[2:]
        unique_bmids = sorted({int(x) for x in input_bmids})
        countCustomizedModel(unique_bmids,cursor)
    elif sys.argv[1] == "topNDurationConfig":
        client_uid = int(sys.argv[2])
        n = int(sys.argv[3])
        findTopLongestDuration(cursor, client_uid, n)
    elif(sys.argv[1] == "listBaseModelKeyWord"):
        listBaseModelKeyWord(cursor,sys.argv[2])
    elif(sys.argv[1] == "printNL2SQLresult"):
        printNL2SQLResult()


    cursor.close()
    mydb.close()


if __name__ == "__main__":
    main()
