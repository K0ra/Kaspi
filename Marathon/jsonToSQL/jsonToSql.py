"""
    The following script is intended to translate a specific
    JSON format string created by my PL/SQL script.
    Since I made a JSON only to the REC_SCREEN table,
    the query is applied only to this table, assuming that other
    tables are filled with data.
    I also created queries for the rest tables but did not
    apply them, because the JSON data of these tables is partial
    being internal compound objects of REC_SCREEN table.

    The main function responsible for parsing is "retrieve_queries".
    Before understanding its logic it is advisable to watch the
    structure of JSON in data1.json.
"""

import cx_Oracle
import json
import config

rclsql = []      # list to store queries to recycler table
transql = []     # list to store queries to transaction table
rec_scrsql = []  # list to store queries to rec_screen table
transaction = {} # temp dict to store json object of transaction table data
recycler = {}    # temp dict to store json object of recycler table data

"""
    Recursive function which parses the JSON object
into INSERT queries for each table, i.e. rec_screen,
transaction, and recycler.
    The queries are stored in lists respective to the table names:
rclsql, transql, rec_scrsql.
"""
def retrieve_queries(table, i, table_name):
    if i < 4:
        keylist = "("
        valuelist = "("
        firstPair = True
        for key, value in table.items():
            if value == "" or value == 0:
                continue
            if not firstPair:
                keylist += ", "
                valuelist += ", "
            firstPair = False
            if key == "transaction":
                transaction = value.copy()
                retrieve_queries(transaction, i + 1, key)
                value = value["id"]
                key = key[:4] # cut the 'transaction' to 'tran'
                key += "_id"  # 'tran_id' is the column name
            if key == "recycler":
                recycler = value.copy()
                retrieve_queries(recycler, i + 1, key)
                value = value["id"]
                key = "rcl_id" # here the column name is assigned explicitly
            keylist += key
            if isinstance(value, str):
                valuelist += "'" + value + "'"
            else:
                valuelist += str(value)
        keylist += ")"
        valuelist += ")"
        stmt = "INSERT INTO " + table_name + " " + keylist + " VALUES " + valuelist
#         print(stmt)
        if table_name == "recycler":
            rclsql.append(stmt)
        elif table_name == "transaction":
            transql.append(stmt)
        elif table_name == "rec_screen":
            rec_scrsql.append(stmt)

def unpack_json(jsonfile):
    """
    Read data from json file.
    Call function that creates INSERT queries from json data
    :param: JSON file name
    """
    with open (jsonfile, 'r') as f:
        jsondata = json.loads(f.read())
    for obj in jsondata:
        retrieve_queries(obj, 1, "rec_screen")

def create_connection():
    """
    Create a database connection to the cx_Oracle database
    specified by config.py (imported)
    :return: Connection object or None
    """
    conn = None
    try:
        conn = cx_Oracle.connect(
            config.username,
            config.password,
            config.dsn,
            encoding=config.encoding)
    except cx_Oracle.Error as error:
        print(error)

    return conn

def create_recscreen(conn):
    """
    Insert data from json into the REC_SCREEN table
    :param: conn:
    """
    cur = conn.cursor()
    for query in rec_scrsql:
        cur.execute(query)
    conn.commit()

def main():
    conn = create_connection()
    unpack_json('data1.json')
    with conn:
        create_recscreen(conn)

if __name__ == '__main__':
    main()
