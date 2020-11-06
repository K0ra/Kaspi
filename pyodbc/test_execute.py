"""
Test DML operations on the Kinetica DB
EXECUTE version

DWH_STAGE2.B0015$CX_POLLS_RESULT - table for tests

DELETE removes all data from the table by chunks of data
which size is specified in the batch_size

UPDATE updates all data from the table by chunks of data
which size is specified in the batch_size
"""

import pyodbc
import cx_Oracle
import datetime
import time

# start_time = time.time()

conn = pyodbc.connect('DRIVER={Kinetica ODBC};URL=http://lun-app4o-lt1:9191')
cur = conn.cursor()

# These settings are necessary for SQL operations on VARCHAR columns
conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
conn.setencoding(encoding='utf-8')

# Create table in Kinetica
# create_table = "" \
#          "CREATE TABLE CX_POLLS_RESULT (" \
#              "ROW_ID  VARCHAR(15)," \
#              "CREATED DATETIME, " \
#              "CREATED_BY VARCHAR(15)," \
#              "LAST_UPD DATETIME," \
#              "LAST_UPD_BY VARCHAR(15)," \
#              "MODIFICATION_NUM DECIMAL," \
#              "CONFLICT_ID VARCHAR(15)," \
#              "DB_LAST_UPD DATETIME," \
#              "ACT_ID VARCHAR(15)," \
#              "ANSWER VARCHAR(250)," \
#              "DB_LAST_UPD_SRC VARCHAR(50)," \
#              "QUESTION_CODE VARCHAR(250)" \
#          ")"
# cur.execute(create_table)
# conn.commit()


# Casts the given item to string and maps the string and datetime items into quotes ''
# Remark*: single quotes should be put to every 'string' and 'date' data types
#          here ONLY datetime and string are implemented which is OK for the test table
def cast(item):
    if isinstance(item, datetime.datetime) or isinstance(item, str):
        return "'" + str(item) + "'"

    return str(item)


# Inserts a single row
def insert(cols, values, table_name):
    col_names = ', '.join(cols)
    val_str = ', '.join([cast(i) for i in values])
    sql_stmt = "INSERT INTO " + table_name + " (" + col_names + ") VALUES (" + val_str + ")"
    # print(sql_stmt)
    cur.execute(sql_stmt)
    cur.commit()


# Updates a single row
def update(par, condition, table_name):
    param = par[0]
    new_val = cast(par[1])
    condition_col = condition[0]
    condition_val = cast(condition[1])
    sql_stmt = "UPDATE " + table_name + " SET " + param + "=" + \
               new_val + " WHERE " + condition_col + "=" + condition_val
    # print(sql_stmt)
    cur.execute(sql_stmt)
    cur.commit()


# Deletes a single row
def delete(id_name, value, table_name):
    value = cast(value)
    sql_stmt = "DELETE FROM " + table_name + " WHERE " + id_name + "=" + value
    cur.execute(sql_stmt)
    cur.commit()


def main(batch_size=10):
    # Connect to Oracle DB
    ora_user = 'DWH_PYTHON'
    ora_password = 'l%^5q03YLhh2a%n5X'
    ora_host = 'kaspi01p1'

    sql = 'select t.* from DWH_STAGE2.B0015$CX_POLLS_RESULT t'
    con0000 = cx_Oracle.connect(ora_user, ora_password, ora_host)  # connect to dwh
    cur0000 = con0000.cursor()
    cur0000.execute(sql)

    # Useful variables
    cols = []   # column_names
    # param and condition are needed for UPDATE operation
    # they store two items: column name and column value
    param = ['QUESTION_CODE', 'KSP KNOW CLIENT']
    # second item (value of ROW_ID) is changing while reading input by one row
    condition = ['ROW_ID']

    # Temporary variable
    idx = 0

    while True:
        # idx += 1
        rows = cur0000.fetchmany(batch_size)
        if not rows:
            break

        cols = [x[0] for x in cur0000.description]

        start_time = time.time()        # Measuring the running time to process insert/delete/update
        for row in rows:
            """!!! The methods provided below should called to check DML operations,
                       uncomment the necessary method, comment the unnecessary method"""
            # ----- TEST INSERT BY ONE VALUE ------
            insert(cols, row, 'CX_POLLS_RESULT')
            # ----- TEST UPDATE BY ONE VALUE ------
            # condition.append(row[0])  # Appending current ROW_ID
            # update(param, condition, 'CX_POLLS_RESULT')
            # condition = condition[:-1]  # Removing current ROW_ID value to update it
                                          # with another ROW_ID in the next loop
            # ----- TEST DELETE BY ONE VALUE ------
            # delete('ROW_ID', row[0], 'CX_POLLS_RESULT')  # Deleting current ROW_ID
        print("--- %s seconds ---" % (time.time() - start_time))

    cur0000.close()
    con0000.close()


if __name__ == '__main__':
    # Provide the batch_size
    main(batch_size=100000)

    # Close connections
    cur.close()
    conn.close()
