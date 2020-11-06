"""
Test DML operations on the Kinetica DB
EXECUTEMANY version

DWH_STAGE2.B0015$CX_POLLS_RESULT - table for tests

DELETE removes all data from the table by chunks of data
which size is specified in the batch_size

UPDATE updates all data from the table by chunks of data
which size is specified in the batch_size
"""


import pyodbc
import cx_Oracle
import time
import statistics

conn = pyodbc.connect('DRIVER={Kinetica ODBC};URL=http://lun-app4o-lt1:9191')
cur = conn.cursor()

# These settings are necessary for SQL operations on VARCHAR columns
conn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
conn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
conn.setencoding(encoding='utf-8')


def insert_batch(cols, rows, table_name):
    col_names = ', '.join(cols)
    cols_num = len(cols)
    val_params = "?, " * cols_num
    val_params = val_params[:-2]    # remove redundant ',' and space
    sql_stmt = "INSERT INTO " + table_name + "(" + col_names + ") VALUES (" + val_params + ")"
    # print(sql_stmt)
    cur.fast_executemany = True     # does not seem to improve the speed
    cur.executemany(sql_stmt, rows)
    cur.commit()


# def check_insert(table_name):
#     sql_stmt = "SELECT t.* FROM " + table_name + " t"
#     cur.execute(sql_stmt)
#     res = cur.fetchall()
#     print("Inserted data:")
#     print(res)


def delete_batch(id_name, id_list, table_name):
    sql_stmt = "DELETE FROM " + table_name + " WHERE " + id_name + "=?"
    # cur.fast_executemany = True
    cur.executemany(sql_stmt, id_list)
    cur.commit()


def update_batch(par_col, condition_col, param, table_name):
    sql_stmt = "UPDATE " + table_name + " SET " + par_col + "=? WHERE " \
               + condition_col + "=?"
    # cur.fast_executemany = True
    cur.executemany(sql_stmt, param)
    cur.commit()


def main(batch_size=100):
    # Connect to Oracle DB
    ora_user = 'DWH_PYTHON'
    ora_password = 'l%^5q03YLhh2a%n5X'
    ora_host = 'kaspi01p1'

    sql = 'select t.* from DWH_STAGE2.B0015$CX_POLLS_RESULT t'
    con0000 = cx_Oracle.connect(ora_user, ora_password, ora_host)
    cur0000 = con0000.cursor()
    cur0000.execute(sql)

    # cur.execute("TRUNCATE TABLE CX_POLLS_RESULT")
    # cur.commit()

    # Temporary variable
    idx = 0
    time_list = []

    while True:
        # idx += 1
        rows = cur0000.fetchmany(batch_size)
        if not rows:
            break

        cols = [x[0] for x in cur0000.description]
        del_ids = []    # list of tuples consisting of the value for the DELETE's WHERE clause
        upd_ids = []    # list of tuples consisting of two values - value for the UPDATE's SET clause
                        # and the value for the UPDATE's WHERE clause

        start_time = time.time()  # Measuring the running time to process insert/delete/update
        for row in rows:
            # upd_tup = ('KSP KNOW CLIENT',)      # the fixed value set to the 'QUESTION_CODE' column
            for i, val in enumerate(row):
                if cur0000.description[i][0] == 'ROW_ID':
                    del_ids.append(tuple([val]))
                    # upd_tup += tuple([val])
            # upd_ids.append(upd_tup)

        """!!! These methods should called to check DML operations,
           uncomment the necessary method, comment the unnecessary method"""
        # insert_batch(cols, rows, 'CX_POLLS_RESULT')
        delete_batch('ROW_ID', del_ids, 'CX_POLLS_RESULT')
        # update_batch('QUESTION_CODE', 'ROW_ID', upd_ids, 'CX_POLLS_RESULT')

        time_list.append(time.time() - start_time)
        print("--- %s seconds ---" % time_list[-1])

    # Output the median time execution of <batch_size> records
    print("--- Median time: %s seconds ---" % statistics.median(time_list))
    cur0000.close()
    con0000.close()


if __name__ == '__main__':
    # Provide the batch_size
    main(1000)

    # Close connections
    cur.close()
    conn.close()