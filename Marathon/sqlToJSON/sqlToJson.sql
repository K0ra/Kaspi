ALTER SESSION SET NLS_LANGUAGE = ENGLISH;
SET SERVEROUTPUT ON

CREATE OR REPLACE PACKAGE rec_screen_json IS
    json_data           CLOB;     -- string to store json data
                                        -- of the whole REC_SCREEN table
    c_date_format       CONSTANT VARCHAR2(20) := '''DD-MON-YYYY''';
    -- Datetime format is specifically for TRANSACTION table
    c_datetime_format   CONSTANT VARCHAR2(30) := '''DD-MON-YYYY,HH24:MI:SS''';
    FUNCTION get_recscr   (
        p_recscr_id    rec_screen.id%TYPE
    ) RETURN CLOB;
    FUNCTION get_transaction (
        p_tran_id   transaction.id%TYPE
    ) RETURN CLOB;
    FUNCTION get_recycler (
        p_rcl_id   recycler.id%TYPE
    ) RETURN CLOB;
    PROCEDURE serialize_table;
    v_recscr_rec  rec_screen%ROWTYPE;
    v_tran_rec    transaction%ROWTYPE;
    v_rcl_rec   recycler%ROWTYPE;
END rec_screen_json;
/

CREATE OR REPLACE PACKAGE BODY rec_screen_json IS
-- Encloses the string in quotes
    FUNCTION enclose (
        p_str   VARCHAR2
    ) RETURN VARCHAR2 IS
        v_str VARCHAR2(100);
    BEGIN
        v_str := concat(concat('"', to_char(p_str)), '"');
        RETURN v_str;
    END;

    PROCEDURE execute_stmt (
        p_stmt  IN      VARCHAR2,
        p_data  IN OUT  CLOB
    ) IS
    BEGIN
        EXECUTE IMMEDIATE p_stmt
        USING OUT p_data;
    END execute_stmt;

-- Returns a json string for a single row of RECYCLER
-- for a specified recycler id
    FUNCTION get_recycler (
        p_rcl_id   recycler.id%TYPE
    ) RETURN CLOB IS
        v_final_data    CLOB;
        v_tmp_data      CLOB;
        v_stmt          VARCHAR2(200);
        CURSOR c_rcl_cursor IS
            SELECT * FROM recycler
            WHERE id = p_rcl_id;
    BEGIN
        v_final_data := '{ ';
        OPEN c_rcl_cursor;
        FETCH c_rcl_cursor INTO rec_screen_json.v_rcl_rec;
        FOR rec IN ( SELECT column_name, data_type
                     FROM all_tab_cols
                     WHERE table_name = 'RECYCLER' )
        LOOP
            IF ( rec.data_type = 'DATE' ) THEN
                v_stmt := 'begin
                :rec := enclose(TO_CHAR(rec_screen_json.v_rcl_rec.'
                || rec.column_name || ', ' || c_date_format || '));
                end;';
            ELSIF ( rec.data_type IN ('VARCHAR2', 'CHAR', 'NCHAR',
            'NVARCHAR2', 'CLOB') ) THEN
                v_stmt := 'begin
                :rec := enclose(rec_screen_json.v_rcl_rec.' ||
                rec.column_name || ');
                end;';
            ELSE  -- assuming NUMBER
                v_stmt := 'begin
                :rec := TO_CHAR(NVL(rec_screen_json.v_rcl_rec.' ||
                rec.column_name || ', 0));
                end;';
            END IF;
            execute_stmt(v_stmt, v_tmp_data);
            v_final_data := CONCAT(v_final_data, enclose(LOWER(rec.column_name))
            || ': ' || v_tmp_data || ', ');
        END LOOP;
        CLOSE c_rcl_cursor;
        -- Removing the redundant comma ', ' for the last input
        v_final_data := SUBSTR(v_final_data, 1, LENGTH(v_final_data) - 2);
        v_final_data := v_final_data || ' }';
        RETURN v_final_data;
    END get_recycler;

-- Returns a json string for a single row of TRANSACTION
-- for a specified transaction id
    FUNCTION get_transaction (
        p_tran_id   transaction.id%TYPE
    ) RETURN CLOB IS
        v_final_data    CLOB;
        v_tmp_data      CLOB;
        v_stmt          VARCHAR2(200);
        CURSOR c_tran_cursor IS
            SELECT * FROM transaction
            WHERE id = p_tran_id;
    BEGIN
        v_final_data := '{ ';
        OPEN c_tran_cursor;
        FETCH c_tran_cursor INTO rec_screen_json.v_tran_rec;
        FOR rec IN ( SELECT column_name, data_type
                     FROM all_tab_cols
                     WHERE table_name = 'TRANSACTION' )
        LOOP
      IF ( rec.column_name = 'RCL_ID' ) THEN
                v_stmt := 'begin
                :rec := rec_screen_json.get_recycler(rec_screen_json.v_tran_rec.' ||
                rec.column_name || ');
                end;';
                execute_stmt(v_stmt, v_tmp_data);
                v_final_data := CONCAT(v_final_data, '"recycler": ' ||
                                v_tmp_data || ', ');
                CONTINUE;
            END IF;
            IF ( rec.data_type = 'DATE' ) THEN
                v_stmt := 'begin
                :rec := enclose(TO_CHAR(rec_screen_json.v_tran_rec.' ||
                rec.column_name || ', ' || c_datetime_format || '));
                end;';
            ELSIF ( rec.data_type IN ('VARCHAR2', 'CHAR', 'NCHAR',
            'NVARCHAR2', 'CLOB') ) THEN
                v_stmt := 'begin
                :rec := enclose(rec_screen_json.v_tran_rec.' ||
                rec.column_name || ');
                end;';
            ELSE  -- assuming NUMBER
                v_stmt := 'begin
                :rec := TO_CHAR(NVL(rec_screen_json.v_tran_rec.' ||
                rec.column_name || ', 0));
                end;';
            END IF;
            execute_stmt(v_stmt, v_tmp_data);
            v_final_data := CONCAT(v_final_data, enclose(LOWER(rec.column_name))
            || ': ' || v_tmp_data || ', ');
        END LOOP;
        CLOSE c_tran_cursor;
        -- Removing the redundant comma ', ' for the last input
        v_final_data := SUBSTR(v_final_data, 1, LENGTH(v_final_data) - 2);
        v_final_data := v_final_data || ' }';
        RETURN v_final_data;
    END get_transaction;

-- Returns a json string for a single row of REC_SCREEN
-- for a specified department id
    FUNCTION get_recscr (
        p_recscr_id    rec_screen.id%TYPE
    ) RETURN CLOB IS
        v_final_data    CLOB;
        v_tmp_data      CLOB;
        v_stmt          VARCHAR2(200);
        CURSOR c_recscr_cursor IS
            SELECT * FROM rec_screen
            WHERE id = p_recscr_id;
    BEGIN
        v_final_data := '{ ';
        OPEN c_recscr_cursor;
        FETCH c_recscr_cursor into rec_screen_json.v_recscr_rec;
        FOR rec IN ( SELECT column_name, data_type
                     FROM all_tab_cols
                     WHERE table_name = 'REC_SCREEN' )
        LOOP
            IF ( rec.column_name = 'TRAN_ID' ) THEN
                v_stmt := 'begin
                :rec := rec_screen_json.get_transaction(rec_screen_json.v_recscr_rec.' ||
                rec.column_name || ');
                end;';
                execute_stmt(v_stmt, v_tmp_data);
                v_final_data := CONCAT(v_final_data, '"transaction": ' ||
                                v_tmp_data || ', ');
                CONTINUE;
            END IF;
            IF ( rec.data_type = 'DATE' ) THEN
                v_stmt := 'begin
                :rec := enclose(TO_CHAR(rec_screen_json.v_recscr_rec.' ||
                rec.column_name || ', ' || c_date_format || '));
                end;';
            ELSIF ( rec.data_type IN ('VARCHAR2', 'CHAR', 'NCHAR',
            'NVARCHAR2', 'CLOB') ) THEN
                v_stmt := 'begin
                :rec := enclose(rec_screen_json.v_recscr_rec.' ||
                rec.column_name || ');
                end;';
            ELSE  -- assuming NUMBER
                v_stmt := 'begin
                :rec := TO_CHAR(NVL(rec_screen_json.v_recscr_rec.' ||
                rec.column_name || ', 0));
                end;';
            END IF;
            execute_stmt(v_stmt, v_tmp_data);
            v_final_data := CONCAT(v_final_data, enclose(LOWER(rec.column_name))
            || ': ' || v_tmp_data || ', ');
        END LOOP;
        CLOSE c_recscr_cursor;
  -- Removing the comma ', ' for the last input
        v_final_data := SUBSTR(v_final_data, 1, LENGTH(v_final_data) - 2);
        v_final_data := v_final_data || ' }';
        RETURN v_final_data;
    END get_recscr;

-- Serializes the whole table into JSON format
    PROCEDURE serialize_table IS
    BEGIN
        json_data := '[ ';
        FOR rcl_rec IN ( SELECT * FROM rec_screen )
        LOOP
            json_data := CONCAT(json_data, get_recscr(rcl_rec.id));
            json_data := CONCAT(json_data, ', ');
        END LOOP;
        json_data := SUBSTR(json_data, 1, LENGTH(json_data) - 2);
        json_data := json_data || ' ]';
        dbms_output.put_line(json_data);
    END serialize_table;
END rec_screen_json;
/

EXECUTE rec_screen_json.serialize_table();
