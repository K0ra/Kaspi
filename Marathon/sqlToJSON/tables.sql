CREATE TABLE recycler (
    id NUMBER(6)
        CONSTRAINT rcl_rclid_pk PRIMARY KEY,
    rcl_number VARCHAR2(100),
    city VARCHAR2(100),
    office VARCHAR2(100),
    start_date DATE DEFAULT SYSDATE
);

CREATE TABLE transaction (
    id NUMBER(6)
        CONSTRAINT tran_id_pk PRIMARY KEY,
    start_date DATE DEFAULT SYSDATE,
    end_date DATE DEFAULT SYSDATE,
    amount NUMBER(7),
    client_id NUMBER(6),
    rcl_id NUMBER(6),
    process VARCHAR2(100),
    product VARCHAR2(100),
    CONSTRAINT trans_rcl_fk FOREIGN KEY (rcl_id)
        REFERENCES recycler(id)
);

CREATE TABLE rec_screen(
    id NUMBER(6)
        CONSTRAINT recscr_id_pk PRIMARY KEY,
    screen_name VARCHAR2(100),
    process VARCHAR2(100),
    tran_id NUMBER(6),
    dur NUMBER(4),
    CONSTRAINT recscr_tran_fk FOREIGN KEY (tran_id)
        REFERENCES transaction(id)
);

COMMIT;
