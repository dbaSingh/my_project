CREATE TABLE EMACH.CLN_TABLE_CFG
(
  OWNER           VARCHAR2(30 BYTE)             NOT NULL,
  TABLE_NAME      VARCHAR2(30 BYTE)             NOT NULL,
  PARTITIONED     VARCHAR2(3 BYTE)              DEFAULT ('N')                 NOT NULL,
  STATUS          CHAR(1 BYTE)                  DEFAULT ('I')                 NOT NULL,
  RETENTION_MTHS  NUMBER                        DEFAULT NULL,
  CLNUP_METHOD    NUMBER                        DEFAULT NULL
)
TABLESPACE USERS
LOGGING 
NOCOMPRESS 
NOCACHE
NOPARALLEL;

COMMENT ON TABLE EMACH.CLN_TABLE_CFG IS 'AUTOMATIC CLEANUP Config Table

This table contains one entry for any table in the database and retention policy';

COMMENT ON COLUMN EMACH.CLN_TABLE_CFG.OWNER IS 'Table Name';

COMMENT ON COLUMN EMACH.CLN_TABLE_CFG.PARTITIONED IS 'Partition Status YES/NO';

COMMENT ON COLUMN EMACH.CLN_TABLE_CFG.STATUS IS 'CLEANUP status
A : ACTIVE
I : INACTIVE - Default value';

COMMENT ON COLUMN EMACH.CLN_TABLE_CFG.RETENTION_MTHS IS 'Number of Months to be KEPT';

COMMENT ON COLUMN EMACH.CLN_TABLE_CFG.CLNUP_METHOD IS 'Cleanup Methods 
1 : DELETE FROM 
2 : ALTER TABLE DROP PARTITION
3 : ALTER TABLE DROP SUBPARTITION
4 : TRUNCATE TABLE ';


