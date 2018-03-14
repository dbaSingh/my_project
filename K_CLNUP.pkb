CREATE OR REPLACE PACKAGE BODY EMACH.K_CLNUP
/**
 * $Id: EMACH.K_CLNUP.SQL 2017/12/18 14:56:48 DBA
 *
 * Any use, copying, modification, distribution and selling of this software
 * and its documentation for any purposes without written permission
 * is hereby prohibited.

 * A package that contains function and procedures for cleanup handling.<br>
 *
 * Last updated: $Date: 2004/10/06 14:56:48 $
 * @author DBA 
 * @version.$Revision: 1.0
 */
IS
    /**
    * Generates Partitions on Interval partitioned table.
    * Usage eg exec EMACH.K_CLNUP.add_mnth_partitions ('EMACH','FHD_HP_ALARM_DET_T_PAR',5);
    * Will add 5 new monthly partitions after last existing one
    */
    PROCEDURE add_mnth_partitions (tabown     IN VARCHAR2 := NULL,
                                   tabname    IN VARCHAR2 := NULL,
                                   part_cnt   IN PLS_INTEGER := 0)
    IS
        V_HIGH_VALUE   TIMESTAMP;
        COUNTER        PLS_INTEGER;
        HIGH_COUNTER   PLS_INTEGER;
        v_dyn_sql      VARCHAR2 (50);
    BEGIN
        COUNTER := 1;

        SELECT TO_DATE (
                   TRIM (
                       '''' FROM REGEXP_SUBSTR (
                                     EXTRACTVALUE (
                                         DBMS_XMLGEN.getxmltype (
                                                'select high_value from all_tab_partitions where table_name='''
                                             || table_name
                                             || ''' and table_owner = '''
                                             || table_owner
                                             || ''' and partition_name = '''
                                             || partition_name
                                             || ''''),
                                         '//text()'),
                                     '''.*?''')),
                   'syyyy-mm-dd hh24:mi:ss')
          INTO V_HIGH_VALUE
          FROM all_tab_partitions
         WHERE     table_name = tabname
               AND table_owner = tabown
               AND partition_position =
                   (SELECT MAX (partition_position)
                      FROM all_tab_partitions
                     WHERE table_name = tabname AND table_owner = tabown);

        IF part_cnt = 0
        THEN
            SELECT MONTHS_BETWEEN (TO_DATE (SYSDATE),
                                   TO_DATE (ADD_MONTHS (V_HIGH_VALUE, 0)))
              INTO high_counter
              FROM DUAL;
        ELSE
            high_counter := part_cnt;
        END IF;

        DBMS_OUTPUT.PUT_LINE (
               '-- adding partitions '
            || COUNTER
            || ' .. '
            || HIGH_COUNTER
            || ' , current high_value is '' '
            || V_HIGH_VALUE
            || ' '' ');


        DBMS_OUTPUT.PUT_LINE (
               'ALTER TABLE '
            || tabown
            || '.'
            || tabname
            || '  SET INTERVAL( NUMTOYMINTERVAL(1,''MONTH''));');

        EXECUTE IMMEDIATE
               'ALTER TABLE '
            || tabown
            || '.'
            || tabname
            || '  SET INTERVAL( NUMTOYMINTERVAL(1,''MONTH''))';

        DBMS_OUTPUT.PUT_LINE (
               'ALTER TABLE '
            || tabown
            || '.'
            || tabname
            || '  SET INTERVAL ( );');

        EXECUTE IMMEDIATE
            'ALTER TABLE ' || tabown || '.' || tabname || '  SET INTERVAL( )';


        FOR V_PART IN counter .. high_counter
        LOOP
            DBMS_OUTPUT.PUT_LINE (
                   'ALTER TABLE '
                || tabown
                || '.'
                || tabname
                || ' ADD PARTITION VALUES LESS THAN (TO_DATE('' '
                || TO_CHAR (ADD_MONTHS (V_HIGH_VALUE, V_PART),
                            'SYYYY-MM-DD HH24:MI:SS')
                || ''', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN''));');

            EXECUTE IMMEDIATE
                   'ALTER TABLE '
                || tabown
                || '.'
                || tabname
                || ' ADD PARTITION VALUES LESS THAN (TO_DATE('' '
                || TO_CHAR (ADD_MONTHS (V_HIGH_VALUE, V_PART),
                            'SYYYY-MM-DD HH24:MI:SS')
                || ''', ''SYYYY-MM-DD HH24:MI:SS'', ''NLS_CALENDAR=GREGORIAN''))';
        END LOOP;

        DBMS_OUTPUT.PUT_LINE (
               'ALTER TABLE '
            || tabown
            || '.'
            || tabname
            || '  SET INTERVAL( NUMTOYMINTERVAL(1,''MONTH''));');

        EXECUTE IMMEDIATE
               'ALTER TABLE '
            || tabown
            || '.'
            || tabname
            || '  SET INTERVAL( NUMTOYMINTERVAL(1,''MONTH''))';
    END add_mnth_partitions;

    /**
    * DROPS Partitions
    * usage drp_mnth_partitions(table_owner,table_name,retention time in months)
    * usage eg exec EMACH.K_CLNUP.drp_mnth_partitions ('EMACH','FHD_HP_ALARM_DET_T_PAR',6);
    * Will drop all exception last 6 partitions before today .
    */
    PROCEDURE drp_mnth_partitions (tabown    IN VARCHAR2 := NULL,
                                   tabname   IN VARCHAR2 := NULL,
                                   retent    IN PLS_INTEGER := 0)
    IS
        CURSOR V_PARTITIONS
        IS
            SELECT PARTITION_NAME, HIGH_VALUE
              FROM DBA_TAB_PARTITIONS
             WHERE TABLE_NAME = tabname AND TABLE_OWNER = tabown;


        V_HIGH_VALUE           TIMESTAMP;
        LAST_PARTITION         EXCEPTION;

        PRAGMA EXCEPTION_INIT (LAST_PARTITION, -14758);
        log_section   CONSTANT VARCHAR2 (28) := 'K_CLN.DRP_MNTH_PARTITIONS';
        pCTX                   PLOG.LOG_CTX;               -- logging context;
    BEGIN
        pCTX := PLOG.init (pSECTION => log_section, pLEVEL => PLOG.LDEBUG);


        IF retent <= 0
        THEN
            DBMS_OUTPUT.PUT_LINE (
                   'Retention set to 0 months - which will drop all partitions - EXECUTION STOPPED for '
                || tabown
                || '.'
                || tabname);
            plog.DEBUG (
                pCTX,
                   'Retention set to 0 months - which will drop all partitions - EXECUTION STOPPED for '
                || tabown
                || '.'
                || tabname);
        ELSE
            DBMS_OUTPUT.PUT_LINE (
                   'Retention set to '
                || retent
                || ' months - which will drop all partitions before '
                || TO_CHAR (
                         (LAST_DAY (
                              TRUNC (ADD_MONTHS (SYSDATE, -1 - retent))))
                       + 0.99999)
                || ' for '
                || tabown
                || '.'
                || tabname);
            plog.DEBUG (
                pCTX,
                   'Retention set to '
                || retent
                || ' months - which will drop all partitions before '
                || TO_CHAR (
                         (LAST_DAY (
                              TRUNC (ADD_MONTHS (SYSDATE, -1 - retent))))
                       + 0.99999)
                || ' for '
                || tabown
                || '.'
                || tabname);

            FOR V_PART IN V_PARTITIONS
            LOOP
                EXECUTE IMMEDIATE
                    'BEGIN :1 := ' || V_PART.HIGH_VALUE || '; END;'
                    USING OUT V_HIGH_VALUE;

                IF V_HIGH_VALUE <=
                     LAST_DAY (TRUNC (ADD_MONTHS (SYSDATE, -1 - retent)))
                   + 0.99999
                THEN
                    BEGIN
                        EXECUTE IMMEDIATE
                               'ALTER TABLE '
                            || tabown
                            || '.'
                            || tabname
                            || ' DROP PARTITION '
                            || V_PART.PARTITION_NAME;

                        DBMS_OUTPUT.PUT_LINE (
                               'ALTER TABLE '
                            || tabown
                            || '.'
                            || tabname
                            || ' DROP PARTITION '
                            || V_PART.PARTITION_NAME
                            || ';');
                        plog.DEBUG (
                            pCTX,
                               'ALTER TABLE '
                            || tabown
                            || '.'
                            || tabname
                            || ' DROP PARTITION '
                            || V_PART.PARTITION_NAME);
                    EXCEPTION
                        WHEN LAST_PARTITION
                        THEN
                            --EXECUTE IMMEDIATE 'ALTER TABLE ' || tabown|| '.'|| tabname|| ' TRUNCATE PARTITION ' || V_PART.PARTITION_NAME || ' DROP ALL STORAGE';
                            DBMS_OUTPUT.PUT_LINE (
                                   'ALTER TABLE '
                                || tabown
                                || '.'
                                || tabname
                                || ' TRUNCATE PARTITION '
                                || V_PART.PARTITION_NAME
                                || ' DROP ALL STORAGE;');
                            plog.DEBUG (
                                pCTX,
                                   'ALTER TABLE '
                                || tabown
                                || '.'
                                || tabname
                                || ' TRUNCATE PARTITION '
                                || V_PART.PARTITION_NAME
                                || ' DROP ALL STORAGE;');
                        WHEN OTHERS
                        THEN
                            plog.DEBUG (
                                pCTX,
                                   'ORA '
                                || SQLCODE
                                || ' ERROR for ALTER TABLE '
                                || tabown
                                || '.'
                                || tabname
                                || ' DROP PARTITION '
                                || V_PART.PARTITION_NAME);



                            IF SQLCODE = -54
                            THEN
                                NULL;
                            ELSE
                                RAISE;
                            END IF;
                    END;
                END IF;
            END LOOP;
        END IF;
    END drp_mnth_partitions;

    /**
    * Read table CLN_TABLE_CFG to list active tables for cleanup
    * exec  EMACH.K_CLNUP.monthly_tables;
    *
    */
    PROCEDURE monthly_tables
    IS
        CURSOR V_TABLES
        IS
            SELECT OWNER,
                   TABLE_NAME,
                   RETENTION_MTHS,
                   CLNUP_METHOD
              FROM EMACH.CLN_TABLE_CFG
             WHERE PARTITIONED = 'YES' AND STATUS = 'A';

        V_CLNUP_DESC    VARCHAR2 (30);
        V_CLUP_PKG      VARCHAR2 (50);
        V_CNT_METHOD    NUMBER;
        v_sqlstring     VARCHAR2 (4000);
        v_plsql_block   VARCHAR2 (500);
    BEGIN
        FOR V_TAB IN V_TABLES
        LOOP
            --Check Method is defined correctly
            v_plsql_block := ' ';

            SELECT COUNT (*)
              INTO v_cnt_method
              FROM EMACH.CLN_METHOD_CFG
             WHERE CLNUP_METHOD = V_TAB.CLNUP_METHOD;

            IF v_cnt_method <> 1
            THEN
                DBMS_OUTPUT.PUT_LINE (' NO CLEANUP METHOD ');
            ELSE
                SELECT NVL (CLNUP_DESC, '-- No Method Defined'),
                       NVL (CLNUP_PKG, ' -- NO PACKAGE ')
                  INTO V_CLNUP_DESC, V_CLUP_PKG
                  FROM EMACH.CLN_METHOD_CFG
                 WHERE CLNUP_METHOD = V_TAB.CLNUP_METHOD;


                DBMS_OUTPUT.PUT_LINE (
                       ' CLEANUP METHOD: '
                    || V_TAB.CLNUP_METHOD
                    || ' using '
                    || V_CLUP_PKG);
                DBMS_OUTPUT.PUT_LINE (
                       ' ON '
                    || V_TAB.OWNER
                    || '.'
                    || V_TAB.TABLE_NAME
                    || ' keeping data for last '
                    || V_TAB.RETENTION_MTHS
                    || ' months.');


                DBMS_OUTPUT.PUT_LINE (' EXECUTE IMMEDIATE ' || v_sqlstring);

                v_plsql_block :=
                    'BEGIN ' || V_CLUP_PKG || '(:cown,:ctab, :ret); END;';

                EXECUTE IMMEDIATE v_plsql_block
                    USING V_TAB.OWNER, V_TAB.TABLE_NAME, V_TAB.RETENTION_MTHS;
            END IF;
        END LOOP;
    END monthly_tables;
END K_CLNUP;
/


