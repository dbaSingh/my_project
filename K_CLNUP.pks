CREATE OR REPLACE PACKAGE EMACH.K_CLNUP
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
                                   part_cnt   IN PLS_INTEGER := 0);


    /**
    * DROPS Partitions
    * usage drp_mnth_partitions(table_owner,table_name,retention time in months)
    * usage eg exec EMACH.K_CLNUP.drp_mnth_partitions ('EMACH','FHD_HP_ALARM_DET_T_PAR',6);
    * Will drop all exception last 6 partitions before today .
    */
    PROCEDURE drp_mnth_partitions (tabown    IN VARCHAR2 := NULL,
                                   tabname   IN VARCHAR2 := NULL,
                                   retent    IN PLS_INTEGER := 0);

    /**
    * Read table CLN_TABLE_CFG to list active tables for cleanup
    * exec  EMACH.K_CLNUP.monthly_tables;
    *
    */
    PROCEDURE monthly_tables;
END K_CLNUP;
/


