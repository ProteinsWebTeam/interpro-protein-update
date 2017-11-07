#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

import cx_Oracle


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s: %(levelname)s: %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)


def find_changes(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        cur = con.cursor()

        """
        Report on changes to assignments of signatures to entries (since the last UniProt protein update)  as follows:
        1) All methods that were previously integrated into an InterPro entry that are now deleted from the database
        2) All methods which have moved from one entry to another (reporting the entry_ac at the time of the last update and the entry_ac now)
        3) All methods which have been unintegrated from an interpro entry (i.e. removed completely from entry2method) but are not actually deleted and the last entry they were integrated into.
        4) All methods which are new and have been integrated into the database
        """

        # Changes to assignment of signatures to entries since
        cur.execute("SELECT LOAD_DATE "
                    "FROM INTERPRO.DB_VERSION "
                    "WHERE DBCODE='T'")
        load_date = cur.fetchone()[0].strftime('%Y-%m-%d %H:%M')

        # Deleted signatures
        logging.info('finding deleted signatures')
        cur.execute("SELECT /*+ PARALLEL */ DISTINCT MA.METHOD_AC, EMA.ENTRY_AC "
                    "FROM INTERPRO.METHOD_AUDIT MA, "
                    "     INTERPRO.ENTRY2METHOD_AUDIT EMA "
                    "WHERE EMA.METHOD_AC = MA.METHOD_AC "
                    "AND MA.ACTION = 'D' "
                    "AND EMA.ACTION = 'D' "
                    "AND MA.TIMESTAMP > ("
                    "  SELECT LOAD_DATE "
                    "  FROM INTERPRO.DB_VERSION "
                    "  WHERE DBCODE = 'T'"
                    ") "
                    "AND EMA.TIMESTAMP > ("
                    "  SELECT LOAD_DATE "
                    "  FROM INTERPRO.DB_VERSION "
                    "  WHERE DBCODE = 'T'"
                    ") "
                    "ORDER BY METHOD_AC")
        deleted_methods = [dict(zip(['method', 'last_entry'], row)) for row in cur]

        # Moved signatures
        logging.info('finding moved signatures')
        cur.execute("SELECT /*+ PARALLEL */ EM.METHOD_AC, EMA.ENTRY_AC, EM.ENTRY_AC "
                    "FROM INTERPRO.ENTRY2METHOD_AUDIT EMA, INTERPRO.ENTRY2METHOD EM "
                    "WHERE EMA.METHOD_AC = EM.METHOD_AC "
                    "AND EMA.ENTRY_AC != EM.ENTRY_AC "
                    "AND EMA.TIMESTAMP > ("
                    "  SELECT LOAD_DATE "
                    "  FROM INTERPRO.DB_VERSION "
                    "  WHERE DBCODE = 'T'"
                    ") "
                    "AND EMA.ACTION = 'D' "
                    "AND NOT EXISTS ("
                    "  SELECT 1 "
                    "  FROM INTERPRO.ENTRY2METHOD_AUDIT EMA2 "
                    "  WHERE EMA2.METHOD_AC = EM.METHOD_AC "
                    "  AND EMA.ENTRY_AC != EMA2.ENTRY_AC "
                    "  AND EMA2.TIMESTAMP BETWEEN ("
                    "    SELECT LOAD_DATE "
                    "    FROM INTERPRO.DB_VERSION "
                    "    WHERE DBCODE = 'T'"
                    "  ) AND EMA.TIMESTAMP "
                    "  AND EMA2.ACTION = 'D'"
                    ") "
                    "ORDER BY METHOD_AC")
        moved_methods = [dict(zip(['method', 'original_entry', 'new_entry'], row)) for row in cur]

        # Deintegrated Signatures (Signature has not been deleted from member database)
        logging.info('finding deintegrated signatures')
        cur.execute("SELECT /*+ PARALLEL */ M.METHOD_AC, EMA.ENTRY_AC "
                    "FROM INTERPRO.ENTRY2METHOD_AUDIT EMA, INTERPRO.METHOD M "
                    "WHERE EXISTS ("
                    "  SELECT 1 "
                    "  FROM INTERPRO.METHOD "
                    "  WHERE EMA.METHOD_AC = METHOD_AC"
                    ") "
                    "AND NOT EXISTS ("
                    "  SELECT 1 "
                    "  FROM INTERPRO.ENTRY2METHOD EM "
                    "  WHERE EM.METHOD_AC = M.METHOD_AC"
                    ") "
                    "AND EMA.METHOD_AC = M.METHOD_AC "
                    "AND EMA.ACTION = 'D' "
                    "AND EMA.TIMESTAMP > ("
                    "  SELECT LOAD_DATE "
                    "  FROM INTERPRO.DB_VERSION "
                    "  WHERE DBCODE = 'T'"
                    ") "
                    "ORDER BY METHOD_AC")
        deintegrated_methods = [dict(zip(['method', 'last_entry'], row)) for row in cur]

        # New Signatures
        logging.info('finding new signatures')
        cur.execute("SELECT /*+ PARALLEL */ MA.METHOD_AC, EM.ENTRY_AC, COUNT(MA.PROTEIN_AC) "
                    "FROM INTERPRO.MATCH MA, INTERPRO.PROTEIN P, INTERPRO.ENTRY2METHOD EM "
                    "WHERE P.PROTEIN_AC = MA.PROTEIN_AC "
                    "AND EM.METHOD_AC = MA.METHOD_AC "
                    "AND P.DBCODE = 'T' "
                    "AND NOT EXISTS("
                    "  SELECT 1 "
                    "  FROM INTERPRO.ENTRY2METHOD_AUDIT EMA "
                    "  WHERE EMA.METHOD_AC = EM.METHOD_AC "
                    "  AND EMA.TIMESTAMP <= ("
                    "    SELECT LOAD_DATE "
                    "    FROM INTERPRO.DB_VERSION "
                    "    WHERE DBCODE = 'T'"
                    "  )"
                    ") "
                    "AND EM.TIMESTAMP > ("
                    "  SELECT LOAD_DATE "
                    "  FROM INTERPRO.DB_VERSION "
                    "  WHERE DBCODE = 'T'"
                    ") "
                    "GROUP BY MA.METHOD_AC, EM.ENTRY_AC "
                    "ORDER BY COUNT(MA.PROTEIN_AC) DESC")
        new_methods = [dict(zip(['method', 'entry', 'count'], row)) for row in cur]

    return {
        'date': load_date,
        'deleted': deleted_methods,
        'moved': moved_methods,
        'deintegrated': deintegrated_methods,
        'new': new_methods
    }
