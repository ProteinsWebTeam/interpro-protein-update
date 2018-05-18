#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
import os

import cx_Oracle

from . import utils


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s: %(levelname)s: %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)


def refresh_uniparc(user, passwd, db, useproc=True):
    with cx_Oracle.connect(user, passwd, db) as con:
        cur = con.cursor()

        cur.execute('SELECT MAX(UPI) FROM UNIPARC.XREF')
        max_upi_before = cur.fetchone()[0]

        logging.info('max UPI: {}'.format(max_upi_before))

        if useproc:
            logging.info('refreshing UNIPARC.XREF')
            cur.callproc('UNIPARC.REFRESH_XREF')
        else:
            logging.info('dropping UNIPARC.XREF_OLD')
            try:
                cur.execute('DROP TABLE UNIPARC.XREF_OLD')
            except cx_Oracle.DatabaseError:
                pass  # Prevent ORA-00942 (table or view does not exist) to be raised

            logging.info('recreating UNIPARC.XREF_NEW')
            cur.execute('CREATE TABLE xref_new TABLESPACE uniparc_tab AS SELECT upi, ac, dbid, deleted, version FROM UNIPARC.xref@UAREAD')

            logging.info('indexing AC column')
            cur.execute('CREATE INDEX xref_ac_new ON xref_new(ac) TABLESPACE uniparc_in')

            logging.info('indexing AC column (upper cases)')
            cur.execute('CREATE INDEX xref_upper_ac_new ON xref_new(UPPER(ac)) TABLESPACE uniparc_ind')

            logging.info('indexing UPI column')
            cur.execute('CREATE INDEX xref_upi_new ON xref_new(upi) TABLESPACE uniparc_ind')

            logging.info('renaming indexes (1/2)')
            cur.execute('ALTER INDEX xref_ac RENAME TO xref_ac_old')
            cur.execute('ALTER INDEX xref_upper_ac RENAME TO xref_upper_ac_old')
            cur.execute('ALTER INDEX xref_upi RENAME TO xref_upi_old')

            logging.info('renaming tables')
            cur.execute('ALTER TABLE xref RENAME TO xref_old')
            cur.execute('ALTER TABLE xref_new RENAME TO xref')

            logging.info('renaming indexes (2/2)')
            cur.execute('ALTER INDEX xref_ac_new RENAME TO xref_ac')
            cur.execute('ALTER INDEX xref_upper_ac_new RENAME TO xref_upper_ac')
            cur.execute('ALTER INDEX xref_upi_new RENAME TO xref_upi')

            logging.info('granting privileges')
            cur.execute('GRANT SELECT ON UNIPARC.XREF TO PUBLIC')

            logging.info('dropping UNIPARC.CV_DATABASE')
            try:
                cur.execute('DROP TABLE UNIPARC.CV_DATABASE')
            except cx_Oracle.DatabaseError:
                pass  # Prevent ORA-00942 (table or view does not exist) to be raised

            logging.info('recreating UNIPARC.CV_DATABASE')
            cur.execute('CREATE TABLE UNIPARC.CV_DATABASE TABLESPACE UNIPARC_TAB AS SELECT * FROM UNIPARC.CV_DATABASE@UAREAD')

            logging.info('creating unique indexes')
            cur.execute('CREATE UNIQUE INDEX PK_CV_DATABASE ON UNIPARC.CV_DATABASE (ID) TABLESPACE UNIPARC_IND')

            logging.info('creating unique index on DESCR column')
            cur.execute('CREATE UNIQUE INDEX UQ_CV_DATABASE$DESCR ON UNIPARC.CV_DATABASE (DESCR) TABLESPACE UNIPARC_IND')

            logging.info('adding primary key constraint')
            cur.execute('ALTER TABLE UNIPARC.CV_DATABASE ADD (CONSTRAINT PK_CV_DATABASE PRIMARY KEY(ID) USING INDEX PK_CV_DATABASE, CONSTRAINT UQ_CV_DATABASE$DESCR UNIQUE (DESCR) USING INDEX UQ_CV_DATABASE$DESCR)')

            logging.info('granting privileges')
            cur.execute('GRANT SELECT ON UNIPARC.CV_DATABASE TO PUBLIC')

            logging.info('committing changes')
            con.commit()

        cur.execute('SELECT MAX(UPI) FROM UNIPARC.XREF')
        max_upi_after = cur.fetchone()[0]
        logging.info('new max UPI: {}'.format(max_upi_after))

    return max_upi_before, max_upi_after


def refresh_method2swiss(user, passwd, db):
    logging.info('refreshing METHOD2SWISS_DE')
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        # Old Happy Helper schema
        cur.execute('TRUNCATE TABLE INTERPRO.METHOD2SWISS_DE')
        cur.execute("INSERT INTO INTERPRO.METHOD2SWISS_DE "
                    "SELECT "
                    "  F2P.SEQ_ID AS PROTEIN_AC, "
                    "  PDV.DESCRIPTION, "
                    "  F2P.FEATURE_ID AS METHOD_AC, "
                    "  'before' AS STATUS "
                    "FROM "
                    "  INTERPRO_ANALYSIS_LOAD.FEATURE2PROTEIN F2P, "
                    "  INTERPRO_ANALYSIS_LOAD.FEATURE_SUMMARY FS, "
                    "  INTERPRO_ANALYSIS_LOAD.PROTEIN_DESCRIPTION_CODE PDC, "
                    "  INTERPRO_ANALYSIS_LOAD.PROTEIN_DESCRIPTION_VALUE PDV "
                    "WHERE F2P.DB = 'S' "
                    "AND F2P.FEATURE_ID = FS.FEATURE_ID "
                    "AND FS.DBCODE != 'm' "
                    "AND F2P.SEQ_ID = PDC.PROTEIN_AC "
                    "AND PDC.DESCRIPTION_ID = PDV.DESCRIPTION_ID")
        con.commit()

        # Pronto schema
        cur.execute('TRUNCATE TABLE INTERPRO_ANALYSIS.METHOD2SWISS_DE')
        cur.execute(
            """
            INSERT /*+APPEND*/ INTO INTERPRO_ANALYSIS.METHOD2SWISS_DE
            SELECT MP.PROTEIN_AC, MP.METHOD_AC, D.TEXT
              FROM INTERPRO_ANALYSIS.METHOD2PROTEIN MP
            INNER JOIN INTERPRO_ANALYSIS.METHOD M ON MP.METHOD_AC = M.METHOD_AC
            INNER JOIN INTERPRO_ANALYSIS.DESC_VALUE D ON MP.DESC_ID = D.DESC_ID
            WHERE MP.DBCODE = 'S'            
            """
        )
        con.commit()


def update_splice_variants(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('updating splice variants')

        logging.info('\tcleaning the database of old varsplic data')

        cur.execute('CREATE TABLE INTERPRO.VARSPLIC_MATCH_OLD AS SELECT * FROM INTERPRO.VARSPLIC_MATCH')
        cur.execute('CREATE TABLE INTERPRO.VARSPLIC_MASTER_OLD AS SELECT * FROM INTERPRO.VARSPLIC_MASTER')
        cur.execute('CREATE TABLE INTERPRO.VARSPLIC_NEW_OLD AS SELECT * FROM INTERPRO.VARSPLIC_NEW')
        cur.execute('TRUNCATE TABLE INTERPRO.VARSPLIC_MATCH')
        cur.execute('TRUNCATE TABLE INTERPRO.VARSPLIC_MASTER')
        cur.execute('TRUNCATE TABLE INTERPRO.VARSPLIC_NEW')

        logging.info('\tupdating VARSPLIC_MASTER with protein data')

        cur.execute("INSERT /*+ APPEND PARALLEL */ INTO INTERPRO.VARSPLIC_MASTER "
                    "SELECT "
                    "  SUBSTR(XREF.AC, 1, INSTR(XREF.AC, '-') - 1), "
                    "  SUBSTR(XREF.AC, INSTR(XREF.AC, '-') + 1), "
                    "  P.CRC64, "
                    "  P.LEN "
                    "FROM "
                    "  UNIPARC.XREF XREF, "
                    "  UNIPARC.PROTEIN P "
                    "WHERE XREF.UPI = P.UPI "
                    "AND XREF.DELETED = 'N' "
                    "AND XREF.DBID IN (24, 25)")
        con.commit()

        logging.info('\tloading match data for varsplic proteins into VARSPLIC_NEW')

        cur.execute("INSERT /*+ APPEND PARALLEL */ INTO INTERPRO.VARSPLIC_NEW "
                    "SELECT "
                    "  XREF.AC, "
                    "  IPR.METHOD_AC, "
                    "  IPR.SEQ_START, "
                    "  IPR.SEQ_END, "
                    "  'T' AS STATUS, "
                    "  O2D.DBCODE, "
                    "  O2D.EVIDENCE, "
                    "  SYSDATE, "
                    "  SYSDATE, "
                    "  SYSDATE, "
                    "  'INTERPRO', "
                    "  IPR.EVALUE "
                    "FROM "
                    "  IPRSCAN.MV_IPRSCAN IPR, "
                    "  INTERPRO.IPRSCAN2DBCODE O2D, "
                    "  UNIPARC.XREF XREF "
                    "WHERE IPR.ANALYSIS_ID = O2D.IPRSCAN_SIG_LIB_REL_ID "
                    "AND XREF.UPI = IPR.UPI "
                    "AND XREF.DELETED = 'N' "
                    "AND XREF.DBID IN (24, 25)")
        con.commit()

        logging.info('\tloading matches into VARSPLIC_MATCH')

        cur.execute("INSERT /*+ APPEND */ INTO INTERPRO.VARSPLIC_MATCH "
                    "SELECT * "
                    "FROM INTERPRO.VARSPLIC_NEW "
                    "WHERE METHOD_AC IN ("
                    "  SELECT METHOD_AC "
                    "  FROM INTERPRO.METHOD "
                    "  WHERE SKIP_FLAG != 'Y'"
                    ")")
        con.commit()

        cur.execute('DROP TABLE INTERPRO.VARSPLIC_MATCH_OLD')
        cur.execute('DROP TABLE INTERPRO.VARSPLIC_MASTER_OLD')
        cur.execute('DROP TABLE INTERPRO.VARSPLIC_NEW_OLD')


def update_taxonomy(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('DROP TABLE INTERPRO.TAXONOMY_LOAD')

        logging.info('loading taxonomy tree from UniProt')

        cur.execute('CREATE TABLE INTERPRO.TAXONOMY_LOAD TABLESPACE INTERPRO_TAB AS '
                    'SELECT /*+ PARALLEL */ '
                    '  N.TAX_ID, '
                    '  N.PARENT_ID, '
                    '  N.SPTR_SCIENTIFIC SCIENTIFIC_NAME, '
                    '  N.RANK, '
                    '  NVL(N.SPTR_COMMON, N.NCBI_COMMON) COMMON_NAME '
                    'FROM TAXONOMY.V_PUBLIC_NODE@SWPREAD N')
        con.commit()

        cur.execute('GRANT DELETE, INSERT, UPDATE ON INTERPRO.TAXONOMY_LOAD TO INTERPRO_PRODUCTION')
        cur.execute('GRANT SELECT ON TAXONOMY_LOAD TO INTERPRO_SELECT')
        cur.execute('GRANT INSERT ON TAXONOMY_LOAD TO INTERPRO_WEBSERVER')

        cur.callproc('INTERPRO.IPRO_UTL_PKG.TABLE_STATS', ['TAXONOMY_LOAD',])
        con.commit()

        logging.info('computing left and right numbers for taxonomy tree')

        cur.execute('DROP TABLE INTERPRO.ETAXI')

        # Table of taxonomic classifications
        cur.execute("CREATE TABLE INTERPRO.ETAXI TABLESPACE INTERPRO_TAB AS "
                    "SELECT /*+ PARALLEL */ "
                    "  N.TAX_ID, N.PARENT_ID, N.SCIENTIFIC_NAME, 'X' COMPLETE_GENOME_FLAG, "
                    "  N.RANK, 0 HIDDEN, LR.TREE_LEFT LEFT_NUMBER, LR.TREE_RIGHT RIGHT_NUMBER, 'X' ANNOTATION_SOURCE, "
                    "  N.SCIENTIFIC_NAME || CASE WHEN N.COMMON_NAME IS NULL THEN '' ELSE ' (' || N.COMMON_NAME || ')' END FULL_NAME "
                    "FROM INTERPRO.TAXONOMY_LOAD N "
                    "JOIN ("
                    "  SELECT TAX_ID, MIN(TREE_NUMBER) TREE_LEFT, MAX(TREE_NUMBER) TREE_RIGHT"
                    "  FROM ("
                    "    SELECT PARENT_ID AS TAX_ID, ROWNUM AS TREE_NUMBER "
                    "    FROM ("
                    "       SELECT TAX_ID, PARENT_ID "
                    "       FROM ("
                    "         SELECT TAX_ID, PARENT_ID FROM INTERPRO.TAXONOMY_LOAD "
                    "         UNION ALL "
                    "         SELECT 9999999 AS TAX_ID, TAX_ID AS PARENT_ID FROM INTERPRO.TAXONOMY_LOAD "
                    "         UNION ALL "
                    "         SELECT 0 AS TAX_ID, TAX_ID AS PARENT_ID FROM INTERPRO.TAXONOMY_LOAD"
                    "       ) "
                    "       START WITH TAX_ID = 1 "
                    "       CONNECT BY PRIOR TAX_ID=PARENT_ID "
                    "       ORDER SIBLINGS BY TAX_ID"
                    "    ) "
                    "    WHERE TAX_ID IN (9999999, 0)"
                    "  )"
                    "  GROUP BY TAX_ID "
                    ") LR "
                    "ON (LR.TAX_ID = N.TAX_ID)")
        con.commit()

        cur.execute('CREATE INDEX ETAXI$L$R$T ON INTERPRO.ETAXI (LEFT_NUMBER, RIGHT_NUMBER, TAX_ID) TABLESPACE INTERPRO_IND')
        cur.execute('CREATE INDEX ETAXI$P$T$R ON INTERPRO.ETAXI (PARENT_ID, TAX_ID, RANK) TABLESPACE INTERPRO_IND')
        cur.execute('CREATE INDEX ETAXI$T$P$R ON INTERPRO.ETAXI (TAX_ID, PARENT_ID, RANK) TABLESPACE INTERPRO_IND')
        cur.execute('GRANT SELECT ON INTERPRO.ETAXI TO INTERPRO_DEVELOPER')
        cur.execute('GRANT ALTER, DELETE, INSERT, SELECT, UPDATE, ON COMMIT REFRESH, QUERY REWRITE, DEBUG, FLASHBACK ON INTERPRO.ETAXI TO INTERPRO_PRODUCTION')
        cur.execute('GRANT SELECT ON INTERPRO.ETAXI TO INTERPRO_SELECT')
        cur.execute('GRANT SELECT ON INTERPRO.ETAXI TO INTERPRO_WEBSERVER')
        cur.execute('GRANT SELECT ON INTERPRO.ETAXI TO PUBLIC')

        # Refresh stats
        cur.callproc('INTERPRO.IPRO_UTL_PKG.TABLE_STATS', ('ETAXI',))
        con.commit()

        logging.info('creating UNIPROT_TAXONOMY by combining protein taxonomy with left numbers')
        cur.execute('DROP TABLE INTERPRO.UNIPROT_TAXONOMY')

        cur.execute('CREATE TABLE INTERPRO.UNIPROT_TAXONOMY TABLESPACE INTERPRO_TAB AS '
                    'SELECT /*+ PARALLEL */ '
                    '  P.PROTEIN_AC, '
                    '  P.TAX_ID, '
                    '  NVL(ET.LEFT_NUMBER, 0) LEFT_NUMBER, '
                    '  NVL(ET.RIGHT_NUMBER, 0) RIGHT_NUMBER '
                    'FROM INTERPRO.PROTEIN P '
                    'LEFT OUTER JOIN INTERPRO.ETAXI ET ON (P.TAX_ID=ET.TAX_ID)')
        con.commit()

        cur.execute('CREATE INDEX UNIPROT_TAXONOMY$L$P ON INTERPRO.UNIPROT_TAXONOMY (LEFT_NUMBER, PROTEIN_AC) TABLESPACE INTERPRO_IND')
        cur.execute('CREATE INDEX UNIPROT_TAXONOMY$P$L ON INTERPRO.UNIPROT_TAXONOMY (PROTEIN_AC, LEFT_NUMBER) TABLESPACE INTERPRO_IND')

        # Refresh stats
        cur.callproc('INTERPRO.IPRO_UTL_PKG.TABLE_STATS', ('UNIPROT_TAXONOMY',))
        con.commit()

        cur.execute('GRANT SELECT ON UNIPROT_TAXONOMY TO INTERPRO_DEVELOPER')
        cur.execute('GRANT ALTER, DELETE, INSERT, SELECT, UPDATE, ON COMMIT REFRESH, QUERY REWRITE, DEBUG, FLASHBACK ON UNIPROT_TAXONOMY TO INTERPRO_PRODUCTION')
        cur.execute('GRANT SELECT ON UNIPROT_TAXONOMY TO INTERPRO_SELECT')
        cur.execute('GRANT SELECT ON UNIPROT_TAXONOMY TO INTERPRO_WEBSERVER')
        cur.execute('GRANT SELECT ON UNIPROT_TAXONOMY TO PUBLIC')

        logging.info('computing the count of each taxon within each entry')
        cur.execute('DROP TABLE INTERPRO.MV_TAX_ENTRY_COUNT')

        '''
        Count of proteins with true matches to InterPro entries
            ENTRY_AC                        InterPro entry
            TAX_ID                          Taxonomic ID of proteins matching entry
            COUNT                           Count of proteins for this entry and tax Id, also including any child tax Ids
            COUNT_SPECIFIED_TAX_ID          Count of proteins for this entry and this tax Id only
        '''
        cur.execute("CREATE TABLE INTERPRO.MV_TAX_ENTRY_COUNT TABLESPACE INTERPRO_TAB AS "
                    "WITH QUERY1 AS ("
                    "  SELECT ENTRY_AC, ANC.PARENT AS TAX_ID, COUNT(1) AS COUNT"
                    "  FROM INTERPRO.UNIPROT_TAXONOMY UT "
                    "  JOIN INTERPRO.MV_ENTRY2PROTEIN_TRUE MVEP "
                    "ON UT.PROTEIN_AC=MVEP.PROTEIN_AC "
                    "  JOIN ("
                    "    SELECT "
                    "      NVL(SUBSTR(SYS_CONNECT_BY_PATH(TAX_ID, '.'), 2, INSTR(SYS_CONNECT_BY_PATH (TAX_ID,'.'),'.',2) - 2), TAX_ID) AS CHILD, "
                    "      TAX_ID AS PARENT "
                    "    FROM INTERPRO.ETAXI ET "
                    "    CONNECT BY PRIOR PARENT_ID=TAX_ID"
                    "  ) ANC "
                    "  ON ANC.CHILD=UT.TAX_ID "
                    "  GROUP BY ENTRY_AC, ANC.PARENT"
                    "), QUERY2 AS ("
                    "  SELECT ENTRY_AC, TAX_ID, COUNT(1) AS COUNT "
                    "  FROM INTERPRO.UNIPROT_TAXONOMY UT "
                    "  JOIN INTERPRO.MV_ENTRY2PROTEIN_TRUE MVEP "
                    "  ON UT.PROTEIN_AC=MVEP.PROTEIN_AC "
                    "  GROUP BY ENTRY_AC, TAX_ID"
                    ")"
                    "SELECT /*+ PARALLEL */ QUERY1.ENTRY_AC, QUERY1.TAX_ID, QUERY1.COUNT AS COUNT, QUERY2.COUNT AS COUNT_SPECIFIED_TAX_ID "
                    "FROM QUERY1 "
                    "LEFT OUTER JOIN QUERY2 "
                    "ON QUERY1.ENTRY_AC = QUERY2.ENTRY_AC AND QUERY1.TAX_ID = QUERY2.TAX_ID")
        con.commit()

        cur.execute('ALTER TABLE INTERPRO.MV_TAX_ENTRY_COUNT '
                    'ADD CONSTRAINT PK_MV_TAX_ENTRY_COUNT '
                    'PRIMARY KEY (ENTRY_AC, TAX_ID) '
                    'USING INDEX TABLESPACE INTERPRO_IND')

        cur.execute('CREATE UNIQUE INDEX TEC_PERF_IND1 '
                    'ON INTERPRO.MV_TAX_ENTRY_COUNT (TAX_ID, ENTRY_AC) '
                    'TABLESPACE INTERPRO_IND')
        cur.execute('GRANT ALTER, DELETE, INSERT, SELECT, UPDATE, ON COMMIT REFRESH, QUERY REWRITE, DEBUG, FLASHBACK '
                    'ON INTERPRO.MV_TAX_ENTRY_COUNT TO INTERPRO_PRODUCTION')
        cur.execute('GRANT SELECT ON INTERPRO.MV_TAX_ENTRY_COUNT TO INTERPRO_SELECT')

        # Refresh stats
        cur.callproc('INTERPRO.IPRO_UTL_PKG.TABLE_STATS', ('MV_TAX_ENTRY_COUNT',))
        con.commit()


def dump(user, passwd, db, outdir, **kwargs):
    smtp_host = kwargs.get('smtp_host')
    from_addr = kwargs.get('from_addr')
    to_addrs = kwargs.get('to_addrs', [])

    try:
        os.makedirs(outdir)
    except FileExistsError:
        pass

    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('dropping indexes')
        cur.callproc('INTERPRO.XREF_SUMMARY_BUILD.DP_INDEX_XREF')

        logging.info('loading xrefs')
        cur.callproc('INTERPRO.XREF_SUMMARY_BUILD.LOAD_XREF')
        con.commit()

        logging.info('creating indexes')
        cur.callproc('INTERPRO.XREF_SUMMARY_BUILD.CR_INDEX_XREF')
        cur.callproc('INTERPRO.XREF_SUMMARY_BUILD.UPD_PROSITE_STATUS_FINAL')
        con.commit()

        cur.callproc('INTERPRO.IPRO_UTL_PKG.TABLE_STATS', ['XREF_SUMMARY', ])
        con.commit()

        logging.info('performing sanity check')
        # UNKNOWN,FALSE NEG AND PARTIAL status in Databases other than Prosite
        cur.execute("SELECT /*+ PARALLEL */ COUNT(*) "
                    "FROM INTERPRO.XREF_SUMMARY X "
                    "WHERE X.DBCODE NOT IN ('P', 'M') AND X.MATCH_STATUS_FINAL NOT IN ('T', 'F')")
        cnt_1 = cur.fetchone()[0]

        # Prosite of Mixed Status
        cur.execute("SELECT /*+ PARALLEL */ COUNT(*) "
                    "FROM ("
                    "  SELECT X.PROTEIN_AC, X.METHOD_AC "
                    "  FROM INTERPRO.XREF_SUMMARY X "
                    "  WHERE X.DBCODE = 'P' AND X.MATCH_STATUS_FINAL IN ('T', '?') "
                    "  GROUP BY X.PROTEIN_AC, X.METHOD_AC "
                    "  HAVING COUNT(DISTINCT X.MATCH_STATUS_FINAL) = 2"
                    ")")
        cnt_2 = cur.fetchone()[0]

        # CRC64 mismatches
        cur.execute("SELECT /*+ PARALLEL */ COUNT(*) "
                    "FROM ("
                    "  SELECT P.PROTEIN_AC, UX.UPI "
                    "  FROM UNIPARC.XREF UX "
                    "  INNER JOIN INTERPRO.PROTEIN P ON UX.AC = P.PROTEIN_AC "
                    "  JOIN UNIPARC.PROTEIN UP ON UX.AC = UP.UPI, "
                    "       INTERPRO.XREF_SUMMARY X "
                    "  WHERE UX.DELETED = 'N' AND X.PROTEIN_AC = P.PROTEIN_AC AND UP.CRC64 != P.CRC64 "
                    ")")
        cnt_3 = cur.fetchone()[0]

        cur.execute("SELECT /*+ PARALLEL */ COUNT(*) "
                    "FROM INTERPRO.XREF_SUMMARY X "
                    "WHERE X.MATCH_STATUS IS NULL")
        cnt_4 = cur.fetchone()[0]

        with open(os.path.join(outdir, 'sanity.tab'), 'wt') as fh:
            fh.write('\n'.join([
                'NUMBER of UNKNOWN,FALSE NEG AND PARTIAL status in Databases other than Prosite = {}',
                'Number Prosite of Mixed Status = {}',
                'Number of crc64 mismatches = {}',
                'Number of lines where match_status is null = {}',
                'All the above should have no known cases.',
                ''
            ]).format(cnt_1, cnt_2, cnt_3, cnt_4))

        logging.info('dumping statistics')
        filename = os.path.join(outdir, 'statistics.txt')

        try:
            os.rename(filename, os.path.join(outdir, 'statistics.old'))
        except FileNotFoundError:
            pass

        # For storing statistics to be emailed to the team
        content = ''
        with open(filename, 'wt') as fh:
            cur.execute("SELECT /*+ PARALLEL */ "
                        "  C.DBNAME, "
                        "  C.DBCODE, "
                        "  X.MATCH_STATUS, "
                        "  COUNT(*), "
                        "  P.DBCODE "
                        "FROM "
                        "  INTERPRO.XREF_SUMMARY X, "
                        "  INTERPRO.CV_DATABASE C, "
                        "  INTERPRO.PROTEIN P "
                        "WHERE X.PROTEIN_AC = P.PROTEIN_AC "
                        "AND C.DBCODE = 'I' "
                        "GROUP BY C.DBNAME, P.DBCODE, C.DBCODE, X.MATCH_STATUS "
                        "ORDER BY C.DBCODE, P.DBCODE, X.MATCH_STATUS")

            for row in cur:
                line = '|'.join(map(str, row)) + '\n'
                fh.write(line)
                content += line

            cur.execute("SELECT /*+ PARALLEL */ "
                        "  C.DBNAME, "
                        "  X.DBCODE, "
                        "  X.MATCH_STATUS, "
                        "  COUNT(*), "
                        "  P.DBCODE "
                        "FROM "
                        "  INTERPRO.XREF_SUMMARY X, "
                        "  INTERPRO.CV_DATABASE C, "
                        "  INTERPRO.PROTEIN P "
                        "WHERE C.DBCODE = X.DBCODE "
                        "AND X.PROTEIN_AC = P.PROTEIN_AC "
                        "GROUP BY C.DBNAME, P.DBCODE, X.DBCODE, X.MATCH_STATUS "
                        "ORDER BY X.DBCODE, P.DBCODE, X.MATCH_STATUS")

            for row in cur:
                line = '|'.join(map(str, row)) + '\n'
                fh.write(line)
                content += line

        if smtp_host and from_addr and to_addrs:
            utils.sendmail(
                server=smtp_host,
                subject='interpro.xref_summary@ippro reloaded and the tab files are available',
                content=content,
                from_addr=from_addr, to_addrs=to_addrs
            )

        # Needed for tabfile headers
        cur.execute("SELECT VERSION, ENTRY_COUNT "
                    "FROM INTERPRO.DB_VERSION "
                    "WHERE DBCODE = 'I'")
        version, entry_count = cur.fetchone()

        logging.info('dumping interpro.tab for SwissProt and TrEMBL')
        with open(os.path.join(outdir, 'interpro.tab'), 'wt') as fh:
            fh.write('#InterPro Version {} Entry Count {}\n'.format(version, entry_count))
            fh.write('#Tab File Date {}\n'.format(datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')))

            cur.execute("SELECT /*+ PARALLEL */ DISTINCT "
                        "  PROTEIN_AC, "
                        "  ENTRY_AC, "
                        "  SHORT_NAME "
                        "FROM INTERPRO.XREF_SUMMARY "
                        "WHERE MATCH_STATUS_FINAL != 'F'")

            for row in cur:
                fh.write('{}\tIP\t{}\t{}\n'.format(*row))

        logging.info('dumping prosite.tab for SwissProt and TrEMBL')
        with open(os.path.join(outdir, 'prosite.tab'), 'wt') as fh:
            fh.write('#InterPro Version {} Entry Count {}\n'.format(version, entry_count))
            fh.write('#Tab File Date {}\n'.format(datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')))

            cur.execute("SELECT /*+ PARALLEL */ DISTINCT "
                        "  PROTEIN_AC, "
                        "  METHOD_AC, "
                        "  METHOD_NAME, "
                        "  MATCH_STATUS_FINAL, "
                        "  MATCH_COUNT "
                        "FROM INTERPRO.XREF_SUMMARY "
                        "WHERE DBCODE IN ('P', 'M') "
                        "AND MATCH_STATUS_FINAL != 'F'")

            for prot_ac, meth_ac, meth_name, match_status, match_count in cur:
                if match_status == 'T':
                    fh.write('{}\tPR\t{}\t{}\t{}\n'.format(prot_ac, meth_ac, meth_name, match_count))
                elif match_status == '?':
                    fh.write('{}\tPR\t{}\t{}\tUNKNOWN_{}\n'.format(prot_ac, meth_ac, meth_name, match_count))
                elif match_status == 'N':
                    fh.write('{}\tPR\t{}\t{}\tFALSE_NEG\n'.format(prot_ac, meth_ac, meth_name))
                elif match_status == 'P':
                    fh.write('{}\tPR\t{}\t{}\tPARTIAL\n'.format(prot_ac, meth_ac, meth_name))
                elif match_status == 'F':
                    # This should never happen since the query has a where condition excluding 'F'
                    fh.write('{}\tPR\t{}\t{}\tFALSE_POS_{}\n'.format(prot_ac, meth_ac, meth_name, match_count))

        logging.info('dumping prints.tab')
        with open(os.path.join(outdir, 'prints.tab'), 'wt') as fh:
            fh.write('#InterPro Version {} Entry Count {}\n'.format(version, entry_count))
            fh.write('#Tab File Date {}\n'.format(datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')))

            cur.execute("SELECT /*+ PARALLEL */ DISTINCT "
                        "  PROTEIN_AC, "
                        "  METHOD_AC, "
                        "  METHOD_NAME "
                        "FROM INTERPRO.XREF_SUMMARY "
                        "WHERE DBCODE = 'F' "
                        "AND MATCH_STATUS_FINAL != 'F'")

            for row in cur:
                fh.write('{}\tPP\t{}\t{}\t1\n'.format(*row))

        dbs = [
            ('pfam', 'PF', 'H'),
            ('prodom', 'PD', 'D'),
            ('smart', 'SM', 'R'),
            ('tigrfams', 'TF', 'N'),
            ('pirsf', 'PI', 'U'),
            ('gene3d', 'G3D', 'X'),
            ('panther', 'PTHR', 'V'),
            ('supfam', 'SF', 'Y'),
            ('hamap', 'HP', 'Q'),
            ('cdd', 'CDD', 'J'),
            ('sfld', 'SFLD', 'B')
        ]

        for db, abbr, dbcode in dbs:
            logging.info('dumping {}.tab'.format(db))

            with open(os.path.join(outdir, db + '.tab'), 'wt') as fh:
                fh.write('#InterPro Version {} Entry Count {}\n'.format(version, entry_count))
                fh.write('#Tab File Date {}\n'.format(datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')))

                cur.execute("SELECT /*+ PARALLEL */ DISTINCT "
                            "  PROTEIN_AC, "
                            "  METHOD_AC, "
                            "  METHOD_NAME, "
                            "  MATCH_COUNT "
                            "FROM INTERPRO.XREF_SUMMARY "
                            "WHERE DBCODE = :1 "
                            "AND MATCH_STATUS_FINAL != 'F'", (dbcode, ))

                for prot_ac, meth_ac, meth_name, match_count in cur:
                    fh.write('{}\t{}\t{}\t{}\t{}\n'.format(prot_ac, abbr, meth_ac, meth_name, match_count))
