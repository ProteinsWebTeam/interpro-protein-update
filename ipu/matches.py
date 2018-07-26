#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging

import cx_Oracle

from . import utils, xref


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s: %(levelname)s: %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)


def prepare_matches(db_user, db_passwd, db_host, **kwargs):
    smtp_host = kwargs.get('smtp_host')
    from_addr = kwargs.get('from_addr')
    to_addrs = kwargs.get('to_addrs', [])

    add_new_matches(db_user, db_passwd, db_host)
    data = pre_prod(db_user, db_passwd, db_host)

    if smtp_host and from_addr and to_addrs:
        content = [
            '{:<30}{:>10}'.format('Matches out of bounds', data['pos_error1']),
            '{:<30}{:>10}'.format('Matches with invalid positions', data['pos_error2']),
            '{:<30}{:>10}'.format('Matches on deleted proteins', data['missing_proteins']),
            '{:<30}{:>10}'.format('Duplicated matches', len(data['duplicate_rows'])),
            '{:<30}{:>10}'.format('Skip-flagged signatures', len(data['skip_flagged_signatures'])),
            '',
            'Overlapping match positions',
            '    Case 1: {:>10}'.format(data['case1']),
            '    Case 2: {:>10}'.format(data['case2']),
            '    Case 3:',
            '        {:<20}{:<20}{:<10}{:<10}'.format('Protein', 'Signature', 'From', 'To'),
            ' ' * 8 + '-' * 60
        ]

        for m in data['case3']:
            content.append(
                ' ' * 8 + '{:<20}{:<20}{:<10}{:<10}'.format(m['protein'], m['method'], m['pos_from'], m['pos_to'])
            )

        """
        smtplib encodes with ascii, which fails to encode '±'.
        the following solution timed out on EBI server, hence it's not used:
        >>> msg = MIMEText(message, _charset='UTF-8')
        >>> msg['Subject'] = Header(subject, 'utf-8')
        """
        content += [
            '',
            '{:<50}{:>10}'.format('Signatures with UniProt for the first time', data['new']),
            '{:<50}{:>10}'.format('Signatures without UniProt matches', data['methods_without_match']),
            '',
            # 'Entry changes of at least ±50% of previous match count',
            'Entry changes of at least +/-50% of previous match count',
            '    {:<15}{:>15}{:>15}{:>15}{:>15}'.format('Entry', 'Previous count', 'New count', 'Change', 'Integrated'),
            '    ' + '-' * 75
        ]

        for e in sorted(data['match_changes'], key=lambda x: x['entry']):
            try:
                p = (e['new'] - e['old']) / e['old'] * 100
            except ZeroDivisionError:
                p = ''
            else:
                p = str(round(p, 1)) + '%'
            finally:
                content.append(
                    '    {:<15}{:>15}{:>15}{:>15}{:>15}'.format(e['entry'], e['old'], e['new'], p, e['checked'])
                )

        content += [
            '',
            'Database count changes',
            '    {:<10}{:<20}{:<20}{:<15}'.format('Code', 'Database', 'Previous count', 'New count'),
            '    ' + '-' * 65
        ]

        for db in sorted(data['db_changes'], key=lambda x: x['name']):
            content.append('    {:<10}{:<20}{:<20}{:<15}'.format(db['code'], db['name'], db['old'], db['new']))

        content += [
            '',
            'Signatures not in the METHOD table having matches',
            '    Signature',
            '    ' + '-' * 20
        ]

        for ac in data['missing_methods']:
            content.append('    {:<20}'.format(ac))

        utils.sendmail(
            server=smtp_host,
            subject='Report from InterPro protein update',
            content='\n'.join(content) + '\n',
            from_addr=from_addr,
            to_addrs=to_addrs
        )

    return data


def add_new_matches(user, passwd, db, chunksize=100000):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()
        cur2 = con.cursor()

        logging.info('truncating staging table')
        cur.execute('TRUNCATE TABLE INTERPRO.MATCH_NEW_STG')

        logging.info('adding new matches to staging table')
        cur.execute("SELECT /*+ PARALLEL */ DISTINCT "
                    "  PS.PROTEIN_AC, "
                    "  IPR.METHOD_AC, "
                    "  IPR.SEQ_START, "
                    "  IPR.SEQ_END, "
                    "  'T' STATUS, "
                    "  I2D.DBCODE, "
                    "  I2D.EVIDENCE, "
                    "  SYSDATE SEQ_DATE, "
                    "  SYSDATE MATCH_DATE, "
                    "  SYSDATE TIMESTAMP, "
                    "  IPR.EVALUE "
                    "FROM "
                    "  IPRSCAN.MV_IPRSCAN IPR, "
                    "  INTERPRO.PROTEIN_TO_SCAN PS, "
                    "  INTERPRO.IPRSCAN2DBCODE I2D "
                    "WHERE PS.UPI = IPR.UPI "
                    "AND I2D.IPRSCAN_SIG_LIB_REL_ID = IPR.ANALYSIS_ID")
        data = []
        cnt = 0

        for row in cur:
            data.append(row)
            cnt += 1

            if not cnt % chunksize:
                cur2.executemany("INSERT INTO INTERPRO.MATCH_NEW_STG ("
                                 "  PROTEIN_AC, "
                                 "  METHOD_AC, "
                                 "  POS_FROM, "
                                 "  POS_TO, "
                                 "  STATUS, "
                                 "  DBCODE, "
                                 "  EVIDENCE, "
                                 "  SEQ_DATE, "
                                 "  MATCH_DATE, "
                                 "  TIMESTAMP, "
                                 "  USERSTAMP, "
                                 "  SCORE"
                                 ") "
                                 "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, 'INTERPRO',  :11)", data)
                data = []
                logging.info('adding new matches to staging table\t{}'.format(cnt))

        if data:
            cur2.executemany("INSERT INTO INTERPRO.MATCH_NEW_STG ("
                             "  PROTEIN_AC, "
                             "  METHOD_AC, "
                             "  POS_FROM, "
                             "  POS_TO, "
                             "  STATUS, "
                             "  DBCODE, "
                             "  EVIDENCE, "
                             "  SEQ_DATE, "
                             "  MATCH_DATE, "
                             "  TIMESTAMP, "
                             "  USERSTAMP, "
                             "  SCORE"
                             ") "
                             "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, 'INTERPRO',  :11)", data)

        con.commit()
        cur2.close()

        logging.info('deleting duplicates Superfamily matches from staging table')
        cur.execute("DELETE FROM INTERPRO.MATCH_NEW_STG M1 "
                    "WHERE EXISTS("
                    "  SELECT 1 "
                    "  FROM INTERPRO.MATCH_NEW_STG M2 "
                    "  WHERE M1.PROTEIN_AC = M2.PROTEIN_AC "
                    "  AND M2.DBCODE = 'Y' "
                    "  AND M1.METHOD_AC = M2.METHOD_AC "
                    "  AND M1.POS_FROM = M2.POS_FROM "
                    "  AND M1.POS_TO = M2.POS_TO "
                    "  AND M1.SCORE > M2.SCORE"
                    ")")
        con.commit()

        logging.info('deleting one-residue matches from staging table')
        cur.execute('DELETE FROM INTERPRO.MATCH_NEW_STG WHERE POS_FROM = POS_TO')
        con.commit()

        logging.info('adding new matches from staging table to final table')
        cur.execute('TRUNCATE TABLE INTERPRO.MATCH_NEW')

        cur.execute('INSERT /*+ PARALLEL */ INTO INTERPRO.MATCH_NEW '
                    'SELECT * '
                    'FROM INTERPRO.MATCH_NEW_STG')
        con.commit()

        cur.callproc('INTERPRO.MATCH_NEW_IDX_PROC')
        con.commit()


def update_matches(user, passwd, db):
    delete_match(user, passwd, db) 
    insert_match(user, passwd, db) 


def delete_match(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('deleting old matches')
        cur.execute('ALTER SESSION FORCE PARALLEL DML PARALLEL 4')
        cur.execute('DELETE /*+ PARALLEL */ '
                    'FROM INTERPRO.MATCH M '
                    'WHERE EXISTS('
                    '  SELECT PROTEIN_AC '
                    '  FROM INTERPRO.PROTEIN_TO_SCAN S '
                    '  WHERE S.PROTEIN_AC = M.PROTEIN_AC'
                    ')')
        con.commit()


def insert_match(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('inserting new matches')
        cur.execute('ALTER SESSION FORCE PARALLEL DML PARALLEL 4')
        cur.execute('INSERT /*+ PARALLEL */ INTO INTERPRO.MATCH '
                    'SELECT * FROM INTERPRO.MATCH_NEW')
        con.commit()


def pre_prod(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('creating pre-production report')

        # Matches past the end of the protein
        logging.info('  matches past the end of the protein')
        cur.execute('SELECT /*+ PARALLEL */ COUNT(*) '
                    'FROM '
                    '  INTERPRO.PROTEIN P, '
                    '  INTERPRO.MATCH_NEW M '
                    'WHERE P.PROTEIN_AC = M.PROTEIN_AC '
                    'AND M.POS_TO > P.LEN')
        cnt_pos_error_1 = cur.fetchone()[0]
        logging.info('    {}'.format(cnt_pos_error_1))

        # Matches where start and end positions make no sense
        logging.info('  matches with invalid start/end posititions')
        cur.execute('SELECT /*+ PARALLEL */ COUNT(*) '
                    'FROM INTERPRO.MATCH_NEW '
                    'WHERE POS_FROM >= POS_TO OR POS_FROM < 1')
        cnt_pos_error_2 = cur.fetchone()[0]
        logging.info('    {}'.format(cnt_pos_error_2))

        # Matches on proteins that do no exist anymore
        logging.info('  matches on deleted proteins')
        cur.execute('SELECT /*+ PARALLEL */ COUNT(*) '
                    'FROM INTERPRO.MATCH_NEW '
                    'WHERE PROTEIN_AC NOT IN ('
                    '  SELECT PROTEIN_AC FROM INTERPRO.PROTEIN'
                    ')')
        cnt_missing_proteins = cur.fetchone()[0]
        logging.info('    {}'.format(cnt_missing_proteins))

        # Number of duplicated rows in match_new
        logging.info('  duplicated entries in MATCH_NEW')
        cur.execute('SELECT M.PROTEIN_AC, M.METHOD_AC, M.POS_FROM, M.POS_TO '
                    'FROM INTERPRO.MATCH_NEW M '
                    'GROUP BY M.PROTEIN_AC, M.METHOD_AC, M.POS_FROM, M.POS_TO '
                    'HAVING COUNT(*) > 1')
        duplicate_rows = [dict(zip(['protein_ac', 'method_ac', 'pos_from', 'pos_to'], row)) for row in cur]
        logging.info('    {}'.format(len(duplicate_rows)))

        # Number of skip-flagged signature matches in match_new (will be deleted later)
        logging.info('  skip-flagged signature matches')
        cur.execute("SELECT /*+ PARALLEL */ N.DBCODE, COUNT(1) "
                    "FROM INTERPRO.MATCH_NEW N, INTERPRO.METHOD M "
                    "WHERE M.METHOD_AC = N.METHOD_AC "
                    "AND M.SKIP_FLAG = 'Y' "
                    "GROUP BY N.DBCODE")
        skip_flagged_signatures = [dict(zip(['db', 'count'], row)) for row in cur]
        logging.info('    {}'.format(len(skip_flagged_signatures)))

        # Reporting Overlapping match positions
        # Case 1: SSF matches
        logging.info('  overlapping match positions (case 1: SSF matches)')
        cur.execute('SELECT /*+ PARALLEL */ COUNT(*) '
                    'FROM INTERPRO.MATCH_NEW M1 '
                    'WHERE EXISTS('
                    '  SELECT 1 '
                    '  FROM INTERPRO.MATCH_NEW_STG M2 '
                    '  WHERE M1.METHOD_AC = M2.METHOD_AC '
                    '  AND M1.PROTEIN_AC = M2.PROTEIN_AC '
                    '  AND M1.POS_FROM < M2.POS_TO '
                    '  AND M1.POS_TO > M2.POS_FROM '
                    '  AND M1.POS_FROM != M2.POS_FROM '
                    '  AND M1.POS_TO != M2.POS_TO'
                    ')')
        cnt_case_1 = cur.fetchone()[0]
        logging.info('    {}'.format(cnt_case_1))

        # Case 2:
        logging.info('  overlapping match positions (case 2)')
        cur.execute("SELECT /*+ PARALLEL */ COUNT(*) "
                    "FROM INTERPRO.MATCH_NEW M1 "
                    "WHERE M1.DBCODE != 'Y' "
                    "AND EXISTS("
                    "  SELECT 1 "
                    "  FROM INTERPRO.MATCH_NEW_STG M2 "
                    "  WHERE M1.METHOD_AC = M2.METHOD_AC "
                    "  AND M1.PROTEIN_AC = M2.PROTEIN_AC "
                    "  AND M1.POS_FROM = M2.POS_FROM "
                    "  AND M1.POS_TO != M2.POS_TO"
                    ")")
        cnt_case_2 = cur.fetchone()[0]
        logging.info('    {}'.format(cnt_case_2))

        # Case 3
        logging.info('  overlapping match positions (case 3)')
        cur.execute("SELECT /*+ PARALLEL */ DISTINCT M1.PROTEIN_AC, M1.METHOD_AC, M1.POS_FROM, M1.POS_TO "
                    "FROM INTERPRO.MATCH_NEW_STG M2, INTERPRO.MATCH_NEW M1 "
                    "WHERE M1.METHOD_AC = M2.METHOD_AC "
                    "AND M1.PROTEIN_AC = M2.PROTEIN_AC "
                    "AND M1.POS_TO = M2.POS_TO "
                    "AND M1.POS_FROM != M2.POS_FROM "
                    "AND M1.DBCODE != 'Y'")
        case_3 = [dict(zip(['protein', 'method', 'pos_from', 'pos_to'], row)) for row in cur]
        logging.info('    {}'.format(len(case_3)))

        # The following entries have to be fixed
        # Methods that now have matches in Uniprot but previously did not (excluding PfamB as we do not integrate them)
        logging.info('  signatures with matches in UniProt for the 1st time')
        cur.execute("SELECT /*+ PARALLEL */ COUNT(DISTINCT METHOD_AC) "
                    "FROM INTERPRO.MATCH_NEW "
                    "WHERE METHOD_AC IN ("
                    "  SELECT DISTINCT METHOD_AC "
                    "  FROM INTERPRO.METHOD "
                    "  WHERE DBCODE != 'f' "
                    "  MINUS "
                    "  SELECT DISTINCT METHOD_AC "
                    "  FROM INTERPRO.MV_METHOD_MATCH"
                    ")")
        cnt_new = cur.fetchone()[0]
        logging.info('    {}'.format(cnt_new))

        # Number of Methods that do not have matches in UniProt
        logging.info('  methods without matches in UniProt')
        cur.execute("SELECT /*+ PARALLEL */ COUNT(DISTINCT METHOD_AC) "
                    "FROM ("
                    "  SELECT METHOD_AC "
                    "  FROM INTERPRO.METHOD "
                    "  WHERE SKIP_FLAG ='N' "
                    "  AND CANDIDATE = 'Y' "
                    "  MINUS "
                    "  ("
                    "    SELECT DISTINCT METHOD_AC "
                    "    FROM INTERPRO.MATCH "
                    "    UNION "
                    "    SELECT DISTINCT METHOD_AC "
                    "    FROM INTERPRO.MATCH_NEW"
                    "  )"
                    ")")
        cnt_methods_no_match = cur.fetchone()[0]
        logging.info('    {}'.format(cnt_methods_no_match))

        # Entry changes of at least +/-50% of previous match count
        logging.info('  entry changes of +/-50% of previous match count')
        cur.execute('SELECT /*+ PARALLEL */ E1.ENTRY_AC, E1.COUNT_OLD, E1.COUNT_NEW, E2.CHECKED FROM ('
                    '  SELECT ENTRY_AC, NVL(CMV, 0) COUNT_OLD, NVL(C_MATCH, 0) + NVL(C_NEW, 0) COUNT_NEW '
                    '  FROM ('
                    '    SELECT ENTRY_AC, COUNT(PROTEIN_AC) CMV '
                    '    FROM INTERPRO.MV_ENTRY2PROTEIN '
                    '    GROUP BY ENTRY_AC'
                    '  ) '
                    '  FULL OUTER JOIN ('
                    '    SELECT ENTRY_AC, COUNT(DISTINCT CASE WHEN PS.PROTEIN_AC IS NULL THEN MO.PROTEIN_AC ELSE NULL END) C_MATCH '
                    '    FROM INTERPRO.ENTRY2METHOD EM '
                    '    JOIN INTERPRO.MATCH MO USING (METHOD_AC) '
                    '    LEFT OUTER JOIN INTERPRO.PROTEIN_TO_SCAN PS ON MO.PROTEIN_AC = PS.PROTEIN_AC '
                    '    GROUP BY ENTRY_AC'
                    '  ) USING (ENTRY_AC) '
                    '  FULL OUTER JOIN ('
                    '    SELECT ENTRY_AC, COUNT(DISTINCT PROTEIN_AC) C_NEW '
                    '    FROM INTERPRO.ENTRY2METHOD EM '
                    '    JOIN INTERPRO.MATCH_NEW MN USING (METHOD_AC) '
                    '    GROUP BY ENTRY_AC'
                    '  ) USING (ENTRY_AC)'
                    ') E1, '
                    'INTERPRO.ENTRY E2 '
                    'WHERE NOT E1.COUNT_NEW - E1.COUNT_OLD BETWEEN -E1.COUNT_OLD*0.5 AND E1.COUNT_OLD*0.5 '
                    'AND E1.ENTRY_AC = E2.ENTRY_AC')
        match_count_changes = [dict(zip(['entry', 'old', 'new', 'checked'], row)) for row in cur]
        logging.info('    {}'.format(len(match_count_changes)))

        # Database count changes
        logging.info('  database count changes')
        cur.execute('SELECT DBCODE, CVD.DBNAME, COUNT_OLD, COUNT_NEW '
                    'FROM INTERPRO.CV_DATABASE CVD '
                    'JOIN ('
                    '  SELECT DBCODE, NVL(OLD.COUNT, 0) AS COUNT_OLD, NVL(MATCH.COUNT, 0) + NVL(MATCH_NEW.COUNT, 0) AS COUNT_NEW FROM ('
                    '    SELECT M1.DBCODE, SUM(MMM.MATCH_COUNT) AS COUNT '
                    '    FROM INTERPRO.METHOD M1 '
                    '    JOIN INTERPRO.MV_METHOD_MATCH MMM USING (METHOD_AC) '
                    '    GROUP BY DBCODE'
                    '  ) OLD '
                    '  FULL OUTER JOIN ('
                    '    SELECT M1.DBCODE, COUNT(*) AS COUNT '
                    '    FROM INTERPRO.MATCH M1 '
                    '    GROUP BY DBCODE'
                    '  ) MATCH USING (DBCODE)  '
                    '  FULL OUTER JOIN ('
                    '    SELECT M1.DBCODE, COUNT(*) AS COUNT '
                    '    FROM INTERPRO.MATCH_NEW M1 '
                    '   GROUP BY DBCODE'
                    '  ) MATCH_NEW USING (DBCODE)'
                    ') USING (DBCODE)')
        db_count_changes = [dict(zip(['code', 'name', 'old', 'new'], row)) for row in cur]
        logging.info('    {}'.format(len(db_count_changes)))

        # Methods not in the METHOD table that have matches in MATCH_NEW
        logging.info('  signatures not in the METHOD table that have matches in the MATCH_NEW table')
        cur.execute('SELECT DISTINCT METHOD_AC '
                    'FROM INTERPRO.MATCH_NEW '
                    'MINUS '
                    'SELECT METHOD_AC '
                    'FROM INTERPRO.METHOD')
        missing_methods = [row[0] for row in cur]
        logging.info('    {}'.format(len(missing_methods)))

    return {
        'pos_error1': cnt_pos_error_1,
        'pos_error2': cnt_pos_error_2,
        'missing_proteins': cnt_missing_proteins,
        'duplicate_rows': duplicate_rows,
        'skip_flagged_signatures': skip_flagged_signatures,
        'case1': cnt_case_1,
        'case2': cnt_case_2,
        'case3': case_3,
        'new': cnt_new,
        'methods_without_match': cnt_methods_no_match,
        'match_changes': match_count_changes,
        'db_changes': db_count_changes,
        'missing_methods': missing_methods
    }


def finalize(method_changes, db_user, db_passwd, db_host, **kwargs):
    smtp_host = kwargs.get('smtp_host')
    from_addr = kwargs.get('from_addr')
    to_addrs_1 = kwargs.get('to_addrs_1', [])  # internal email (curators)
    to_addrs_2 = kwargs.get('to_addrs_2', [])  # "public" email (UniProt, etc.)

    # Splice variants, required for MV tables
    xref.update_splice_variants(db_user, db_passwd, db_host)

    if smtp_host and from_addr and to_addrs_1:
        # Alert curators
        content = [
            'Dear curators,'
            ''
            'Please log out of talisman. MV tables are being updated. '
            'The internal InterPro website may not function properly. '
            'This will take approximately 20 hours and you will be notified when this has completed.',
            '',
            'Thank you'
        ]

        utils.sendmail(
            server=smtp_host,
            subject='MV tables update in progress',
            content='\n'.join(content) + '\n',
            from_addr=from_addr,
            to_addrs=to_addrs_1
        )

    with cx_Oracle.connect(db_user, db_passwd, db_host) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('updating materialised views of data')

        cur.callproc('INTERPRO.REFRESH_MATCH_COUNTS.REFRESH')
        con.commit()

        logging.info('updating match statistics table in database')

        cur.execute('CREATE TABLE INTERPRO.MATCH_STATS_OLD '
                    'AS SELECT * '
                    'FROM INTERPRO.MATCH_STATS')

        cur.execute('TRUNCATE TABLE INTERPRO.MATCH_STATS')

        cur.execute('INSERT /*+ APPEND PARALLEL */ INTO INTERPRO.MATCH_STATS '
                    'SELECT '
                    '  C.DBCODE, '
                    '  C.DBCODE, '
                    '  M1.STATUS, '
                    '  COUNT(M1.STATUS) AS COUNT '
                    'FROM '
                    '  INTERPRO.CV_DATABASE C, '
                    '  INTERPRO.MATCH M1 '
                    'WHERE C.DBCODE = M1.DBCODE '
                    'GROUP BY C.DBNAME, C.DBCODE, M1.STATUS')
        con.commit()

        cur.execute('DROP TABLE INTERPRO.MATCH_STATS_OLD')

    # Taxonomy
    xref.update_taxonomy(db_user, db_passwd, db_host)

    # Send report (global announcement)
    if smtp_host and from_addr and to_addrs_2:
        content = [
            'Dear all,',
            '',
            'Below are listed the signature-entry assignments that changed since {}.'.format(method_changes['date']),
            '',
            'Deleted signatures:',
            '    {:<20}{:<20}'.format('Signature', 'Last entry'),
            '    ' + '-' * 40
        ]

        for s in method_changes['deleted']:
            content.append('    {:<20}{:<20}'.format(s['method'], s['last_entry']))

        content += [
            '',
            'Moved signatures',
            '    {:<20}{:<20}{:<20}'.format('Signatures', 'Original entry', 'New entry'),
            '    ' + '-' * 60
        ]

        for s in method_changes['moved']:
            content.append('    {:<20}{:<20}{:<20}'.format(s['method'], s['original_entry'], s['new_entry']))

        content += [
            '',
            'Unintegrated signatures (still in member database)',
            '    {:<20}{:<20}'.format('Signature', 'Last entry'),
            '    ' + '-' * 40
        ]

        for s in method_changes['deintegrated']:
            content.append('    {:<20}{:<20}'.format(s['method'], s['last_entry']))

        content += [
            '',
            'New signatures',
            '    {:<20}{:<15}{:>10}'.format('Signature', 'Entry', 'TrEMBL count'),
            '    ' + '-' * 50
        ]

        for s in method_changes['new']:
            content.append('    {:<20}{:<15}{:>10}'.format(s['method'], s['entry'], s['count']))

        utils.sendmail(
            server=smtp_host,
            subject='Protein update completed',
            content='\n'.join(content) + '\n',
            from_addr=from_addr,
            to_addrs=to_addrs_2
        )


def refresh_interpro2go(db_user, db_passwd, db_host):
    with cx_Oracle.connect(db_user, db_passwd, db_host) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('updating REFRESH_MV_PDB2INTERPRO2GO')
        cur.callproc('INTERPRO.REFRESH_MATCH_COUNTS.REFRESH_MV_PDB2INTERPRO2GO')
        con.commit()

        logging.info('updating REFRESH_MV_UNIPROT2INTERPRO2GO')
        cur.callproc('INTERPRO.REFRESH_MATCH_COUNTS.REFRESH_MV_UNIPROT2INTERPRO2GO')
        con.commit()


def update_site_matches(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('populating INTERPRO.SITE_MATCH_NEW')
        cur.execute('TRUNCATE TABLE INTERPRO.SITE_MATCH_NEW')

        cur.execute('INSERT /*+ APPEND PARALLEL */ INTO INTERPRO.SITE_MATCH_NEW ('
                    '  PROTEIN_AC, '
                    '  METHOD_AC, '
                    '  LOC_START, '
                    '  LOC_END, '
                    '  DESCRIPTION, '
                    '  RESIDUE, '
                    '  RESIDUE_START, '
                    '  RESIDUE_END, '
                    '  NUM_SITES, '
                    '  DBCODE'
                    ') '
                    'SELECT '
                    '  P.PROTEIN_AC, '
                    '  MS.METHOD_AC, '
                    '  MS.LOC_START, '
                    '  MS.LOC_END, '
                    '  MS.DESCRIPTION, '
                    '  MS.RESIDUE,'
                    '  MS.RESIDUE_START, '
                    '  MS.RESIDUE_END, '
                    '  MS.NUM_SITES, '
                    '  I2D.DBCODE '
                    'FROM '
                    '  IPRSCAN.SITE MS, '
                    '  INTERPRO.PROTEIN_TO_SCAN P, '
                    '  INTERPRO.IPRSCAN2DBCODE I2D '
                    'WHERE MS.UPI = P.UPI '
                    'AND MS.ANALYSIS_ID = I2D.IPRSCAN_SIG_LIB_REL_ID')
        con.commit()

        logging.info('checking INTERPRO.SITE_MATCH_NEW')
        cur.execute('SELECT DISTINCT PROTEIN_AC, METHOD_AC, LOC_START, LOC_END '
                    'FROM INTERPRO.SITE_MATCH_NEW '
                    'MINUS ('
                    '  SELECT DISTINCT PROTEIN_AC, METHOD_AC, POS_FROM, POS_TO '
                    '  FROM INTERPRO.MATCH PARTITION (MATCH_DBCODE_J) '
                    '  UNION '
                    '  SELECT DISTINCT PROTEIN_AC, METHOD_AC, POS_FROM, POS_TO '
                    '  FROM INTERPRO.MATCH PARTITION (MATCH_DBCODE_B)'
                    ')')
        n = sum([1 for _ in cur])

        if n:
            logging.critical('error: {} matches in SITE_MATCH_NEW that are not in MATCH'.format(n))
            return False

        logging.info('deleting old matches')
        cur.execute('DELETE /*+ PARALLEL */ FROM INTERPRO.SITE_MATCH M '
                    'WHERE EXISTS('
                    '  SELECT PROTEIN_AC '
                    '  FROM INTERPRO.PROTEIN_TO_SCAN S '
                    '  WHERE S.PROTEIN_AC = M.PROTEIN_AC'
                    ')')

        logging.info('inserting new matches')
        cur.execute('INSERT INTO INTERPRO.SITE_MATCH '
                    'SELECT * FROM INTERPRO.SITE_MATCH_NEW')
        con.commit()

    return True


def rebuild_matches(user, passwd, db, insert=True, index=True, replace=True):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        if insert:
            logging.info('dropping table')
            try:
                cur.execute('DROP TABLE INTERPRO.MATCH_TST')
            except:
                pass

            logging.info('creating table')
            cur.execute(
                """
                CREATE TABLE INTERPRO.MATCH_TST
                (
                    PROTEIN_AC VARCHAR(15) NOT NULL,
                    METHOD_AC VARCHAR(25) NOT NULL,
                    MODEL_AC VARCHAR2(25) NOT NULL,
                    POS_FROM NUMBER(5,0),
                    POS_TO NUMBER(5,0),
                    STATUS CHAR(1) NOT NULL,
                    DBCODE CHAR(1) NOT NULL,
                    EVIDENCE CHAR(3),
                    USERSTAMP VARCHAR2(30) NOT NULL,
                    SCORE FLOAT(126)
                )
                PARTITION BY LIST (DBCODE)
                (
                    PARTITION MATCH_DBCODE_V VALUES ('V'),
                    PARTITION MATCH_DBCODE_R VALUES ('R'),
                    PARTITION MATCH_DBCODE_U VALUES ('U'),
                    PARTITION MATCH_DBCODE_P VALUES ('P'),
                    PARTITION MATCH_DBCODE_Y VALUES ('Y'),
                    PARTITION MATCH_DBCODE_D VALUES ('D'),
                    PARTITION MATCH_DBCODE_M VALUES ('M'),
                    PARTITION MATCH_DBCODE_Q VALUES ('Q'),
                    PARTITION MATCH_DBCODE_X VALUES ('X'),
                    PARTITION MATCH_DBCODE_N VALUES ('N'),
                    PARTITION MATCH_DBCODE_H VALUES ('H'),
                    PARTITION MATCH_DBCODE_G VALUES ('g'),
                    PARTITION MATCH_DBCODE_F VALUES ('F'),
                    PARTITION MATCH_DBCODE_B VALUES ('B'),
                    PARTITION MATCH_DBCODE_J VALUES ('J')
                )
                """
            )

            logging.info('populating table 1/2')
            #
            cur.execute(
                """
                INSERT /*+ APPEND */ INTO INTERPRO.MATCH_TST
                SELECT DISTINCT
                  UPX.AC,
                  IPR.METHOD_AC,
                  IPR.MODEL_AC,
                  IPR.SEQ_START,
                  IPR.SEQ_END,
                  'T',
                  I2D.DBCODE,
                  I2D.EVIDENCE,
                  'INTERPRO',
                  IPR.EVALUE
                FROM IPRSCAN.MV_IPRSCAN IPR
                INNER JOIN UNIPARC.XREF UPX ON (IPR.UPI = UPX.UPI AND UPX.DBID IN (2, 3) AND UPX.DELETED = 'N')
                INNER JOIN INTERPRO.IPRSCAN2DBCODE I2D ON IPR.ANALYSIS_ID = I2D.IPRSCAN_SIG_LIB_REL_ID
                WHERE IPR.SEQ_START < IPR.SEQ_END
                AND I2D.DBCODE != 'Y'
                """
            )
            con.commit()

            logging.info('populating table 2/2')
            cur.execute(
                """
                INSERT INTO /*+ APPEND */ INTERPRO.MATCH_TST
                    SELECT
                        A.AC,
                        A.METHOD_AC,
                        A.MODEL_AC,
                        A.SEQ_START,
                        A.SEQ_END,
                        'T',
                        A.DBCODE,
                        A.EVIDENCE,
                        'INTERPRO',
                        A.EVALUE
                    FROM (
                        SELECT
                            UPX.AC,
                            IPR.METHOD_AC,
                            IPR.MODEL_AC,
                            IPR.SEQ_START,
                            IPR.SEQ_END,
                            I2D.DBCODE,
                            I2D.EVIDENCE,
                            IPR.EVALUE,
                            ROW_NUMBER() OVER (PARTITION BY UPX.AC, IPR.METHOD_AC, IPR.SEQ_START, IPR.SEQ_END ORDER BY IPR.EVALUE) RN
                        FROM IPRSCAN.MV_IPRSCAN IPR
                            INNER JOIN UNIPARC.XREF UPX ON (IPR.UPI = UPX.UPI AND UPX.DBID IN (2, 3) AND UPX.DELETED = 'N')
                            INNER JOIN INTERPRO.IPRSCAN2DBCODE I2D ON IPR.ANALYSIS_ID = I2D.IPRSCAN_SIG_LIB_REL_ID
                        WHERE IPR.SEQ_START < IPR.SEQ_END AND I2D.DBCODE = 'Y'
                    ) A
                WHERE A.RN = 1
                """
            )
            con.commit()

        if index:
            logging.info('adding constraint: PK_MATCH_TST')
            cur.execute(
                """
                ALTER TABLE INTERPRO.MATCH_TST
                ADD CONSTRAINT PK_MATCH_TST PRIMARY KEY (PROTEIN_AC, METHOD_AC, MODEL_AC, POS_FROM, POS_TO)
                USING INDEX TABLESPACE INTERPRO_IND
                """
            )
            logging.info('adding constraint: CK_MATCH_TST$FROM')
            cur.execute(
                """
                ALTER TABLE INTERPRO.MATCH_TST
                ADD CONSTRAINT CK_MATCH_TST$FROM CHECK (POS_FROM >= 1)
                """
            )
            logging.info('adding constraint: CK_MATCH_TST$NEG')
            cur.execute(
                """
                ALTER TABLE INTERPRO.MATCH_TST
                ADD CONSTRAINT CK_MATCH_TST$NEG CHECK (POS_TO - POS_FROM > 0)
                """
            )
            logging.info('adding constraint: CK_MATCH_TST$STATUS')
            cur.execute(
                """
                ALTER TABLE INTERPRO.MATCH_TST
                ADD CONSTRAINT CK_MATCH_TST$STATUS CHECK (STATUS!='N' OR (STATUS='N' AND DBCODE IN ('P', 'M', 'Q')))
                """
            )
            logging.info('adding constraint: FK_MATCH_TST$DBCODE')
            cur.execute(
                """
                ALTER TABLE INTERPRO.MATCH_TST 
                ADD CONSTRAINT FK_MATCH_TST$DBCODE FOREIGN KEY (DBCODE) REFERENCES INTERPRO.CV_DATABASE (DBCODE)
                """
            )
            logging.info('adding constraint: FK_MATCH_TST$EVIDENCE')
            cur.execute(
                """
                ALTER TABLE INTERPRO.MATCH_TST 
                ADD CONSTRAINT FK_MATCH_TST$EVIDENCE FOREIGN KEY (EVIDENCE) REFERENCES INTERPRO.CV_EVIDENCE (CODE)
                """
            )
            logging.info('adding constraint: FK_MATCH_TST$METHOD')
            cur.execute(
                """
                ALTER TABLE INTERPRO.MATCH_TST 
                ADD CONSTRAINT FK_MATCH_TST$METHOD FOREIGN KEY (METHOD_AC) REFERENCES INTERPRO.METHOD (METHOD_AC) ON DELETE CASCADE
                """
            )
            logging.info('adding constraint: FK_MATCH_TST$PROTEIN')
            cur.execute(
                """
                ALTER TABLE INTERPRO.MATCH_TST 
                ADD CONSTRAINT FK_MATCH_TST$PROTEIN FOREIGN KEY (PROTEIN_AC) REFERENCES INTERPRO.PROTEIN (PROTEIN_AC) ON DELETE CASCADE
                """
            )
            logging.info('adding constraint: FK_MATCH_TST$STATUS')
            cur.execute(
                """
                ALTER TABLE INTERPRO.MATCH_TST 
                ADD CONSTRAINT FK_MATCH_TST$STATUS FOREIGN KEY (STATUS) REFERENCES INTERPRO.CV_STATUS (CODE)
                """
            )

            # logging.info('creating index on: STATUS')
            # cur.execute("CREATE INDEX I_MATCH_TST$STATUS ON INTERPRO.MATCH_TST ('STATUS')")
            # logging.info('creating index on: DBCODE')
            # cur.execute("CREATE INDEX I_MATCH_TST$DBCODE ON INTERPRO.MATCH_TST ('DBCODE')")
            # logging.info('creating index on: EVIDENCE')
            # cur.execute("CREATE INDEX I_MATCH_TST$EVIDENCE ON INTERPRO.MATCH_TST ('EVIDENCE')")
            logging.info('creating index on: METHOD_AC')
            cur.execute("CREATE INDEX I_MATCH_TST$METHOD_AC ON INTERPRO.MATCH_TST ('METHOD_AC') "
                        "TABLESPACE INTERPRO_IND NOLOGGING")

        if replace:
            try:
                cur.execute('DROP TABLE INTERPRO.MATCH CASCADE CONSTRAINTS ')
            except:
                pass

            cur.execute('ALTER TABLE INTERPRO.MATCH_TST RENAME CONSTRAINT PK_MATCH_TST TO PK_MATCH')
            cur.execute('ALTER TABLE INTERPRO.MATCH_TST RENAME CONSTRAINT CK_MATCH_TST$FROM TO CK_MATCH$FROM')
            cur.execute('ALTER TABLE INTERPRO.MATCH_TST RENAME CONSTRAINT CK_MATCH_TST$NEG TO CK_MATCH$NEG')
            cur.execute('ALTER TABLE INTERPRO.MATCH_TST RENAME CONSTRAINT CK_MATCH_TST$STATUS TO CK_MATCH$STATUS')
            cur.execute('ALTER TABLE INTERPRO.MATCH_TST RENAME CONSTRAINT FK_MATCH_TST$DBCODE TO FK_MATCH$DBCODE')
            cur.execute('ALTER TABLE INTERPRO.MATCH_TST RENAME CONSTRAINT FK_MATCH_TST$EVIDENCE TO FK_MATCH$EVIDENCE')
            cur.execute('ALTER TABLE INTERPRO.MATCH_TST RENAME CONSTRAINT FK_MATCH_TST$METHOD TO FK_MATCH$METHOD')
            cur.execute('ALTER TABLE INTERPRO.MATCH_TST RENAME CONSTRAINT FK_MATCH_TST$PROTEIN TO FK_MATCH$PROTEIN')
            cur.execute('ALTER TABLE INTERPRO.MATCH_TST RENAME CONSTRAINT FK_MATCH_TST$STATUS TO FK_MATCH$STATUS')
            cur.execute('ALTER INDEX I_MATCH_TST$METHOD_AC RENAME TO I_MATCH$METHOD_AC')
            cur.execute('ALTER TABLE INTERPRO.MATCH_TST RENAME TO MATCH')


        logging.info('complete')
