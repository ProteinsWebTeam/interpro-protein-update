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


def prepare_feature_matches(db_user, db_passwd, db_host, **kwargs):
    smtp_host = kwargs.get('smtp_host')
    from_addr = kwargs.get('from_addr')
    to_addrs = kwargs.get('to_addrs', [])

    add_new_feature_matches(db_user, db_passwd, db_host)
    data = pre_prod(db_user, db_passwd, db_host)

    if smtp_host and from_addr and to_addrs:
        content = [
            '{:<30}{:>10}'.format('Feature matches out of bounds', data['pos_error1']),
            '{:<30}{:>10}'.format('Feature matches with invalid positions', data['pos_error2']),
            '{:<30}{:>10}'.format('Feature matches on deleted proteins', data['missing_proteins']),
            '{:<30}{:>10}'.format('Duplicated feature matches', len(data['duplicate_rows'])),
            '',
            'Overlapping feature match positions',
            '    Case 1: {:>10}'.format(data['case1']),
            '    Case 2: {:>10}'.format(data['case2']),
            '    Case 3:',
            '        {:<20}{:<20}{:<10}{:<10}{:<3}{:<19}'.format('Protein', 'Signature', 'From', 'To', 'DB', 'Feature'),
            ' ' * 8 + '-' * 80
        ]

        for m in data['case3']:
            content.append(
                ' ' * 8 + '{:<20}{:<20}{:<10}{:<10}{:<3}{:<19}'.format(
                    m['protein'], m['method'], m['pos_from'],
                    m['pos_to'], m['dbcode'],
                    m['seq_feature'] if m['seq_feature'] else ''
                ))

        """
        smtplib encodes with ascii, which fails to encode '±'.
        the following solution timed out on EBI server, hence it's not used:
        >>> msg = MIMEText(message, _charset='UTF-8')
        >>> msg['Subject'] = Header(subject, 'utf-8')
        """
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
            'Signatures not in the FEATURE_METHOD table having matches',
            '    Signature',
            '    ' + '-' * 20
        ]

        for ac in data['missing_methods']:
            content.append('    {:<20}'.format(ac))

        utils.sendmail(
            server=smtp_host,
            subject='Features report from InterPro protein update',
            content='\n'.join(content) + '\n',
            from_addr=from_addr,
            to_addrs=to_addrs
        )

    return data


def add_new_feature_matches(user, passwd, db, chunksize=100000):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()
        cur2 = con.cursor()

        logging.info('truncating staging table')
        cur.execute('TRUNCATE TABLE INTERPRO.FEATURE_MATCH_NEW_STG')

        logging.info('adding new feature matches to staging table')
        cur.execute("SELECT /*+ PARALLEL */ DISTINCT "
                    "  PS.PROTEIN_AC, "
                    "  IPR.METHOD_AC, "
                    "  IPR.SEQ_FEATURE, "
                    "  IPR.SEQ_START, "
                    "  IPR.SEQ_END, "
                    "  I2D.DBCODE, "
                    "  SYSDATE SEQ_DATE, "
                    "  SYSDATE MATCH_DATE, "
                    "  SYSDATE TIMESTAMP, "
                    "  'INTERPRO' as USERSTAMP "
                    "FROM "
                    "  IPRSCAN.MV_IPRSCAN IPR, "
                    "  INTERPRO.PROTEIN_TO_SCAN PS, "
                    "  INTERPRO.IPRSCAN2DBCODE I2D "
                    "WHERE PS.UPI = IPR.UPI "
                    "  AND I2D.IPRSCAN_SIG_LIB_REL_ID = IPR.ANALYSIS_ID "
                    "  AND I2D.DBCODE IN ('g', 'j', 'n', 'q', 's', 'v', 'x')")
        data = []
        data_len = 0
        cnt = 0

        for row in cur:
            data.append(row)
            if data_len == 0:
                data_len = len(data)
                cur2.bindarraysize = data_len
                db_types2 = (d[1] for d in cur.description)
                cur2.setinputsizes(*db_types2)

            cnt += 1

            if not cnt % chunksize:
                cur2.executemany("INSERT INTO INTERPRO.FEATURE_MATCH_NEW_STG ("
                                 "  PROTEIN_AC, "
                                 "  METHOD_AC, "
                                 "  SEQ_FEATURE, "
                                 "  POS_FROM, "
                                 "  POS_TO, "
                                 "  DBCODE, "
                                 "  SEQ_DATE, "
                                 "  MATCH_DATE, "
                                 "  TIMESTAMP, "
                                 "  USERSTAMP"
                                 ") "
                                 "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)", data)
                data = []
                logging.info('adding new feature matches to staging table\t{0}'.format(cnt))

        if data:
            cur2.executemany("INSERT INTO INTERPRO.FEATURE_MATCH_NEW_STG ("
                             "  PROTEIN_AC, "
                             "  METHOD_AC, "
                             "  SEQ_FEATURE, "
                             "  POS_FROM, "
                             "  POS_TO, "
                             "  DBCODE, "
                             "  SEQ_DATE, "
                             "  MATCH_DATE, "
                             "  TIMESTAMP, "
                             "  USERSTAMP"
                             ") "
                             "VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10)", data)

        logging.info('added new feature matches to staging table\t{0}'.format(cnt))

        con.commit()
        cur2.close()

        logging.info('adding new feature matches from staging table to final table')
        cur.execute('TRUNCATE TABLE INTERPRO.FEATURE_MATCH_NEW')

        cur.execute('INSERT /*+ PARALLEL */ INTO INTERPRO.FEATURE_MATCH_NEW '
                    'SELECT * '
                    'FROM INTERPRO.FEATURE_MATCH_NEW_STG')
        con.commit()

        cur.callproc('INTERPRO.FEATURE_MATCH_NEW_IDX_PROC')
        con.commit()


def update_feature_matches(user, passwd, db):
    delete_feature_match(user, passwd, db)
    insert_feature_match(user, passwd, db)


def delete_feature_match(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('deleting old feature matches')
        cur.execute('ALTER SESSION FORCE PARALLEL DML PARALLEL 4')
        cur.execute('DELETE /*+ PARALLEL */ '
                    'FROM INTERPRO.FEATURE_MATCH M '
                    'WHERE EXISTS('
                    '  SELECT PROTEIN_AC '
                    '  FROM INTERPRO.PROTEIN_TO_SCAN S '
                    '  WHERE S.PROTEIN_AC = M.PROTEIN_AC'
                    ')')
        con.commit()


def insert_feature_match(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('inserting new feature matches')
        cur.execute('ALTER SESSION FORCE PARALLEL DML PARALLEL 4')
        cur.execute('INSERT /*+ PARALLEL */ INTO INTERPRO.FEATURE_MATCH '
                    'SELECT * FROM INTERPRO.FEATURE_MATCH_NEW')
        con.commit()


def pre_prod(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('creating pre-production report')

        # Feature matches past the end of the protein
        logging.info('  matches past the end of the protein')
        cur.execute('SELECT /*+ PARALLEL */ COUNT(*) '
                    'FROM '
                    '  INTERPRO.PROTEIN P, '
                    '  INTERPRO.FEATURE_MATCH_NEW M '
                    'WHERE P.PROTEIN_AC = M.PROTEIN_AC '
                    'AND M.POS_TO > P.LEN')
        cnt_pos_error_1 = cur.fetchone()[0]
        logging.info('    {}'.format(cnt_pos_error_1))

        # Feature matches where start and end positions make no sense
        logging.info('  matches with invalid start/end posititions')
        cur.execute('SELECT /*+ PARALLEL */ COUNT(*) '
                    'FROM INTERPRO.FEATURE_MATCH_NEW '
                    'WHERE POS_FROM > POS_TO OR POS_FROM < 1')
        cnt_pos_error_2 = cur.fetchone()[0]
        logging.info('    {}'.format(cnt_pos_error_2))

        # Feature matches on proteins that do no exist anymore
        logging.info('  feature matches on deleted proteins')
        cur.execute('SELECT /*+ PARALLEL */ COUNT(*) '
                    'FROM INTERPRO.FEATURE_MATCH_NEW '
                    'WHERE PROTEIN_AC NOT IN ('
                    '  SELECT PROTEIN_AC FROM INTERPRO.PROTEIN'
                    ')')
        cnt_missing_proteins = cur.fetchone()[0]
        logging.info('    {}'.format(cnt_missing_proteins))

        # Number of duplicated rows in feature_match_new
        logging.info('  duplicated entries in FEATURE_MATCH_NEW')
        cur.execute('SELECT M.PROTEIN_AC, M.METHOD_AC, M.POS_FROM, M.POS_TO, M.DBCODE '
                    'FROM INTERPRO.FEATURE_MATCH_NEW M '
                    'GROUP BY M.PROTEIN_AC, M.METHOD_AC, M.POS_FROM, M.POS_TO, M.DBCODE '
                    'HAVING COUNT(*) > 1')
        duplicate_rows = [dict(zip(['protein_ac', 'method_ac', 'pos_from', 'pos_to', 'dbcode'], row)) for row in cur]
        logging.info('    {}'.format(len(duplicate_rows)))

        # Reporting Overlapping feature match positions
        # Case 1: SSF feature matches
        logging.info('  overlapping match positions (case 1: SSF feature matches)')
        cur.execute('SELECT /*+ PARALLEL */ COUNT(*) '
                    'FROM INTERPRO.FEATURE_MATCH_NEW M1 '
                    'WHERE EXISTS('
                    '  SELECT 1 '
                    '  FROM INTERPRO.FEATURE_MATCH_NEW_STG M2 '
                    '  WHERE M1.METHOD_AC = M2.METHOD_AC '
                    '  AND M1.PROTEIN_AC = M2.PROTEIN_AC '
                    '  AND M1.POS_FROM < M2.POS_TO '
                    '  AND M1.POS_TO > M2.POS_FROM '
                    '  AND M1.POS_FROM != M2.POS_FROM '
                    '  AND M1.POS_TO != M2.POS_TO'
                    '  AND M1.DBCODE = M2.DBCODE'
                    ')')
        cnt_case_1 = cur.fetchone()[0]
        logging.info('    {}'.format(cnt_case_1))

        # Case 2:
        logging.info('  overlapping feature match positions (case 2)')
        cur.execute("SELECT /*+ PARALLEL */ COUNT(*) "
                    "FROM INTERPRO.FEATURE_MATCH_NEW M1 "
                    "WHERE M1.DBCODE != 'Y' "
                    "AND EXISTS("
                    "  SELECT 1 "
                    "  FROM INTERPRO.FEATURE_MATCH_NEW_STG M2 "
                    "  WHERE M1.METHOD_AC = M2.METHOD_AC "
                    "  AND M1.PROTEIN_AC = M2.PROTEIN_AC "
                    "  AND M1.POS_FROM = M2.POS_FROM "
                    "  AND M1.POS_TO != M2.POS_TO"
                    "  AND M1.DBCODE = M2.DBCODE"
                    ")")
        cnt_case_2 = cur.fetchone()[0]
        logging.info('    {}'.format(cnt_case_2))

        # Case 3
        logging.info('  overlapping feature match positions (case 3)')
        cur.execute("SELECT /*+ PARALLEL */ DISTINCT M1.PROTEIN_AC, M1.METHOD_AC, M1.POS_FROM, M1.POS_TO, M1.DBCODE, M1.SEQ_FEATURE "
                    "FROM INTERPRO.FEATURE_MATCH_NEW_STG M2, INTERPRO.FEATURE_MATCH_NEW M1 "
                    "WHERE M1.METHOD_AC = M2.METHOD_AC "
                    "AND M1.PROTEIN_AC = M2.PROTEIN_AC "
                    "AND M1.POS_TO = M2.POS_TO "
                    "AND M1.POS_FROM != M2.POS_FROM "
                    "AND M1.DBCODE = M2.DBCODE ")
        case_3 = [dict(zip(['protein', 'method', 'pos_from', 'pos_to', 'dbcode', 'seq_feature'], row)) for row in cur]
        logging.info('    {}'.format(len(case_3)))

        # Database count changes
        logging.info('  database count changes')
        cur.execute('SELECT DBCODE, CVD.DBNAME, COUNT_OLD, COUNT_NEW '
                    'FROM INTERPRO.CV_DATABASE CVD '
                    'JOIN ('
                    '  SELECT DBCODE, NVL(OLD.COUNT, 0) AS COUNT_OLD, NVL(FEATURE_MATCH.COUNT, 0) + NVL(FEATURE_MATCH_NEW.COUNT, 0) AS COUNT_NEW FROM ('
                    '    SELECT M1.DBCODE, SUM(MMM.MATCH_COUNT) AS COUNT '
                    '    FROM INTERPRO.FEATURE_METHOD M1 '
                    '    JOIN INTERPRO.MV_FEA_METHOD_MATCH MMM USING (METHOD_AC) '
                    '    GROUP BY DBCODE'
                    '  ) OLD '
                    '  FULL OUTER JOIN ('
                    '    SELECT M1.DBCODE, COUNT(*) AS COUNT '
                    '    FROM INTERPRO.FEATURE_MATCH M1 '
                    '    GROUP BY DBCODE'
                    '  ) FEATURE_MATCH USING (DBCODE)  '
                    '  FULL OUTER JOIN ('
                    '    SELECT M1.DBCODE, COUNT(*) AS COUNT '
                    '    FROM INTERPRO.FEATURE_MATCH_NEW M1 '
                    '   GROUP BY DBCODE'
                    '  ) FEATURE_MATCH_NEW USING (DBCODE)'
                    ') USING (DBCODE)')
        db_count_changes = [dict(zip(['code', 'name', 'old', 'new'], row)) for row in cur]
        logging.info('    {}'.format(len(db_count_changes)))

        # Methods not in the FEATURE_METHOD table that have feature matches in FEATURE_MATCH_NEW
        logging.info('  signatures not in the FEATURE_METHOD table that have feature matches in the FEATURE_MATCH_NEW table')
        cur.execute('SELECT DISTINCT METHOD_AC '
                    'FROM INTERPRO.FEATURE_MATCH_NEW '
                    'MINUS '
                    'SELECT METHOD_AC '
                    'FROM INTERPRO.FEATURE_METHOD')
        missing_methods = [row[0] for row in cur]
        logging.info('    {}'.format(len(missing_methods)))

    return {
        'pos_error1': cnt_pos_error_1,
        'pos_error2': cnt_pos_error_2,
        'missing_proteins': cnt_missing_proteins,
        'duplicate_rows': duplicate_rows,
        # 'case1': cnt_case_1,
        # 'case2': cnt_case_2,
        # 'case3': case_3,
        'case1': 0,
        'case2': 0,
        'case3': [],
        'db_changes': db_count_changes,
        'missing_methods': missing_methods
    }

def refresh(db_user, db_passwd, db_host):
    with cx_Oracle.connect(db_user, db_passwd, db_host) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('Refreshing feature match MV tables')
        cur.callproc('INTERPRO.REFRESH_FEATURE_MATCH_COUNTS.REFRESH')
        con.commit()