#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
import sys
import cx_Oracle
from mundone import Batch, Task

from . import utils


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s: %(levelname)s: %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)


def compare_ispro_ippro(db_user, db_passwd, db_host, **kwargs):
    smtp_host = kwargs.get('smtp_host')
    from_addr = kwargs.get('from_addr')
    to_addrs = kwargs.get('to_addrs', [])

    analysis_tables = {
        'coils': ['coils', 'coils', 'coils', 'coils'],
        'blast_prodom': ['blast_prodom', 'blast_prodom', 'blast_prodom', 'blast_prodom'],
        'finger_prints': ['finger_prints', 'finger_prints', 'finger_prints', 'finger_prints'],
        'hmmer2': ['hmmer2', 'hmmer2', 'hmmer2', 'hmmer2'],
        'hmmer3': ['hmmer3', 'hmmer3', 'hmmer3', 'hmmer3'],
        'mobidb_lite': ['mobidb_lite', 'mobidb_lite', 'mobidb_lite', 'mobidb_lite'],
        'panther': ['panther', 'panther', 'panther', 'panther'],
        'pattern_scan': ['pattern_scan', 'pattern_scan', 'pattern_scan', 'pattern_scan'],
        'phobius': ['phobius', 'phobius', 'phobius', 'phobius'],
        'profile_scan': ['profile_scan', 'profile_scan', 'profile_scan', 'profile_scan'],
        'rpsblast': ['rpsblast', 'rpsblast', 'rpsblast', 'rpsblast'],
        'signalp': ['signalp', 'signalp', 'signalp', 'signalp'],
        'superfamily': ['super_family_hmmer3', 'superfamilyhmmer3', 'super_family_hmmer3', 'superfamilyhmmer3'],
        'tmhmm': ['tmhmm', 'tmhmm', 'tmhmm', 'tmhmm']
    }

    max_upis = {
        'uapro': None,
        'uaread': None
    }

    with cx_Oracle.connect(db_user, db_passwd, db_host) as con:
        con.autocommit = 0
        cur = con.cursor()

        indexes = utils.get_indexes(cur, db_user, 'MV_IPRSCAN')
        is_valid = None
        for idx in indexes:
            if idx['name'] == 'MV_IPRSCAN_UPI_METHOD_AN_IDX':
                is_valid = idx['status'] == 'VALID'
                break

        if is_valid is None:
            logging.critical('MV_IPRSCAN_UPI_METHOD_AN_IDX index not found')
            exit(1)
        elif not is_valid:
            logging.info('rebuilding index MV_IPRSCAN_UPI_METHOD_AN_IDX')
            utils.rebuild_index(cur, db_user, 'MV_IPRSCAN_UPI_METHOD_AN_IDX', hint='PARALLEL 12 NOLOGGING')
        con.commit()

        logging.info("checking UniParc's highest UPI")
        cur.execute('SELECT MAX(UPI) FROM UNIPARC.PROTEIN')
        max_upis['uapro'] = cur.fetchone()[0]
        logging.info('\tMax @UAPRO:          {}'.format(max_upis['uapro']))

        cur.execute('SELECT MAX(UPI) FROM UNIPARC.PROTEIN@UAREAD')
        max_upis['uaread'] = cur.fetchone()[0]
        logging.info('\tMax @UAREAD:         {}'.format(max_upis['uaread']))

        counts = {}
        logging.info('checking IPPRO/ISPRO sync status')
        for analysis, tables in sorted(analysis_tables.items()):
            logging.info('\t{}'.format(analysis))
            separator = '' if analysis == 'superfamily' else '_'

            cur.execute('SELECT MAX(ID) '
                        'FROM mv_{}{}match'.format(tables[0], separator))
            max_id = cur.fetchone()[0]

            cur.execute('SELECT COUNT(ID) '
                        'FROM mv_{}{}match '
                        'WHERE ID <= :1'.format(tables[0], separator), (max_id,))
            ippro_match = cur.fetchone()[0]

            cur.execute('SELECT COUNT(ID) '
                        'FROM {}{}match@ispro '
                        'WHERE ID <= :1'.format(tables[2], separator), (max_id,))
            ispro_match = cur.fetchone()[0]

            cur.execute('SELECT COUNT(ID) '
                        'FROM mv_{}_location '
                        'WHERE MATCH_ID <= :1'.format(tables[1]), (max_id,))
            ippro_location = cur.fetchone()[0]

            cur.execute('SELECT COUNT(ID) '
                        'FROM {}_location@ispro '
                        'WHERE MATCH_ID <= :1'.format(tables[3]), (max_id,))
            ispro_location = cur.fetchone()[0]

            counts[analysis] = {
                'ippro_match': ippro_match,
                'ispro_match': ispro_match,
                'ippro_location': ippro_location,
                'ispro_location': ispro_location
            }

        cur.execute("SELECT t1.LIBRARY "
                    "FROM MV_SIGNATURE_LIBRARY_RELEASE t1 "
                    "JOIN MV_SIG_REL_LIB_TO_CV_ANALYSIS t2 "
                    "ON t1.ID = t2.SIG_REL_LIBRARY_ID "
                    "WHERE t1.LIBRARY = 'COILS' OR t1.LIBRARY = 'TMHMM' "
                    "OR t1.LIBRARY = 'PHOBIUS' OR t1.LIBRARY LIKE 'SIGNALP%'")
        non_members_databases = [row[0] for row in cur]

        cur.execute("SELECT t2.ID, t3.CV_ANALYSIS_ID, t2.LIBRARY, t2.VERSION, t3.ACTIVE "
                    "FROM IPRSCAN.IPRSCAN_RELEASES t1 "
                    "JOIN MV_SIGNATURE_LIBRARY_RELEASE t2 "
                    "ON t1.ANALYSIS_ID = t2.ID "
                    "JOIN MV_SIG_REL_LIB_TO_CV_ANALYSIS t3 "
                    "ON t2.ID = t3.SIG_REL_LIBRARY_ID "
                    "UNION "
                    "SELECT t1.ID, t2.CV_ANALYSIS_ID, t1.LIBRARY, t1.VERSION, t2.ACTIVE "
                    "FROM MV_SIGNATURE_LIBRARY_RELEASE t1 "
                    "JOIN MV_SIG_REL_LIB_TO_CV_ANALYSIS t2 "
                    "ON t1.ID = t2.SIG_REL_LIBRARY_ID "
                    "WHERE t1.LIBRARY = 'TMHMM' OR t1.LIBRARY = 'PHOBIUS' OR t1.LIBRARY LIKE 'SIGNALP%' ")

        interpro_dbs = []
        for row in cur:
            interpro_dbs.append({
                'id': row[0],
                'cv_id': row[1],
                'name': row[2],
                'version': row[3],
                'active': bool(row[4]),
                'is_member': row[2] not in non_members_databases,
                'max_upi': None,
                'up_to_date': None
            })

        logging.info('highest UPI:')
        for i, mem_db in enumerate(interpro_dbs):
            logging.info('\t{}-{}'.format(mem_db['name'], mem_db['version']))
            cur.execute('SELECT MAX(UPI) '
                        'FROM IPRSCAN.MV_IPRSCAN '
                        'WHERE ANALYSIS_ID = :1', (mem_db['id'],))
            max_upi = cur.fetchone()[0]

            if max_upi is None:
                interpro_dbs[i]['max_upi'] = ''
                interpro_dbs[i]['up_to_date'] = False
            else:
                interpro_dbs[i]['max_upi'] = max_upi
                interpro_dbs[i]['up_to_date'] = max_upi >= max_upis['uaread']

    # Generate and send report
    ref_dbs = {
        'cdd': 'rpsblast',
        'coils': 'coils',
        'gene3d': 'hmmer3',
        'hamap': 'profile_scan',
        'mobidb_lite': 'mobidb_lite',
        'panther': 'panther',
        'pfam': 'hmmer3',
        'phobius': 'phobius',
        'pirsf': 'hmmer3',
        'prints': 'finger_prints',
        'prodom': 'blast_prodom',
        'prosite_patterns': 'pattern_scan',
        'prosite_profiles': 'profile_scan',
        'sfld': 'hmmer3',
        'signalp_euk': 'signalp',
        'signalp_gram_negative': 'signalp',
        'signalp_gram_positive': 'signalp',
        'smart': 'hmmer2',
        'superfamily': 'superfamily',
        'tigrfam': 'hmmer3',
        'tmhmm': 'tmhmm'
    }
    to_refresh = []
    mem_db_out = ''
    non_mem_db_out = ''
    is_ready = True

    for db in sorted(interpro_dbs, key=lambda x: x['name']):
        if db['active']:
            if db['up_to_date']:
                iprscan_str = 'up-to-date'
            else:
                iprscan_str = 'not up-to-date'
                # to_refresh.append('MV_IPRSCAN')
                is_ready = False

            c = counts[ref_dbs[db['name'].lower()]]
            if c['ippro_match'] >= c['ispro_match'] and c['ippro_location'] >= c['ispro_location']:
                sync_str = 'sync'
            else:
                sync_str = 'not sync'
                to_refresh.append(db)
                is_ready = False

            line = '{:<36}\t{:<16}\t{:<20}\t{}\n'.format(
                '  - ' + db['name'] + '-' + db['version'],
                db['max_upi'],
                iprscan_str,
                sync_str
            )

            if db['is_member']:
                mem_db_out += line
            else:
                non_mem_db_out += line

    if smtp_host and from_addr and to_addrs:
        content = [
            'Max UPI in UniParc',
            '------------------',
            '',
            '  - UAPRO:   {}'.format(max_upis['uapro']),
            '  - UAREAD:  {}'.format(max_upis['uaread']),
            '',
            'Analysis',
            '--------',
            '',
            'Member databases:',
            mem_db_out,
            'Non-member databases:',
            non_mem_db_out
        ]

        if to_refresh:
            content += [
                '',
                'The following tables have to be refreshed:'
            ]

            for table in to_refresh:
                content.append('  - ' + table['name'])

        utils.sendmail(
            server=smtp_host,
            subject='ISPRO/{} status report'.format(db_host.upper()),
            content='\n'.join(content) + '\n',
            from_addr=from_addr,
            to_addrs=to_addrs
        )

    return is_ready


def refresh_mv_iprscan(user, passwd, host, method='C', **kwargs):

    # TODO Add model_ac column to *pct* tables, plus support for location fragments too?
    # TODO Or delete this refresh_mv_iprscan if never used! <-- DO THIS!
    logging.critical('error: refresh_mv_iprscan code was called that should no longer be used?')
    return False

    workdir = kwargs.get('workdir', os.getcwd())
    queue = kwargs.get('queue')
    parallel = kwargs.get('parallel', 1)
    lsf_log = kwargs.get('log', False)

    # refresh MV tables
    analysis_tables = {
        'blast_prodom': ['blast_prodom', 'blast_prodom'],
        'coils': ['coils', 'coils'],
        'finger_prints': ['finger_prints', 'finger_prints'],
        'hmmer2': ['hmmer2', 'hmmer2'],
        'hmmer3': ['hmmer3', 'hmmer3'],
        'mobidb_lite': ['mobidb_lite', 'mobidb_lite'],
        'panther': ['panther', 'panther'],
        'pattern_scan': ['pattern_scan', 'pattern_scan'],
        'phobius': ['phobius', 'phobius'],
        'profile_scan': ['profile_scan', 'profile_scan'],
        'rpsblast': ['rpsblast', 'rpsblast'],
        'signalp': ['signalp', 'signalp'],
        'superfamily': ['super_family_hmmer3', 'superfamilyhmmer3'],
        'tmhmm': ['tmhmm', 'tmhmm']
    }

    logging.info('refreshing materialized views')
    tasks = []
    for analysis, tables in sorted(analysis_tables.items()):
        separator = '' if analysis == 'superfamily' else '_'
        match_table = 'mv_{}{}match'.format(tables[0], separator)
        location_table = 'mv_{}_location'.format(tables[1])

        tasks.append(Task(
            fn=utils.refresh_materialized_view,
            args=(user, passwd, host, match_table, method),
            lsf=dict(name=match_table),
            log=lsf_log
        ))

        tasks.append(Task(
            fn=utils.refresh_materialized_view,
            args=(user, passwd, host, location_table, method),
            lsf=dict(name=location_table),
            log=lsf_log
        ))

    for table in ['mv_protein_xref', 'mv_signature', 'mv_signature_library_release', 'mv_sig_rel_lib_to_cv_analysis']:
        tasks.append(Task(
            fn=utils.refresh_materialized_view,
            args=(user, passwd, host, table, method),
            lsf=dict(name=table),
            log=lsf_log
        ))

    batch = Batch(tasks, dir=workdir)

    if not batch.start().wait().is_done():
        logging.critical('error while refreshing materialized views')
        return False

    # Refresh Partition Change Tracking tables
    logging.info('refreshing PCT tables')

    tasks = [
        # Member databases
        Task(fn=_refresh_pct_blast_prodom, args=(user, passwd, host), lsf=dict(name='BLAST_PRODOM', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_gene3d, args=(user, passwd, host), lsf=dict(name='GENE3D', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_hamap, args=(user, passwd, host), lsf=dict(name='HAMAP', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_mobidb_lite, args=(user, passwd, host), lsf=dict(name='MOBIDB_LITE', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_panther, args=(user, passwd, host), lsf=dict(name='PANTHER', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_prosite_patterns, args=(user, passwd, host), lsf=dict(name='PROSITE_PATTERNS', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_pfam, args=(user, passwd, host), lsf=dict(name='PFAM', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_pirsf, args=(user, passwd, host), lsf=dict(name='PIRSF', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_prints, args=(user, passwd, host), lsf=dict(name='PRINTS', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_prosite_profiles, args=(user, passwd, host), lsf=dict(name='PROSITE_PROFILES', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_sfld, args=(user, passwd, host), lsf=dict(name='SFLD', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_smart, args=(user, passwd, host), lsf=dict(name='SMART, queue=queue'), log=lsf_log),
        Task(fn=_refresh_pct_rpblast, args=(user, passwd, host), lsf=dict(name='RPSBLAST', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_superfamily, args=(user, passwd, host), lsf=dict(name='SUPERFAMILY', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_tigrfam, args=(user, passwd, host), lsf=dict(name='TIGRFAM', queue=queue), log=lsf_log),
        # Non-member databases
        Task(fn=_refresh_pct_coils, args=(user, passwd, host), lsf=dict(name='COILS', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_phobius, args=(user, passwd, host), lsf=dict(name='PHOBIUS', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_signalp, args=(user, passwd, host), lsf=dict(name='SIGNALP', queue=queue), log=lsf_log),
        Task(fn=_refresh_pct_tmhmm, args=(user, passwd, host), lsf=dict(name='TMHMM', queue=queue), log=lsf_log)
    ]

    batch = Batch(tasks, dir=workdir)
    if not batch.start().wait().is_done():
        logging.critical('error while refreshing PCT table')
        return False

    with cx_Oracle.connect(user, passwd, host) as con:
        con.autocommit = 0
        cur = con.cursor()

        databases = [
            # Member databases
            ('PRODOM', 'PCT_BLAST_PRODOM'),
            ('GENE3D', 'PCT_GENE3D'),
            ('HAMAP', 'PCT_HAMAP'),
            ('MOBIDBLITE', 'PCT_MOBIDB_LITE'),
            ('PANTHER111', 'PCT_PANTHER'),
            ('PROSITE_PATTERNS', 'PCT_PROSITE_PATTERNS'),
            ('PFAM', 'PCT_PFAM'),
            ('PIRSF', 'PCT_PIRSF'),
            ('PROSITE_PROFILES', 'PCT_PROSITE_PROFILES'),
            ('PRINTS', 'PCT_PRINTS'),
            ('RPSBLAST', 'PCT_RPSBLAST'),
            ('SFLD2', 'PCT_SFLD'),
            ('SMART', 'PCT_SMART'),
            ('SUPERFAMILY', 'PCT_SUPERFAMILY'),
            ('TIGRFAM', 'PCT_TIGRFAM'),

            # Non-member databases
            ('COILS', 'PCT_COILS'),
            ('PHOBIUS', 'PCT_PHOBIUS'),
            ('SIGNALP_EUK', 'PCT_SIGNALP_30'),
            ('SIGNALP_GRAM_POSITIVE', 'PCT_SIGNALP_31'),
            ('SIGNALP_GRAM_NEGATIVE', 'PCT_SIGNALP_32'),
            ('TMHMM', 'PCT_TMHMM')
        ]
        for partition, table in databases:
            logging.info('update partition {} in IPRSCAN.MV_IPRSCAN'.format(partition))
            cur.execute('ALTER TABLE IPRSCAN.MV_IPRSCAN '
                        'EXCHANGE PARTITION {} WITH TABLE {} '
                        'INCLUDING INDEXES '
                        'WITHOUT VALIDATION'.format(partition, table))
        con.commit()

        # Rebuid indexes
        if parallel and parallel > 1:
            logging.info('rebuilding index MV_IPRSCAN_ANALYSIS_ID_UPIX')
            cur.execute('ALTER INDEX IPRSCAN.MV_IPRSCAN_ANALYSIS_ID_UPIX '
                        'REBUILD NOLOGGING PARALLEL {}'.format(parallel))

            logging.info('rebuilding index MV_IPRSCAN_ANALYSIS_ID_MAJORX')
            cur.execute(
                'ALTER INDEX IPRSCAN.MV_IPRSCAN_ANALYSIS_ID_MAJORX '
                'REBUILD NOLOGGING PARALLEL {}'.format(parallel))

            logging.info('rebuilding index MV_IPRSCAN_UPI_METHOD_ACX')
            cur.execute('ALTER INDEX IPRSCAN.MV_IPRSCAN_UPI_METHOD_ACX '
                        'REBUILD NOLOGGING PARALLEL {}'.format(parallel))

            logging.info('rebuilding index MV_IPRSCAN_UPI_METHOD_AN_IDX')
            cur.execute('ALTER INDEX IPRSCAN.MV_IPRSCAN_UPI_METHOD_AN_IDX '
                        'REBUILD NOLOGGING PARALLEL {}'.format(parallel))
        else:
            logging.info('rebuilding index MV_IPRSCAN_ANALYSIS_ID_UPIX')
            cur.execute('ALTER INDEX IPRSCAN.MV_IPRSCAN_ANALYSIS_ID_UPIX '
                        'REBUILD NOLOGGING')

            logging.info('rebuilding index MV_IPRSCAN_ANALYSIS_ID_MAJORX')
            cur.execute('ALTER INDEX IPRSCAN.MV_IPRSCAN_ANALYSIS_ID_MAJORX '
                        'REBUILD NOLOGGING')

            logging.info('rebuilding index MV_IPRSCAN_UPI_METHOD_ACX')
            cur.execute('ALTER INDEX IPRSCAN.MV_IPRSCAN_UPI_METHOD_ACX '
                        'REBUILD NOLOGGING')

            logging.info('rebuilding index MV_IPRSCAN_UPI_METHOD_AN_IDX')
            cur.execute('ALTER INDEX IPRSCAN.MV_IPRSCAN_UPI_METHOD_AN_IDX '
                        'REBUILD NOLOGGING')

        con.commit()

    return True


def refresh_site(user, passwd, host, method='C', **kwargs):
    workdir = kwargs.get('workdir', os.getcwd())
    queue = kwargs.get('queue')
    parallel = kwargs.get('parallel', 1)

    # Refreshing MV tables
    logging.info('refreshing materialized views')

    tasks = [
        Task(
            fn=utils.refresh_materialized_view,
            args=(user, passwd, host, 'MV_CDD_SITE', method),
            lsf=dict(name='MV_CDD_SITE'),
            log=False
        ),
        Task(
            fn=utils.refresh_materialized_view,
            args=(user, passwd, host, 'MV_SFLD_SITE', method),
            lsf=dict(name='MV_SFLD_SITE'),
            log=False
        )
    ]

    batch = Batch(tasks, dir=workdir)

    if not batch.start().wait().is_done():
        logging.critical('error while refreshing materialized views')
        return False

    # Refresh Partition Change Tracking tables
    logging.info('refreshing PCT tables')
    tasks = [
        Task(
            fn=_refresh_pct_cdd_site,
            args=(user, passwd, host),
            lsf=dict(name='CDD_SITE'),
            log=False
        ),
        Task(
            fn=_refresh_pct_sfld_site,
            args=(user, passwd, host),
            lsf=dict(name='SFLD_SITE'),
            log=False
        )
    ]

    batch = Batch(tasks, dir=workdir)
    if not batch.start().wait().is_done():
        logging.critical('error while refreshing PCT table')
        return False

    with cx_Oracle.connect(user, passwd, host) as con:
        con.autocommit = 0
        cur = con.cursor()

        # Swap partitions
        databases = [
            ('CDD', 'PCT_CDD_SITE'),
            ('SFLD', 'PCT_SFLD_SITE'),
        ]

        for partition, table in databases:
            logging.info('update partition {} in IPRSCAN.SITE'.format(partition))
            cur.execute('ALTER TABLE IPRSCAN.SITE '
                        'EXCHANGE PARTITION {} WITH TABLE {} '
                        'INCLUDING INDEXES '
                        'WITHOUT VALIDATION'.format(partition, table))
        con.commit()

        # Rebuid indexes
        if parallel and parallel > 1:
            logging.info('rebuilding index SITE_UPI_AND_ANA_IDX')
            cur.execute('ALTER INDEX IPRSCAN.SITE_UPI_AND_ANA_IDX REBUILD NOLOGGING PARALLEL {}'.format(parallel))
        else:
            logging.info('rebuilding index SITE_UPI_AND_ANA_IDX')
            cur.execute('ALTER INDEX IPRSCAN.SITE_UPI_AND_ANA_IDX REBUILD NOLOGGING')

        con.commit()

    return True


def refresh(db_user, db_passwd, db_host, **kwargs):
    method = kwargs.get('method', 'C')
    parallel = kwargs.get('parallel', 1)
    queue = kwargs.get('queue')
    workdir = kwargs.get('workdir', os.getcwd())
    lsf_log = kwargs.get('log', False)

    b = refresh_mv_iprscan(
        db_user, db_passwd, db_host, method,
        workdir=workdir, queue=queue, parallel=parallel, log=lsf_log
    )

    if not b:
        return False

    b = refresh_site(
        db_user, db_passwd, db_host, method,
        workdir=workdir, queue=queue, parallel=parallel
    )

    if not b:
        return False

    return True


def protein2scan(db_user, db_passwd, db_host):
    with cx_Oracle.connect(db_user, db_passwd, db_host) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('finding and loading proteins to be scanned')
        cur.execute('TRUNCATE TABLE INTERPRO.PROTEIN_TO_SCAN')
        con.commit()

        cur.execute(
            "INSERT /*+ APPEND PARALLEL */ INTO INTERPRO.PROTEIN_TO_SCAN (PROTEIN_AC, DBCODE, TIMESTAMP, UPI) "
            "SELECT "
            "  PRO.PROTEIN_AC, "
            "  PRO.DBCODE, "
            "  PRO.TIMESTAMP, "
            "  UPD.UPI "
            "FROM INTERPRO.PROTEIN PRO "
            "LEFT OUTER JOIN ("
            "  SELECT /*+ PARALLEL INDEX_JOIN(A1 PK_PROTEIN, I_PROTEIN$CRC64) */ DISTINCT "
            "    UPX.UPI, "
            "    UPX.AC, "
            "    UPP.CRC64 "
            "  FROM UNIPARC.XREF UPX "
            "  JOIN UNIPARC.PROTEIN UPP ON (UPP.UPI = UPX.UPI) "
            "  WHERE UPX.DBID IN (2, 3)"
            ") UPD "
            "ON (UPD.AC = PRO.PROTEIN_AC AND UPD.CRC64 = PRO.CRC64)"
            "WHERE PRO.PROTEIN_AC IN ("
            "  SELECT NEW_PROTEIN_AC "
            "  FROM INTERPRO.PROTEIN_CHANGES "
            "  WHERE FLAG IN ('N', 'S')"
            ")")
        con.commit()


def check(db_user_pro, db_passwd_pro, db_host, **kwargs):
    smtp_host = kwargs.get('smtp_host')
    from_addr = kwargs.get('from_addr')
    to_addrs = kwargs.get('to_addrs', [])

    is_ready = True
    with cx_Oracle.connect(db_user_pro, db_passwd_pro, db_host) as con:
        cur = con.cursor()

        logging.info('generating IPRSCAN health check report')
        cur.execute("SELECT /*+ PARALLEL */ "
                    "  MSLR.LIBRARY, "
                    "  MSLR.VERSION, "
                    "  M.UPI, "
                    "  C.DBSHORT, "
                    "  D.VERSION, "
                    "  U.UPI "
                    "FROM "
                    "  IPRSCAN.MV_SIGNATURE_LIBRARY_RELEASE MSLR, "
                    "  INTERPRO.CV_DATABASE C, "
                    "  INTERPRO.DB_VERSION D, "
                    "  INTERPRO.IPRSCAN2DBCODE I2D, "
                    "  ("
                    "    SELECT MAX(UPI) UPI "
                    "    FROM INTERPRO.PROTEIN_TO_SCAN"
                    "  ) U,"
                    "  ("
                    "    SELECT ANALYSIS_ID, MAX(UPI) UPI "
                    "    FROM IPRSCAN.MV_IPRSCAN "
                    "    GROUP BY ANALYSIS_ID"
                    "  ) M "
                    "WHERE I2D.DBCODE = D.DBCODE "
                    "AND MSLR.ID = M.ANALYSIS_ID "
                    "AND C.DBCODE = I2D.DBCODE "
                    "AND MSLR.ID = I2D.IPRSCAN_SIG_LIB_REL_ID "
                    "AND C.DBSHORT NOT IN ('GO','MEROPS','SWISSPROT','TREMBL','INTERPRO')")

        results = []
        for iprscan_db, iprscan_version, iprscan_upi, ippro_db, ippro_version, ippro_upi in cur:
            if iprscan_version != ippro_version or iprscan_upi < ippro_upi:
                is_ready = False

            results.append({
                'iprscan_db': iprscan_db,
                'iprscan_version': iprscan_version,
                'iprscan_upi': iprscan_upi,
                'ippro_db': ippro_db,
                'ippro_version': ippro_version,
                'ippro_upi': ippro_upi
            })

    # todo: do not hardcode this value
    if len(results) != 15:
        is_ready = False

    if smtp_host and from_addr and to_addrs:
        # Report IPRSCAN health check
        try:
            db_name = db_host.rsplit('/', 1)[1]
        except IndexError:
            db_name = db_host

        content = [
            'Subject: IPRSCAN Health Check on {}'.format(db_name),
            '',
            'IPRSCAN Health Check',
            '--------------------',
            '',
            '{:<20}{:<20}{:<20}{:<20}{:<20}'.format(
                'Database',
                'Version (IPRSCAN)',
                'Version (IPPRO)',
                'Max UPI (IPRSCAN)',
                'Max UPI (IPPRO)'
            ),
            '-' * 100,
            ''
        ]

        for db in sorted(results, key=lambda x: x['ippro_db']):
            content.append('{:<20}{:<20}{:<20}{:<20}{:<20}'.format(
                db['iprscan_db'],
                db['iprscan_version'],
                db['ippro_version'],
                db['iprscan_upi'],
                db['ippro_upi']
            ))

        if is_ready:
            content += [
                '',
                'Everything looks OK.'
            ]
        else:
            content += [
                '',
                'There\'s something fishy.',
                'Check that all member databases are included, '
                'that the versions between IPRSCAN and IPPRO are the same, '
                'and IPRSCAN\'s UPI are greater than or equal to IPPRO\'s.'
            ]

        utils.sendmail(
            server=smtp_host,
            subject='IPRSCAN Health Check on {}'.format(db_name),
            content='\n'.join(content) + '\n',
            from_addr=from_addr,
            to_addrs=to_addrs
        )

    if not is_ready:
        logging.critical('IPRSCAN does not look ready')
        exit(1)


def recreate_aa_iprscan(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('recreating IPRSCAN.AA_IPRSCAN')

        cur.execute('DROP MATERIALIZED VIEW IPRSCAN.AA_IPRSCAN')
        con.commit()

        '''
        Create the view with up-to-date analysis_ids:
            3 PHOBIUS, 29 TMHMM, 30 SIGNALP_EUK, 31 SIGNALP_GRAM_POSITIVE
            32 SIGNALP_GRAM_NEGATIVE, 33 COILS, 36 PROSITE_PATTERNS,
            37 PROSITE_PROFILES, 69 MOBIDBLITE
        '''
        cur.execute("CREATE MATERIALIZED VIEW IPRSCAN.AA_IPRSCAN REFRESH FORCE ON DEMAND AS ("
                    "  SELECT "
                    "    UPI, "
                    "    ANALYSIS_ID AS LIBRARY_ID, "
                    "    METHOD_AC AS SIGNATURE, "
                    "    SEQ_START, "
                    "    SEQ_END, "
                    "    SEQ_FEATURE "
                    "  FROM IPRSCAN.MV_IPRSCAN PARTITION(TMHMM) "
                    "  UNION "
                    "  SELECT "
                    "    UPI, "
                    "    ANALYSIS_ID AS LIBRARY_ID, "
                    "    METHOD_AC AS SIGNATURE, "
                    "    SEQ_START, "
                    "    SEQ_END, "
                    "    SEQ_FEATURE "
                    "  FROM IPRSCAN.MV_IPRSCAN PARTITION(SIGNALP_EUK) "
                    "  UNION "
                    "  SELECT "
                    "    UPI, "
                    "    ANALYSIS_ID AS LIBRARY_ID, "
                    "    METHOD_AC AS SIGNATURE, "
                    "    SEQ_START, "
                    "    SEQ_END, "
                    "    SEQ_FEATURE "
                    "  FROM IPRSCAN.MV_IPRSCAN PARTITION(SIGNALP_GRAM_POSITIVE) "
                    "  UNION "
                    "  SELECT "
                    "    UPI, "
                    "    ANALYSIS_ID AS LIBRARY_ID, "
                    "    METHOD_AC AS SIGNATURE, "
                    "    SEQ_START, "
                    "    SEQ_END, "
                    "    SEQ_FEATURE "
                    "  FROM IPRSCAN.MV_IPRSCAN PARTITION(SIGNALP_GRAM_NEGATIVE) "
                    "  UNION "
                    "  SELECT "
                    "    UPI, "
                    "    ANALYSIS_ID AS LIBRARY_ID, "
                    "    METHOD_AC AS SIGNATURE, "
                    "    SEQ_START, "
                    "    SEQ_END, "
                    "    SEQ_FEATURE "
                    "  FROM IPRSCAN.MV_IPRSCAN PARTITION(COILS) "
                    "  UNION "
                    "  SELECT "
                    "    UPI, "
                    "    ANALYSIS_ID AS LIBRARY_ID, "
                    "    METHOD_AC AS SIGNATURE, "
                    "    SEQ_START, "
                    "    SEQ_END, "
                    "    SEQ_FEATURE "
                    "  FROM IPRSCAN.MV_IPRSCAN PARTITION(PROSITE_PATTERNS) "
                    "  UNION "
                    "  SELECT "
                    "    UPI, "
                    "    ANALYSIS_ID AS LIBRARY_ID, "
                    "    METHOD_AC AS SIGNATURE, "
                    "    SEQ_START, "
                    "    SEQ_END, "
                    "    SEQ_FEATURE "
                    "  FROM IPRSCAN.MV_IPRSCAN PARTITION(PROSITE_PROFILES) "
                    "  UNION "
                    "  SELECT "
                    "    UPI, "
                    "    ANALYSIS_ID AS LIBRARY_ID, "
                    "    METHOD_AC AS SIGNATURE, "
                    "    SEQ_START, "
                    "    SEQ_END, "
                    "    SEQ_FEATURE "
                    "  FROM IPRSCAN.MV_IPRSCAN PARTITION(PHOBIUS) "
                    "  WHERE METHOD_AC in ('SIGNAL_PEPTIDE','TRANSMEMBRANE') "
                    "  UNION "
                    "  SELECT "
                    "    UPI, "
                    "    ANALYSIS_ID AS LIBRARY_ID, "
                    "    METHOD_AC AS SIGNATURE, "
                    "    SEQ_START, "
                    "    SEQ_END, "
                    "    SEQ_FEATURE "
                    "  FROM IPRSCAN.MV_IPRSCAN PARTITION(MOBIDBLITE) "
                    ")")
        con.commit()

        # Create two indexes on the view
        cur.execute('CREATE INDEX IX_MV_IPRSCAN_AID ON IPRSCAN.AA_IPRSCAN(UPI)')
        cur.execute('CREATE INDEX IX_MV_IPRSCAN_SIG ON IPRSCAN.AA_IPRSCAN(SIGNATURE)')

        # Allow access for Kraken user to the view
        utils.grant(cur, 'SELECT', 'IPRSCAN.AA_IPRSCAN', 'KRAKEN')

        # Extra grants for INTERPRO_SELECT
        utils.grant(cur, 'SELECT', 'IPRSCAN.MV_PROFILE_SCAN_MATCH', 'INTERPRO_SELECT')
        utils.grant(cur, 'SELECT', 'IPRSCAN.MV_PROFILE_SCAN_LOCATION', 'INTERPRO_SELECT')
        utils.grant(cur, 'SELECT', 'IPRSCAN.MV_PATTERN_SCAN_MATCH', 'INTERPRO_SELECT')
        utils.grant(cur, 'SELECT', 'IPRSCAN.MV_PATTERN_SCAN_LOCATION', 'INTERPRO_SELECT')
        utils.grant(cur, 'SELECT', 'IPRSCAN.MV_SIGNATURE', 'INTERPRO_SELECT')


def _refresh_pct_blast_prodom(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_BLAST_PRODOM')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_BLAST_PRODOM NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_BLAST_PRODOM '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'CAST(NULL AS NUMBER) HMM_START, '
                    'CAST(NULL AS NUMBER) HMM_END, '
                    'CAST(NULL AS NUMBER) HMM_LENGTH, '
                    'CAST(NULL AS VARCHAR2(25 CHAR)) HMM_BOUNDS, '
                    'HL.SCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQSCORE, '
                    'HL.EVALUE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQEVALUE, '
                    'CAST(NULL AS NUMBER) ENVELOPE_START, '
                    'CAST(NULL AS NUMBER) ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_BLAST_PRODOM_MATCH HM, '
                    'IPRSCAN.MV_BLAST_PRODOM_LOCATION HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_cdd_site(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_CDD_SITE')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_CDD_SITE '
                    'SELECT /*+ PARALLEL */ '
                    '  UPI, '
                    '  40 AS ANALYSIS_ID, '
                    '  METHOD_AC,'
                    '  LOC_START,'
                    '  LOC_END,'
                    '  NUM_SITES,'
                    '  RESIDUE,'
                    '  RES_START,'
                    '  RES_END,'
                    '  DESCRIPTION '
                    'FROM IPRSCAN.MV_CDD_SITE')
        con.commit()


def _refresh_pct_coils(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_COILS')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_COILS NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_COILS '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'CAST(NULL AS NUMBER) HMM_START, '
                    'CAST(NULL AS NUMBER) HMM_END, '
                    'CAST(NULL AS NUMBER) HMM_LENGTH, '
                    'CAST(NULL AS VARCHAR2(25 CHAR)) HMM_BOUNDS, '
                    'CAST(NULL AS BINARY_DOUBLE) SCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQSCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) EVALUE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQEVALUE, '
                    'CAST(NULL AS NUMBER) ENVELOPE_START, '
                    'CAST(NULL AS NUMBER) ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_COILS_MATCH PARTITION(COILS) HM, '
                    'IPRSCAN.MV_COILS_LOCATION PARTITION(COILS) HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE HM.SIGNATURE_LIBRARY_RELEASE_ID = 33 '
                    'AND HL.SIGNATURE_LIBRARY_RELEASE_ID = 33 '
                    'AND S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_gene3d(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_GENE3D')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_GENE3D NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_GENE3D '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'HL.HMM_START, '
                    'HL.HMM_END, '
                    'HL.HMM_LENGTH, '
                    'HL.HMM_BOUNDS, '
                    'HL.SCORE, '
                    'HM.SCORE SEQSCORE, '
                    'HL.EVALUE, '
                    'HM.EVALUE SEQVALUE, '
                    'HL.ENVELOPE_START, '
                    'HL.ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_HMMER3_MATCH HM, '
                    'IPRSCAN.MV_HMMER3_LOCATION HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE HM.SIGNATURE_LIBRARY_RELEASE_ID = 46 '
                    'AND HL.SIGNATURE_LIBRARY_RELEASE_ID = 46 '
                    'AND S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_hamap(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_HAMAP')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_HAMAP NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_HAMAP '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'CAST(NULL AS NUMBER) HMM_START, '
                    'CAST(NULL AS NUMBER) HMM_END, '
                    'CAST(NULL AS NUMBER) HMM_LENGTH, '
                    'CAST(NULL AS VARCHAR2(25 CHAR)) HMM_BOUNDS, '
                    'HL.SCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQSCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) EVALUE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQEVALUE, '
                    'CAST(NULL AS NUMBER) ENVELOPE_START, '
                    'CAST(NULL AS NUMBER) ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_PROFILE_SCAN_MATCH HM, '
                    'IPRSCAN.MV_PROFILE_SCAN_LOCATION HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE HM.SIGNATURE_LIBRARY_RELEASE_ID = 52 '
                    'AND HL.SIGNATURE_LIBRARY_RELEASE_ID = 52 '
                    'AND S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_mobidb_lite(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_MOBIDB_LITE')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_MOBIDB_LITE NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_MOBIDB_LITE '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'CAST(NULL AS NUMBER) HMM_START, '
                    'CAST(NULL AS NUMBER) HMM_END, '
                    'CAST(NULL AS NUMBER) HMM_LENGTH, '
                    'CAST(NULL AS VARCHAR2(25 CHAR)) HMM_BOUNDS, '
                    'CAST(NULL AS BINARY_DOUBLE) SCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQSCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) EVALUE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQEVALUE, '
                    'CAST(NULL AS NUMBER) ENVELOPE_START, '
                    'CAST(NULL AS NUMBER) ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_MOBIDB_LITE_MATCH HM, '
                    'IPRSCAN.MV_MOBIDB_LITE_LOCATION HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE HM.SIGNATURE_LIBRARY_RELEASE_ID = 44 '
                    'AND HL.SIGNATURE_LIBRARY_RELEASE_ID = 44 '
                    'AND S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_panther(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_PANTHER')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_PANTHER NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_PANTHER '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'CAST(NULL AS NUMBER) HMM_START, '
                    'CAST(NULL AS NUMBER) HMM_END, '
                    'CAST(NULL AS NUMBER) HMM_LENGTH, '
                    'CAST(NULL AS VARCHAR2(25 CHAR)) HMM_BOUNDS, '
                    'HM.SCORE, '
                    'HM.SCORE SEQSCORE, '
                    'HM.EVALUE, '
                    'HM.EVALUE SEQVALUE, '
                    'CAST(NULL AS NUMBER) ENVELOPE_START, '
                    'CAST(NULL AS NUMBER) ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_PANTHER_MATCH PARTITION(PANTHER111) HM, '
                    'IPRSCAN.MV_PANTHER_LOCATION PARTITION(PANTHER111) HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_pfam(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_PFAM')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_PFAM NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_PFAM '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'HL.HMM_START, '
                    'HL.HMM_END, '
                    'HL.HMM_LENGTH, '
                    'HL.HMM_BOUNDS, '
                    'HL.SCORE, '
                    'HM.SCORE SEQSCORE, '
                    'HL.EVALUE, '
                    'HM.EVALUE SEQVALUE, '
                    'HL.ENVELOPE_START, '
                    'HL.ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_HMMER3_MATCH HM, '
                    'IPRSCAN.MV_HMMER3_LOCATION HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE HM.SIGNATURE_LIBRARY_RELEASE_ID = 55 '
                    'AND HL.SIGNATURE_LIBRARY_RELEASE_ID = 55 '
                    'AND S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_phobius(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_PHOBIUS')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_PHOBIUS NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_PHOBIUS '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'CAST(NULL AS NUMBER) HMM_START, '
                    'CAST(NULL AS NUMBER) HMM_END, '
                    'CAST(NULL AS NUMBER) HMM_LENGTH, '
                    'CAST(NULL AS VARCHAR2(25 CHAR)) HMM_BOUNDS, '
                    'CAST(NULL AS BINARY_DOUBLE) SCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQSCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) EVALUE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQEVALUE, '
                    'CAST(NULL AS NUMBER) ENVELOPE_START, '
                    'CAST(NULL AS NUMBER) ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_PHOBIUS_MATCH HM, '
                    'IPRSCAN.MV_PHOBIUS_LOCATION HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_pirsf(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_PIRSF')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_PIRSF NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_PIRSF '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'HL.HMM_START, '
                    'HL.HMM_END, '
                    'HL.HMM_LENGTH, '
                    'HL.HMM_BOUNDS, '
                    'HL.SCORE, '
                    'HM.SCORE SEQSCORE, '
                    'HL.EVALUE, '
                    'HM.EVALUE SEQVALUE, '
                    'HL.ENVELOPE_START, '
                    'HL.ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_HMMER3_MATCH HM, '
                    'IPRSCAN.MV_HMMER3_LOCATION HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE HM.SIGNATURE_LIBRARY_RELEASE_ID = 56 '
                    'AND HL.SIGNATURE_LIBRARY_RELEASE_ID = 56 '
                    'AND S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_prints(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_PRINTS')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_PRINTS NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_PRINTS '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'CAST(NULL AS NUMBER) HMM_START, '
                    'CAST(NULL AS NUMBER) HMM_END, '
                    'CAST(NULL AS NUMBER) HMM_LENGTH, '
                    'HM.GRAPHSCAN HMM_BOUNDS, '
                    'HL.SCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQSCORE, '
                    'HM.EVALUE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQEVALUE, '
                    'CAST(NULL AS NUMBER) ENVELOPE_START, '
                    'CAST(NULL AS NUMBER) ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_FINGER_PRINTS_MATCH HM, '
                    'IPRSCAN.MV_FINGER_PRINTS_LOCATION HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE HM.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_prosite_patterns(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_PROSITE_PATTERNS')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_PROSITE_PATTERNS NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_PROSITE_PATTERNS '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'CAST(NULL AS NUMBER) HMM_START, '
                    'CAST(NULL AS NUMBER) HMM_END, '
                    'CAST(NULL AS NUMBER) HMM_LENGTH, '
                    'CAST(NULL AS VARCHAR2(25 CHAR)) HMM_BOUNDS, '
                    'CAST(NULL AS BINARY_DOUBLE) SCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQSCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) EVALUE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQEVALUE, '
                    'CAST(NULL AS NUMBER) ENVELOPE_START, '
                    'CAST(NULL AS NUMBER) ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_PATTERN_SCAN_MATCH HM, '
                    'IPRSCAN.MV_PATTERN_SCAN_LOCATION HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE HM.SIGNATURE_LIBRARY_RELEASE_ID = 54 '
                    'AND HL.SIGNATURE_LIBRARY_RELEASE_ID = 54 '
                    'AND S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_prosite_profiles(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_PROSITE_PROFILES')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_PROSITE_PROFILES NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_PROSITE_PROFILES '
                    'SELECT /*+ PARALLEL */ '
                    '  S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    '  PX.IDENTIFIER UPI, '
                    '  S.ACCESSION METHOD_AC, '
                    '  REL.RELNO_MAJOR,'
                    '  REL.RELNO_MINOR, '
                    '  HL.LOC_START SEQ_START, '
                    '  HL.LOC_END SEQ_END, '
                    '  CAST(NULL AS NUMBER) HMM_START, '
                    '  CAST(NULL AS NUMBER) HMM_END, '
                    '  CAST(NULL AS NUMBER) HMM_LENGTH, '
                    '  CAST(NULL AS VARCHAR2(25 CHAR)) HMM_BOUNDS, '
                    '  HL.SCORE, '
                    '  CAST(NULL AS BINARY_DOUBLE) SEQSCORE, '
                    '  CAST(NULL AS BINARY_DOUBLE) EVALUE, '
                    '  CAST(NULL AS BINARY_DOUBLE) SEQEVALUE, '
                    '  CAST(NULL AS NUMBER) ENVELOPE_START, '
                    '  CAST(NULL AS NUMBER) ENVELOPE_END '
                    'FROM '
                    '  IPRSCAN.MV_PROTEIN_XREF PX, '
                    '  IPRSCAN.MV_SIGNATURE S, '
                    '  IPRSCAN.MV_PROFILE_SCAN_MATCH PARTITION(PROSITEPROFILES20132) HM, '
                    '  IPRSCAN.MV_PROFILE_SCAN_LOCATION PARTITION(PROSITEPROFILES20132) HL, '
                    '  IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE HM.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_rpblast(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_RPSBLAST')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_RPSBLAST NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_RPSBLAST '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'CAST(NULL AS NUMBER) HMM_START, '
                    'CAST(NULL AS NUMBER) HMM_END, '
                    'CAST(NULL AS NUMBER) HMM_LENGTH, '
                    'CAST(NULL AS VARCHAR2(25 CHAR)) HMM_BOUNDS, '
                    'HL.SCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQSCORE, '
                    'HL.EVALUE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQEVALUE, '
                    'CAST(NULL AS NUMBER) ENVELOPE_START, '
                    'CAST(NULL AS NUMBER) ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_RPSBLAST_MATCH HM, '
                    'IPRSCAN.MV_RPSBLAST_LOCATION HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE S.SIGNATURE_LIBRARY_RELEASE_ID = 57 '
                    'AND S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_sfld(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_SFLD')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_SFLD NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_SFLD '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'HL.HMM_START, '
                    'HL.HMM_END, '
                    'HL.HMM_LENGTH, '
                    'HL.HMM_BOUNDS, '
                    'HL.SCORE, '
                    'HM.SCORE SEQSCORE, '
                    'HL.EVALUE, '
                    'HM.EVALUE SEQVALUE, '
                    'HL.ENVELOPE_START, '
                    'HL.ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_HMMER3_MATCH HM, '
                    'IPRSCAN.MV_HMMER3_LOCATION HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE HM.SIGNATURE_LIBRARY_RELEASE_ID = 50 '
                    'AND HL.SIGNATURE_LIBRARY_RELEASE_ID = 50 '
                    'AND S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_sfld_site(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_SFLD_SITE')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_SFLD_SITE '
                    'SELECT /*+ PARALLEL */ '
                    '  UPI, '
                    '  50 AS ANALYSIS_ID, '
                    '  METHOD_AC,'
                    '  LOC_START,'
                    '  LOC_END,'
                    '  NUM_SITES,'
                    '  RESIDUE,'
                    '  RES_START,'
                    '  RES_END,'
                    '  DESCRIPTION '
                    'FROM IPRSCAN.MV_SFLD_SITE')
        con.commit()


def _refresh_pct_signalp(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_SIGNALP')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_SIGNALP NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_SIGNALP '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'CAST(NULL AS NUMBER) HMM_START, '
                    'CAST(NULL AS NUMBER) HMM_END, '
                    'CAST(NULL AS NUMBER) HMM_LENGTH, '
                    'CAST(NULL AS VARCHAR2(25 CHAR)) HMM_BOUNDS, '
                    'HL.SCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQSCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) EVALUE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQEVALUE, '
                    'CAST(NULL AS NUMBER) ENVELOPE_START, '
                    'CAST(NULL AS NUMBER) ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_SIGNALP_MATCH HM, '
                    'IPRSCAN.MV_SIGNALP_LOCATION HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE HM.SIGNATURE_LIBRARY_RELEASE_ID IN (30, 31, 32) '
                    'AND HL.SIGNATURE_LIBRARY_RELEASE_ID IN (30, 31, 32) '
                    'AND S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()

        for analysis_id in [30, 31, 32]:
            cur.execute('DROP TABLE IPRSCAN.PCT_SIGNALP_{}'.format(analysis_id))
            # Do not bind variable as bind variables are only allowed in DML or query statements.
            # CREATE is a DDL statement.
            cur.execute('CREATE TABLE IPRSCAN.PCT_SIGNALP_{0} AS '
                        'SELECT /*+ PARALLEL */ * '
                        'FROM IPRSCAN.PCT_SIGNALP '
                        'WHERE ANALYSIS_ID = {0}'.format(analysis_id))

        con.commit()


def _refresh_pct_smart(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_SMART')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_SMART NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_SMART '
                    'SELECT /*+ PARALLEL */ '
                    '  S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    '  PX.IDENTIFIER UPI, '
                    '  S.ACCESSION METHOD_AC, '
                    '  REL.RELNO_MAJOR,'
                    '  REL.RELNO_MINOR, '
                    '  HL.LOC_START SEQ_START, '
                    '  HL.LOC_END SEQ_END, '
                    '  HL.HMM_START, '
                    '  HL.HMM_END, '
                    '  HL.HMM_LENGTH, '
                    '  HL.HMM_BOUNDS, '
                    '  HL.SCORE, '
                    '  HM.SCORE SEQSCORE, '
                    '  HL.EVALUE, '
                    '  HM.EVALUE SEQVALUE, '
                    '  CAST(NULL AS NUMBER) ENVELOPE_START, '
                    '  CAST(NULL AS NUMBER) ENVELOPE_END '
                    'FROM '
                    '  IPRSCAN.MV_PROTEIN_XREF PX, '
                    '  IPRSCAN.MV_SIGNATURE S, '
                    '  IPRSCAN.MV_HMMER2_MATCH PARTITION(SMART) HM, '
                    '  IPRSCAN.MV_HMMER2_LOCATION PARTITION(SMART) HL, '
                    '  IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE HM.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_superfamily(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_SUPERFAMILY')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_SUPERFAMILY NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_SUPERFAMILY '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'CAST(NULL AS NUMBER) HMM_START, '
                    'CAST(NULL AS NUMBER) HMM_END, '
                    'CAST(NULL AS NUMBER) HMM_LENGTH, '
                    'CAST(NULL AS VARCHAR2(25 CHAR)) HMM_BOUNDS, '
                    'CAST(NULL AS BINARY_DOUBLE) SCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQSCORE, '
                    'HM.EVALUE, '
                    'HM.EVALUE SEQEVALUE, '
                    'CAST(NULL AS NUMBER) ENVELOPE_START, '
                    'CAST(NULL AS NUMBER) ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_SUPER_FAMILY_HMMER3MATCH HM, '
                    'IPRSCAN.MV_SUPERFAMILYHMMER3_LOCATION HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_tigrfam(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_TIGRFAM')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_TIGRFAM NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_TIGRFAM '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'HL.HMM_START, '
                    'HL.HMM_END, '
                    'HL.HMM_LENGTH, '
                    'HL.HMM_BOUNDS, '
                    'HL.SCORE, '
                    'HM.SCORE SEQSCORE, '
                    'HL.EVALUE, '
                    'HM.EVALUE SEQVALUE, '
                    'HL.ENVELOPE_START, '
                    'HL.ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_HMMER3_MATCH HM, '
                    'IPRSCAN.MV_HMMER3_LOCATION HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE HM.SIGNATURE_LIBRARY_RELEASE_ID = 23 '
                    'AND HL.SIGNATURE_LIBRARY_RELEASE_ID = 23 '
                    'AND S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def _refresh_pct_tmhmm(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute('TRUNCATE TABLE IPRSCAN.PCT_TMHMM')
        con.commit()

        cur.execute('ALTER TABLE IPRSCAN.PCT_TMHMM NOLOGGING')
        con.commit()

        cur.execute('INSERT /*+ APPEND NOLOGGING PARALLEL */ INTO IPRSCAN.PCT_TMHMM '
                    'SELECT /*+ PARALLEL */ S.SIGNATURE_LIBRARY_RELEASE_ID ANALYSIS_ID, '
                    'PX.IDENTIFIER UPI, '
                    'S.ACCESSION METHOD_AC, '
                    'REL.RELNO_MAJOR,'
                    'REL.RELNO_MINOR, '
                    'HL.LOC_START SEQ_START, '
                    'HL.LOC_END SEQ_END, '
                    'CAST(NULL AS NUMBER) HMM_START, '
                    'CAST(NULL AS NUMBER) HMM_END, '
                    'CAST(NULL AS NUMBER) HMM_LENGTH, '
                    'CAST(NULL AS VARCHAR2(25 CHAR)) HMM_BOUNDS, '
                    'HL.SCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQSCORE, '
                    'CAST(NULL AS BINARY_DOUBLE) EVALUE, '
                    'CAST(NULL AS BINARY_DOUBLE) SEQEVALUE, '
                    'CAST(NULL AS NUMBER) ENVELOPE_START, '
                    'CAST(NULL AS NUMBER) ENVELOPE_END '
                    'FROM IPRSCAN.MV_PROTEIN_XREF PX, '
                    'IPRSCAN.MV_SIGNATURE S, '
                    'IPRSCAN.MV_TMHMM_MATCH HM, '
                    'IPRSCAN.MV_TMHMM_LOCATION HL, '
                    'IPRSCAN.IPRSCAN_RELEASES REL '
                    'WHERE HM.SIGNATURE_LIBRARY_RELEASE_ID = 29 '
                    'AND HL.SIGNATURE_LIBRARY_RELEASE_ID = 29 '
                    'AND S.SIGNATURE_LIBRARY_RELEASE_ID = REL.ANALYSIS_ID '
                    'AND HM.PROTEIN_ID = PX.PROTEIN_ID '
                    'AND HM.SIGNATURE_ID = S.ID '
                    'AND HL.MATCH_ID = HM.ID')
        con.commit()


def report_swissprot_changes(user, passwd, db, updates, prefix='swiss_de_report_'):
    databases = {}
    analyses = []
    for dbcode, last_id, new_id in updates:
        databases[dbcode] = (last_id, new_id)
        analyses += [last_id, new_id]

    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()
        cur.execute(
            """
            SELECT DISTINCT M.DBCODE, IPR.ANALYSIS_ID, IPR.METHOD_AC, D.TEXT, E.ENTRY_AC, E.NAME, E.ENTRY_TYPE
              FROM INTERPRO.PROTEIN P
                INNER JOIN UNIPARC.XREF UX ON P.PROTEIN_AC = UX.AC
                INNER JOIN IPRSCAN.MV_IPRSCAN_MINI IPR ON UX.UPI = IPR.UPI
                INNER JOIN INTERPRO_ANALYSIS.PROTEIN_DESC P2D ON UX.AC = P2D.PROTEIN_AC
                INNER JOIN INTERPRO_ANALYSIS.DESC_VALUE D ON P2D.DESC_ID = D.DESC_ID
                INNER JOIN INTERPRO.METHOD M ON IPR.METHOD_AC = M.METHOD_AC
                INNER JOIN INTERPRO.ENTRY2METHOD E2M ON M.METHOD_AC = E2M.METHOD_AC
                INNER JOIN INTERPRO.ENTRY E ON E2M.ENTRY_AC = E.ENTRY_AC
            WHERE P.DBCODE = 'S'
            AND P.FRAGMENT = 'N'
            AND IPR.ANALYSIS_ID IN ({})
            """.format(format(','.join([':'+str(i+1) for i in range(len(analyses))]))),
            analyses
        )

        methods = {}
        for row in cur:
            dbcode = row[0]
            analsysis_id = row[1]
            method_ac = row[2]
            descr = row[3]
            entry_ac = row[4]
            entry_name = row[5]
            entry_type = row[6]

            last_id, new_id = databases[dbcode]
            if dbcode in methods:
                db = methods[dbcode]
            else:
                db = methods[dbcode] = {}

            if method_ac in db:
                m = db[method_ac]
            else:
                m = db[method_ac] = {
                    'acc': method_ac,
                    'entry': (entry_ac, entry_name, entry_type),
                    'analyses': {
                        last_id: set(),
                        new_id: set()
                    }
                }

            m['analyses'][analsysis_id].add(descr)

        cur.execute(
            """
            SELECT DBCODE, DBSHORT
            FROM INTERPRO.CV_DATABASE 
            WHERE DBCODE IN ({})
            """.format(format(','.join([':'+str(i+1) for i in range(len(databases))]))),
            list(databases.keys())
        )

        dbnames = dict(cur.fetchall())

    for dbcode in methods:
        last_id, new_id = databases[dbcode]
        lines = []
        for method_ac in methods[dbcode]:
            m = methods[dbcode][method_ac]

            entry_ac, entry_name, entry_type = m['entry']
            last_descrs = m['analyses'][last_id]
            new_descrs = m['analyses'][new_id]
            n_last = len(last_descrs)
            n_new = len(new_descrs)

            change = '{:.1f}'.format(n_new / n_last * 100) if n_last else 'N/A'

            gained = ' | '.join(new_descrs - last_descrs)
            lost = ' | '.join(last_descrs - new_descrs)

            lines.append((method_ac, entry_ac, entry_name, entry_type, n_last, n_new, change, gained, lost))

        dbshort = dbnames[dbcode]
        with open(prefix + dbshort + '.tsv', 'wt') as fh:
            fh.write('Method\tEntry\tName\tType\t# of old descriptions\t# of new descriptions\tChange (%)\t'
                     'Descriptions gained\tDescriptions lost\n')

            for cols in sorted(lines, key=lambda x: (0 if x[3] == 'F' else 1, x[3], x[1])):
                fh.write('\t'.join(map(str, cols)) + '\n')
