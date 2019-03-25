#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import configparser
import logging
import os

from mundone import Task, Workflow

import ipu.iprscan
import ipu.matches
import ipu.feature_matches
import ipu.methods
import ipu.proteins
import ipu.utils
import ipu.xref


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s: %(levelname)s: %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)


def parse_addrs(s):
    return [addr.strip() for addr in s.split(',') if len(addr.strip())]


def main():
    parser = argparse.ArgumentParser(description='Perform the InterPro Protein Update')
    parser.add_argument('config', metavar='config.ini', help='configuration file')
    parser.add_argument('-t', '--tasks', nargs='*', help='tasks to run')
    parser.add_argument('-l', '--list', action='store_true', default=False,
                        help='list steps that would be processed, but do not process them')
    parser.add_argument('-d', '--detach', action='store_true', default=False,
                        help='do not wait for tasks to complete (only tasks without dependencies are run)')
    parser.add_argument('--nodep', action='store_true', default=False, help='do not include dependencies (run only the requested tasks)')
    parser.add_argument('--lowmem', action='store_true', default=False, help='optimized for low-resources databases')
    args = parser.parse_args()

    if not os.path.isfile(args.config):
        logging.critical("cannot open '{}': no such file or directory".format(args.config))
        exit(1)

    config = configparser.ConfigParser()
    config.read(args.config)

    db_host = None
    db_user_pro = None
    db_user_scan = None
    db_user_parc = None

    uniprot_version = None
    uniprot_date = None
    swissprot_file = None
    trembl_file = None

    outdir = None
    tmpdir = None
    tabdir = None

    queue = None

    smtp_host = None
    sender = None
    mail_interpro = None
    mail_aa = None
    mail_uniprot = None

    # Get database credentials from config file
    try:
        db_host = config['database']['host']
        db_user_pro = config['database']['user_pro'].split('/', 1)
        db_user_scan = config['database']['user_scan'].split('/', 1)
    except KeyError:
        logging.critical("could not parse the 'database' section")
        exit(1)

    if len(db_user_pro) != 2:
        logging.critical("wrong format for 'user_pro' (expect user/password)")
        exit(1)
    elif len(db_user_scan) != 2:
        logging.critical("wrong format for 'user_scan' (expect user/password)")
        exit(1)

    if args.lowmem:
        # user_parc is only used when the --lowmem option is ON (and this option should not be ON in production)
        try:
            db_user_parc = config['database']['user_parc'].split('/', 1)
        except KeyError:
            logging.critical("could not parse the 'user_parc' section")
            exit(1)

        if len(db_user_parc) != 2:
            logging.critical("wrong format for 'user_parc' (expect user/password)")
            exit(1)
        elif not ipu.utils.test_con(*db_user_parc, db_host):
            username, password = db_user_parc
            logging.critical('could not create a connection with {}/{}@{}'.format(
                username, '*' * len(password), db_host)
            )
            exit(1)

    # Test database connection
    if not ipu.utils.test_con(*db_user_pro, db_host):
        username, password = db_user_pro
        logging.critical('could not create a connection with {}/{}@{}'.format(
            username, '*' * len(password), db_host)
        )
        exit(1)
    elif not ipu.utils.test_con(*db_user_scan, db_host):
        username, password = db_user_scan
        logging.critical('could not create a connection with {}/{}@{}'.format(
            username, '*' * len(password), db_host)
        )
        exit(1)

    # Get UniProt info and flat file paths
    try:
        uniprot_version = config['UniProt']['version']
        uniprot_date = config['UniProt']['date']
        swissprot_file = config['UniProt']['swissprot_file']
        trembl_file = config['UniProt']['trembl_file']
    except KeyError:
        logging.critical("could not parse the 'UniProt' section")
        exit(1)

    # Get working directories
    try:
        outdir = config['directories']['out']
        tmpdir = config['directories']['tmp']
        tabdir = config['directories']['tab']
    except KeyError:
        logging.critical("could not parse the 'directories' section")
        exit(1)

    # Create the directories (if they do not exist)
    for d in (outdir, tmpdir, tabdir):
        try:
            os.makedirs(d)
        except FileExistsError:
            pass

    # LSF queue name
    try:
        queue = config['cluster']['queue']
    except KeyError:
        pass
    else:
        if not len(queue):
            queue = None

    # STMP credentials to send reports
    try:
        smtp_host = config['mail']['server']
        sender = config['mail']['sender']
        mail_interpro = parse_addrs(config['mail']['interpro'])
        mail_aa = parse_addrs(config['mail']['aa'])
        mail_uniprot = parse_addrs(config['mail']['uniprot'])
    except KeyError:
        logging.critical("could not parse the 'mail' section")
        exit(1)

    # workflow tasks
    tasks = [
        # Update 1A
        Task(
            name='load_swissprot',
            fn=ipu.proteins.read_flat_file,
            args=(swissprot_file, os.path.join(outdir, 'swiss.h5')),
            lsf=dict(queue=queue, mem=500),
            log=os.path.join(outdir, 'load_swissprot')
        ),
        Task(
            name='load_trembl',
            fn=ipu.proteins.read_flat_file,
            args=(trembl_file, os.path.join(outdir, 'trembl.h5')),
            lsf=dict(queue=queue, mem=30000),
            log=os.path.join(outdir, 'load_trembl')
        ),
        Task(
            name='dump_db',
            fn=ipu.proteins.dump_proteins,
            args=(*db_user_pro, db_host, os.path.join(outdir, 'db.h5')),
            lsf=dict(queue=queue, mem=16000),
            log=os.path.join(outdir, 'dump_db')
        ),
        Task(
            name='merge_h5',
            fn=ipu.proteins.merge_h5,
            requires=['load_swissprot', 'load_trembl'],
            args=(
                [os.path.join(outdir, 'swiss.h5'), os.path.join(outdir, 'trembl.h5')],
                os.path.join(outdir, 'uniprot.h5')
            ),
            lsf=dict(queue=queue, mem=5000),
            log=os.path.join(outdir, 'merge_h5')
        ),
        Task(
            name='insert_proteins',
            fn=ipu.proteins.insert,
            requires=['dump_db', 'merge_h5'],
            args=(
                os.path.join(outdir, 'db.h5'),
                os.path.join(outdir, 'uniprot.h5'),
                *db_user_pro,
                db_host
            ),
            kwargs=dict(chunksize=100000),
            lsf=dict(queue=queue, mem=32000),
            log=os.path.join(outdir, 'insert_proteins')
        ),
        Task(
            name='method_changes',
            fn=ipu.methods.find_changes,
            requires=['insert_proteins'],
            args=(*db_user_pro, db_host),
            lsf=dict(queue=queue),
            log=os.path.join(outdir, 'method_changes')
        ),

        # Update UniParc.xref table
        Task(
            name='uniparc_xref',
            fn=ipu.xref.refresh_uniparc,
            args=(*(db_user_parc if args.lowmem else db_user_pro), db_host),
            kwargs=dict(useproc=not args.lowmem),
            lsf=dict(queue=queue),
            log=os.path.join(outdir, 'uniparc_xref')
        ),

        # Update 1B
        Task(
            name='update_proteins',
            fn=ipu.proteins.update_prod_tables,
            requires=['method_changes'],
            input=['load_swissprot', 'load_trembl'],
            args=(*db_user_pro, db_host, uniprot_version, uniprot_date),
            kwargs=dict(outdir=outdir, workdir=tmpdir, queue=queue, iter=not args.lowmem),
            lsf=dict(queue=queue),
            log=os.path.join(outdir, 'update_proteins')
        ),

        # IPRSCAN is ready
        Task(
            name='iprscan_precheck',
            fn=ipu.iprscan.compare_ispro_ippro,
            args=(*db_user_scan, db_host),
            kwargs=dict(smtp_host=smtp_host, from_addr=sender, to_addrs=mail_interpro),
            lsf=dict(queue=queue),
            log=os.path.join(outdir, 'iprscan_precheck')
        ),

        # Refresh IPRSCAN with ISPRO data
        Task(
            name='iprscan_refresh',
            fn=ipu.iprscan.refresh,
            args=(*db_user_scan, db_host),
            kwargs=dict(method='C', parallel=6, queue=queue, workdir=tmpdir, log=True),
            lsf=dict(queue=queue),
            skip=True,
            log=os.path.join(outdir, 'iprscan_refresh')
        ),

        # Rebuild indexes and refresh PROTEIN_TO_SCAN
        Task(
            name='protein2scan',
            fn=ipu.iprscan.protein2scan,
            requires=['update_proteins', 'uniparc_xref'],
            args=(*db_user_pro, db_host),
            lsf=dict(queue=queue),
            log=os.path.join(outdir, 'protein2scan')
        ),

        # IPRSCAN check
        Task(
            name='iprscan_check',
            fn=ipu.iprscan.check,
            requires=['protein2scan'],
            args=(*db_user_pro, db_host),
            kwargs=dict(smtp_host=smtp_host, from_addr=sender, to_addrs=mail_interpro),
            lsf=dict(queue=queue),
            skip=True
        ),

        # Refresh Method2Swiss
        Task(
            name='method2swiss',
            fn=ipu.xref.refresh_method2swiss,
            args=(*db_user_pro, db_host),
            lsf=dict(queue=queue),
            log=os.path.join(outdir, 'method2swiss')
        ),

        # Update 2
        # todo: skip case1,2,3 counts as we don't need those anymore
        Task(
            name='prepare_matches',
            fn=ipu.matches.prepare_matches,
            requires=['protein2scan'],
            args=(*db_user_pro, db_host),
            lsf=dict(queue=queue, mem=8000),  # add_new() requires ~500M, but pre_prod might require more
            log=os.path.join(outdir, 'prepare_matches')
        ),

        Task(
            name='prepare_feature_matches',
            fn=ipu.feature_matches.prepare_feature_matches,
            requires=['protein2scan'],
            args=(*db_user_pro, db_host),
            lsf=dict(queue=queue, mem=8000),  # add_new() requires ~500M, but pre_prod might require more
            log=os.path.join(outdir, 'prepare_feature_matches')
        ),

        # Refresh AA_IPRSCAN
        Task(
            name='aa_iprscan',
            fn=ipu.iprscan.recreate_aa_iprscan,
            requires=['protein2scan'],
            args=(*db_user_pro, db_host),
            lsf=dict(queue=queue),
            log=os.path.join(outdir, 'aa_iprscan')
        ),

        # Update3
        # todo: read output from prepare* task and check for values in report
        Task(
            name='update_matches',
            fn=ipu.matches.update_matches,
            requires=['prepare_matches'],
            args=(*db_user_pro, db_host),
            lsf=dict(queue=queue),
            log=os.path.join(outdir, 'update_matches')
        ),

        Task(
            name='update_feature_matches',
            fn=ipu.feature_matches.update_feature_matches,
            requires=['prepare_feature_matches'],
            args=(*db_user_pro, db_host),
            lsf=dict(queue=queue),
            log=os.path.join(outdir, 'update_feature_matches')
        ),

        Task(
            name='finalize',
            fn=ipu.matches.finalize,
            requires=['update_matches', 'update_feature_matches'],
            input=['method_changes'],
            args=(*db_user_pro, db_host),
            kwargs=dict(
                smtp_host=smtp_host,
                from_addr=sender,
                to_addrs_1=mail_interpro,
                to_addrs_2=mail_interpro + mail_aa + mail_uniprot,
            ),
            lsf=dict(queue=queue),
            log=os.path.join(outdir, 'finalize')
        ),

        Task(
            name='refresh_go',
            fn=ipu.matches.refresh_interpro2go,
            requires=['finalize'],
            args=(*db_user_pro, db_host),
            lsf=dict(queue=queue),
            log=os.path.join(outdir, 'refresh_go')
        ),

        Task(
            name='refresh_feature_matches',
            fn=ipu.feature_matches.refresh,
            requires=['finalize'],
            args=(*db_user_pro, db_host),
            lsf=dict(queue=queue),
            log=os.path.join(outdir, 'refresh_feature_matches')
        ),

        # Check CRC64
        Task(
            name='crc64',
            fn=ipu.proteins.check_crc64,
            requires=['update_proteins', 'uniparc_xref'],
            args=(*db_user_pro, db_host),
            lsf=dict(queue=queue),
            log=os.path.join(outdir, 'crc64')
        ),

        # Update site_matches
        Task(
            name='site_match',
            fn=ipu.matches.update_site_matches,
            requires=['update_matches'],
            args=(*db_user_pro, db_host),
            lsf=dict(queue=queue),
            log=os.path.join(outdir, 'site_match')
        ),

        # XREF summary
        Task(
            name='dump_xref',
            fn=ipu.xref.dump,
            requires=['update_matches'],
            args=(*db_user_pro, db_host, tabdir),
            kwargs=dict(smtp_host=smtp_host, from_addr=sender, to_addrs=mail_interpro),
            lsf=dict(queue=queue),
            log=os.path.join(outdir, 'dump_xref')
        )
    ]

    if args.detach:
        secs = 0
        cascade_kill = False
    else:
        secs = 10
        cascade_kill = True

    w = Workflow(tasks, dir=tmpdir, db=os.path.join(outdir, 'workflow.db'), cascade_kill=cascade_kill)
    w.run(args.tasks, process=(not args.list), incdep=(not args.nodep), secs=secs)


if __name__ == '__main__':
    main()
