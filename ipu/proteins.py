#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
import os

import cx_Oracle
import h5py
import numpy as np
import pyswiss
from mundone import Batch, Task

from . import utils


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s: %(levelname)s: %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)


def read_flat_file(filename, output):
    logging.info('reading {}'.format(filename))
    new_proteins, new_pairs = pyswiss.load(filename)
    logging.info('{} proteins and {} pairs read'.format(new_proteins.size, new_pairs.size))

    logging.info('writing to {}'.format(output))
    with h5py.File(output, 'w') as fh:
        grp = fh.create_group('proteins')
        grp.create_dataset('ac', data=new_proteins['ac'], compression='gzip')
        grp.create_dataset('name', data=new_proteins['name'], compression='gzip')
        grp.create_dataset('dbcode', data=new_proteins['dbcode'], compression='gzip')
        grp.create_dataset('isfrag', data=new_proteins['isfrag'], compression='gzip')
        grp.create_dataset('crc64', data=new_proteins['crc64'], compression='gzip')
        grp.create_dataset('len', data=new_proteins['len'], compression='gzip')
        grp.create_dataset('taxid', data=new_proteins['taxid'], compression='gzip')
        grp.create_dataset('year', data=new_proteins['year'], compression='gzip')
        grp.create_dataset('month', data=new_proteins['month'], compression='gzip')
        grp.create_dataset('day', data=new_proteins['day'], compression='gzip')

        grp = fh.create_group('pairs')
        grp.create_dataset('ac', data=new_pairs['ac'], compression='gzip')
        grp.create_dataset('sec', data=new_pairs['sec'], compression='gzip')

    return int(new_proteins.size)


def dump_proteins(user, passwd, db, output):
    logging.info('loading proteins from INTERPRO.PROTEIN')
    with cx_Oracle.connect(user, passwd, db) as con:
        cur = con.cursor()
        cur.execute('SELECT COUNT(*) FROM INTERPRO.PROTEIN')
        cnt = cur.fetchone()[0]

        proteins = np.empty(cnt, dtype=[
            ('ac', 'S15'),
            ('name', 'S16'),
            ('dbcode', 'S1'),
            ('isfrag', 'S1'),
            ('crc64', 'S16'),
            ('len', 'int32'),
            ('taxid', 'int32')
        ])

        cur.execute('SELECT PROTEIN_AC, NAME, DBCODE, FRAGMENT, CRC64, LEN, TAX_ID FROM INTERPRO.PROTEIN')
        for i, row in enumerate(cur):
            proteins[i] = row

        logging.info('{} proteins loaded'.format(proteins.size))

    logging.info('writing to {}'.format(output))
    with h5py.File(output, 'w') as fh:
        grp = fh.create_group('proteins')
        grp.create_dataset('ac', data=proteins['ac'].astype('S15'), compression='gzip')
        grp.create_dataset('name', data=proteins['name'].astype('S16'), compression='gzip')
        grp.create_dataset('dbcode', data=proteins['dbcode'].astype('S1'), compression='gzip')
        grp.create_dataset('isfrag', data=proteins['isfrag'].astype('S1'), compression='gzip')
        grp.create_dataset('crc64', data=proteins['crc64'].astype('S16'), compression='gzip')
        grp.create_dataset('len', data=proteins['len'], compression='gzip')
        grp.create_dataset('taxid', data=proteins['taxid'], compression='gzip')


def merge_h5(inputs, output):
    handlers = [h5py.File(f, 'r') for f in inputs]

    with h5py.File(output, 'w') as fho:
        grp = fho.create_group('proteins')
        for dset in ('ac', 'name', 'dbcode', 'isfrag', 'crc64', 'len', 'taxid', 'year', 'month', 'day'):
            grp.create_dataset(
                dset,
                data=np.concatenate([fh['proteins/' + dset].value for fh in handlers]),
                compression='gzip'
            )

        grp = fho.create_group('pairs')
        for dset in ('ac', 'sec'):
            grp.create_dataset(
                dset,
                data=np.concatenate([fh['pairs/' + dset].value for fh in handlers]),
                compression='gzip'
            )

    for fh in handlers:
        fh.close()


def insert(old_h5, new_h5, db_user, db_passwd, db_host, **kwargs):
    chunksize = kwargs.get('chunksize', 1000000)

    changes = {
        'deleted': 0,
        'merged': 0,
        'new': 0,
        'sequence': 0,
        'annotation': 0
    }

    with h5py.File(old_h5, 'r') as fh1, h5py.File(new_h5, 'r') as fh2:
        old_ac = fh1['proteins/ac'].value
        new_ac = fh2['proteins/ac'].value
        new_sec = fh2['pairs/sec'].value

        if new_ac.size != np.unique(new_ac).size:
            logging.critical('duplicated entries in {}'.format(new_ac))
            exit(1)

        # Find deleted proteins
        logging.info('finding deleted proteins')
        deleted = np.setdiff1d(np.setdiff1d(old_ac, new_ac, assume_unique=True), new_sec, assume_unique=True)
        changes['deleted'] = deleted.size
        logging.info('{} deleted proteins'.format(deleted.size))

        # Find newly merged proteins
        logging.info('finding merged proteins')
        merged = np.intersect1d(np.setdiff1d(new_sec, new_ac, assume_unique=True), old_ac, assume_unique=True)
        changes['merged'] = merged.size
        logging.info('{} merged proteins'.format(merged.size))

        # Find new proteins
        logging.info('finding new proteins')
        new = np.setdiff1d(new_ac, old_ac, assume_unique=True)
        changes['new'] = new.size
        logging.info('{} new proteins'.format(merged.size))

        # Find indices for non-new proteins in both arrays
        logging.info('joining/sorting proteins')
        mask1 = np.in1d(old_ac, new_ac, assume_unique=True)
        mask2 = np.in1d(new_ac, old_ac, assume_unique=True)

        # Get the indices that would sort the arrays such as old_ac[mask1][x1] = new_ac[mask2][x2]
        x1 = np.argsort(old_ac[mask1])
        x2 = np.argsort(new_ac[mask2])

        old_ac = old_ac[mask1][x1]

        # Find sequence changes
        logging.info('finding sequence changes')
        old_val = fh1['proteins/crc64'].value[mask1][x1]
        new_val = fh2['proteins/crc64'].value[mask2][x2]
        crc_mask = old_val != new_val
        seq_changes = old_ac[crc_mask]
        changes['sequence'] = seq_changes.size
        logging.info('{} sequence changes'.format(seq_changes.size))

        # Keep only proteins with the same CRC64
        old_ac = old_ac[~crc_mask]

        # Find annotation changes
        logging.info('finding annotation changes')
        mask = np.zeros(old_ac.size, dtype=bool)
        for dset in ('name', 'dbcode', 'isfrag', 'len', 'taxid'):
            old_val = fh1['proteins/' + dset].value[mask1][x1][~crc_mask]
            new_val = fh2['proteins/' + dset].value[mask2][x2][~crc_mask]
            mask |= old_val != new_val

        anno_changes = old_ac[mask]
        changes['annotation'] = anno_changes.size
        logging.info('{} annotation changes'.format(anno_changes.size))
        old_ac = None

        # Find changed proteins to discard unchanged ones
        logging.info('discarding unchanged proteins')
        mask = np.in1d(new_ac, np.concatenate((deleted, merged, new, seq_changes, anno_changes)), assume_unique=True)

        new_proteins = np.empty(np.sum(mask), dtype=[
            ('ac', 'S15'), ('name', 'S16'), ('dbcode', 'S1'), ('isfrag', 'S1'), ('crc64', 'S16'),
            ('len', 'int32'), ('year', 'int16'), ('month', 'int16'), ('day', 'int16'), ('taxid', 'int32')
        ])
        new_proteins['ac'] = new_ac[mask]
        for dset in ('name', 'dbcode', 'isfrag', 'crc64', 'len', 'year', 'month', 'day', 'taxid'):
            new_proteins[dset] = fh2['proteins/' + dset].value[mask]

        new_pairs = np.empty(fh2['pairs/ac'].len(), dtype=[('ac', 'S15'), ('sec', 'S15')])
        new_pairs['ac'] = fh2['pairs/ac'].value
        new_pairs['sec'] = fh2['pairs/sec'].value

    with cx_Oracle.connect(db_user, db_passwd, db_host) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('truncating tables')
        cur.execute('TRUNCATE TABLE INTERPRO.PROTEIN_NEW')
        cur.execute('TRUNCATE TABLE INTERPRO.PROTEIN_ACCPAIR_NEW')
        cur.execute('TRUNCATE TABLE INTERPRO.PROTEIN_CHANGES')
        cur.execute('TRUNCATE TABLE INTERPRO.PROTEIN_TO_SCAN')
        cur.execute('TRUNCATE TABLE INTERPRO.MATCH_NEW')
        con.commit()

        logging.info('populating PROTEIN_CHANGES')
        for i in range(0, deleted.size, chunksize):
            data = [('D', ac.decode()) for ac in deleted[i:i + chunksize]]
            cur.executemany('INSERT INTO INTERPRO.PROTEIN_CHANGES (FLAG, OLD_PROTEIN_AC) '
                            'VALUES (:1, :2)', data)
        con.commit()

        for i in range(0, merged.size, chunksize):
            data = [('M', ac.decode()) for ac in merged[i:i + chunksize]]
            cur.executemany('INSERT INTO INTERPRO.PROTEIN_CHANGES (FLAG, OLD_PROTEIN_AC) '
                            'VALUES (:1, :2)', data)
        con.commit()

        for i in range(0, new.size, chunksize):
            data = [('N', ac.decode()) for ac in new[i:i + chunksize]]
            cur.executemany('INSERT INTO INTERPRO.PROTEIN_CHANGES (FLAG, NEW_PROTEIN_AC) '
                            'VALUES (:1, :2)', data)
        con.commit()

        for i in range(0, seq_changes.size, chunksize):
            data = [('S', ac.decode(), ac.decode()) for ac in seq_changes[i:i + chunksize]]
            cur.executemany('INSERT INTO INTERPRO.PROTEIN_CHANGES (FLAG, OLD_PROTEIN_AC, NEW_PROTEIN_AC) '
                            'VALUES (:1, :2, :3)', data)
        con.commit()

        for i in range(0, anno_changes.size, chunksize):
            data = [('A', ac.decode(), ac.decode()) for ac in anno_changes[i:i + chunksize]]
            cur.executemany('INSERT INTO INTERPRO.PROTEIN_CHANGES (FLAG, OLD_PROTEIN_AC, NEW_PROTEIN_AC) '
                            'VALUES (:1, :2, :3)', data)
        con.commit()

        logging.info('populating PROTEIN_NEW')
        for i in range(0, new_proteins.size, chunksize):
            data = []
            for e in new_proteins[i:i + chunksize].tolist():
                ac, name, dbcode, isfrag, crc64, _len, year, month, day, taxid = e
                data.append((
                    ac.decode(),
                    name.decode(),
                    dbcode.decode(),
                    isfrag.decode(),
                    crc64.decode(),
                    _len,
                    datetime.date(year, month, day),
                    taxid
                ))

            cur.executemany('INSERT INTO INTERPRO.PROTEIN_NEW VALUES (:1, :2, :3, :4, :5, :6, :7, :8)', data)

        logging.info('populating PROTEIN_ACCPAIR_NEW')
        for i in range(0, new_pairs.size, chunksize):
            data = [(ac.decode(), sec.decode()) for ac, sec in new_pairs[i:i + chunksize].tolist()]
            cur.executemany('INSERT INTO INTERPRO.PROTEIN_ACCPAIR_NEW '
                            'VALUES (:1, :2, SYSDATE)', data)
        con.commit()

    return changes


def update_prod_tables(cnt_swiss, cnt_trembl, user, passwd, host, rel_version, rel_date, **kwargs):
    outdir = kwargs.get('outdir', os.getcwd())
    queue = kwargs.get('queue')
    workdir = kwargs.get('workdir', os.getcwd())

    if kwargs.get('iter', True):
        delete(user, passwd, host,
               outdir=os.path.join(outdir, 'delete'),
               queue=queue,
               workdir=workdir)
    else:
        delete_alt(user, passwd, host, workdir=workdir, queue=queue)

    update(user, passwd, host)

    update_db_info(user, passwd, host, cnt_swiss, cnt_trembl, rel_version, rel_date)


def delete(user, passwd, db, **kwargs):
    chunksize = kwargs.get('chunksize', 100000)
    logdir = kwargs.get('outdir')
    queue = kwargs.get('queue')
    workdir = kwargs.get('workdir', os.getcwd())

    try:
        os.makedirs(logdir)
    except FileExistsError:
        pass
    except:
        logdir = None

    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('truncating INTERPRO.DELETE_PROTEIN_STG')
        cur.execute('TRUNCATE TABLE INTERPRO.DELETE_PROTEIN_STG')
        con.commit()

        logging.info('populating INTERPRO.DELETE_PROTEIN_STG')
        cur.execute("INSERT INTO INTERPRO.DELETE_PROTEIN_STG "
                    "SELECT OLD_PROTEIN_AC, ROWNUM ID "
                    "FROM INTERPRO.PROTEIN_CHANGES "
                    "WHERE FLAG IN ('D', 'M')")
        cnt_to_delete_all = cur.rowcount
        con.commit()

        cur.execute("SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME "
                    "FROM ALL_CONSTRAINTS "
                    "WHERE CONSTRAINT_TYPE='R' "
                    "AND R_CONSTRAINT_NAME IN ("
                    "  SELECT CONSTRAINT_NAME "
                    "  FROM ALL_CONSTRAINTS "
                    "  WHERE CONSTRAINT_TYPE IN ('P', 'U') "
                    "  AND OWNER='INTERPRO' "
                    "  AND TABLE_NAME='PROTEIN'"
                    ")")
        tables = cur.fetchall()

    tables = [dict(zip(['owner', 'name', 'constraint'], t)) for t in tables]

    # Count rows to be deleted
    logging.info('counting rows to be deleted')
    tasks = []
    for t in tables:
        tasks.append(
            Task(
                fn=_count_proteins_to_delete,
                args=(user, passwd, db, t['owner'], t['name']),
                lsf=dict(name=t['name'], queue=queue),
                log=False
            )
        )

    tasks.append(
        Task(
            fn=_count_proteins_to_delete,
            args=(user, passwd, db, 'INTERPRO', 'PROTEIN'),
            lsf=dict(name='PROTEIN', queue=queue),
            log=False
        )
    )

    batch = Batch(tasks, dir=workdir)
    if not batch.start().wait().is_done():
        logging.critical('one or more tasks failed')
        exit(1)

    counts = dict(zip(
        [t['name'] for t in tables] + ['PROTEIN'],  # key: name of table
        batch.results                               # val: num of proteins to delete
    ))

    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        # Disable constraints (child tables)
        logging.info('disabling constraints')
        for t in tables:
            utils.toggle_constraint(cur, t['owner'], t['name'], t['constraint'], enable=False)

        # Disable constraints (PROTEIN table)
        constraints = utils.get_constraints(cur, 'INTERPRO', 'PROTEIN')
        for c in constraints:
            utils.toggle_constraint(cur, 'INTERPRO', 'PROTEIN', c['name'], enable=False)

        con.commit()

    # Delete rows
    logging.info('deleting rows')
    tasks = []
    for t in tables:
        logfile = os.path.join(logdir, t['name'] + '.log') if logdir else None
        tasks.append(
            Task(
                fn=_delete_iter,
                args=(user, passwd, db, t['owner'], t['name'], cnt_to_delete_all),
                kwargs=dict(chunksize=chunksize, logfile=logfile),
                lsf=dict(name=t['name'], queue=queue),
                log=False
            )
        )

    logfile = os.path.join(logdir, 'PROTEIN.log') if logdir else None
    tasks.append(
        Task(
            fn=_delete_iter,
            args=(user, passwd, db, 'INTERPRO', 'PROTEIN', cnt_to_delete_all),
            kwargs=dict(chunksize=chunksize, logfile=logfile),
            lsf=dict(name='PROTEIN', queue=queue),
            log=False
        )
    )

    batch = Batch(tasks, dir=workdir)
    if not batch.start().wait().is_done():
        logging.critical('one or more tasks failed')
        exit(1)

    counts2 = dict(zip(
        [t['name'] for t in tables] + ['PROTEIN'],  # key: name of table
        batch.results                               # val: num of proteins deleted
    ))

    for table in counts:
        counts[table] -= counts2[table]

    if any(counts.values()):  # unexpected counts: some rows were not deleted
        logging.critical('the following tables still contain deleted proteins: {}'.format(
            ', '.join([t for t, c in counts.items() if c])
        ))
        exit(1)

    logging.info('enabling constraints')

    # Enable all constraints of protein table
    utils.enable_table_constraints(user, passwd, db, 'INTERPRO', 'PROTEIN')

    # Then enable foreign-key constraints of the child tables
    tasks = []
    for t in tables:
        tasks.append(
            Task(
                fn=utils.toggle_constraint,
                args=(None, t['owner'], t['name'], t['constraint']),
                kwargs=dict(enable=True, credentials=(user, passwd, db)),
                lsf=dict(name=t['constraint'], queue=queue),
                log=False
            )
        )

    batch = Batch(tasks, dir=workdir)
    if not batch.start().wait().is_done():
        # not really sure if we should stop the workflow here
        logging.error('one or more tasks failed')


def _count_proteins_to_delete(user, passwd, db, owner, table):
    with cx_Oracle.connect(user, passwd, db) as con:
        cur = con.cursor()
        cur.execute('SELECT /*+ PARALLEL */ COUNT(*) '
                    'FROM {}.{} '
                    'WHERE PROTEIN_AC IN ('
                    '  SELECT OLD_PROTEIN_AC '
                    '  FROM INTERPRO.DELETE_PROTEIN_STG'
                    ')'.format(owner, table))
        cnt_to_delete = cur.fetchone()[0]

    print(owner, table, cnt_to_delete)

    return cnt_to_delete


def _delete_iter(user, passwd, db, owner, table, n, **kwargs):
    chunksize = kwargs.get('chunksize', 100000)
    logfile = kwargs.get('logfile')
    if not logfile:
        logfile = os.devnull

    cnt_deleted = 0

    with cx_Oracle.connect(user, passwd, db) as con, open(logfile, 'wt') as fh:
        con.autocommit = 0
        cur = con.cursor()

        i = 0
        for i in range(0, n, chunksize):
            fh.write('{:%Y-%m-%d %H:%M:%S}\t{}.{}\t{}/{}\n'.format(datetime.datetime.now(), owner, table, i, n))

            cur.execute('DELETE FROM {}.{} WHERE PROTEIN_AC IN ('
                        '  SELECT OLD_PROTEIN_AC '
                        '  FROM INTERPRO.DELETE_PROTEIN_STG '
                        '  WHERE ID >= :1 AND ID < :2'
                        ')'.format(owner, table), (i, i + chunksize))
            cnt_deleted += cur.rowcount
            con.commit()

        fh.write('{:%Y-%m-%d %H:%M:%S}\t{}.{}\tdone\n'.format(datetime.datetime.now(), owner, table))

    return cnt_deleted


def delete_alt(user, passwd, db, **kwargs):
    workdir = kwargs.get('workdir', os.getcwd())
    queue = kwargs.get('queue')

    # Disable constraints
    logging.info('disabling constraints')
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('truncating INTERPRO.DELETE_PROTEIN_STG')
        cur.execute('TRUNCATE TABLE INTERPRO.DELETE_PROTEIN_STG')
        con.commit()

        logging.info('populating INTERPRO.DELETE_PROTEIN_STG')
        cur.execute("INSERT INTO INTERPRO.DELETE_PROTEIN_STG "
                    "SELECT OLD_PROTEIN_AC, ROWNUM ID "
                    "FROM INTERPRO.PROTEIN_CHANGES "
                    "WHERE FLAG IN ('D', 'M')")
        con.commit()

        cur.execute("SELECT OLD_PROTEIN_AC "
                    "FROM INTERPRO.DELETE_PROTEIN_STG")
        deleted = frozenset([row[0] for row in cur])

        # Disable constraints (child tables)
        cur.execute("SELECT OWNER, TABLE_NAME, CONSTRAINT_NAME "
                    "FROM ALL_CONSTRAINTS "
                    "WHERE CONSTRAINT_TYPE='R' "
                    "AND R_CONSTRAINT_NAME IN ("
                    "  SELECT CONSTRAINT_NAME "
                    "  FROM ALL_CONSTRAINTS "
                    "  WHERE CONSTRAINT_TYPE IN ('P', 'U') "
                    "  AND OWNER='INTERPRO' "
                    "  AND TABLE_NAME='PROTEIN'"
                    ")")
        tables = cur.fetchall()
        for owner, table, constraint in tables:
            utils.toggle_constraint(cur, owner, table, constraint, enable=False)

        # Disable constraints (PROTEIN table)
        constraints = utils.get_constraints(cur, 'INTERPRO', 'PROTEIN')
        for c in constraints:
            utils.toggle_constraint(cur, 'INTERPRO', 'PROTEIN', c['name'], enable=False)

        con.commit()

    logging.info('dumping tables then loading data without deleted proteins')
    tasks = []

    # EXAMPLE
    table = 'EXAMPLE'
    columns = [
        ('ENTRY_AC', 'CHAR(9)'),
        ('PROTEIN_AC', 'CHAR(15)')
    ]
    tasks.append(
        Task(
            fn=utils.dump_and_load,
            args=(user, passwd, db, 'INTERPRO', table, columns, os.path.join(workdir, table + '.dat')),
            kwargs=dict(exclude=deleted, idx=1),
            lsf=dict(mem=4000, tmp=100000, name=table, queue=queue)
        )
    )

    # MATCH
    table = 'MATCH'
    columns = [
        ('PROTEIN_AC', 'CHAR(15)'),
        ('METHOD_AC', 'CHAR(25)'),
        ('POS_FROM', 'INTEGER EXTERNAL'),
        ('POS_TO', 'INTEGER EXTERNAL'),
        ('STATUS', 'CHAR(1)'),
        ('DBCODE', 'CHAR(1)'),
        ('EVIDENCE', 'CHAR(3)'),
        ('SEQ_DATE', 'DATE "YYYY-MM-DD HH24:MI:SS"'),
        ('MATCH_DATE', 'DATE "YYYY-MM-DD HH24:MI:SS"'),
        ('TIMESTAMP', 'DATE "YYYY-MM-DD HH24:MI:SS"'),
        ('USERSTAMP', 'CHAR(30)'),
        ('SCORE', 'FLOAT EXTERNAL NULLIF (SCORE = "NULL")')
    ]
    tasks.append(
        Task(
            fn=utils.dump_and_load,
            args=(user, passwd, db, 'INTERPRO', table, columns, os.path.join(workdir, table + '.dat')),
            kwargs=dict(exclude=deleted, idx=0),
            lsf=dict(mem=4000, tmp=100000, name=table, queue=queue)
        )
    )

    # MATCH_NEW
    table = 'MATCH_NEW'
    tasks.append(
        Task(
            fn=utils.dump_and_load,
            args=(user, passwd, db, 'INTERPRO', table, columns, os.path.join(workdir, table + '.dat')),
            kwargs=dict(exclude=deleted, idx=0),
            lsf=dict(mem=4000, tmp=100000, name=table, queue=queue)
        )
    )

    # MEROPS
    table = 'MEROPS'
    columns = [
        ('CODE', 'CHAR(8)'),
        ('PROTEIN_AC', 'CHAR(15)'),
        ('POS_FROM', 'INTEGER EXTERNAL'),
        ('POS_TO', 'INTEGER EXTERNAL'),
        ('NAME', 'CHAR(120)'),
        ('METHOD_AC', 'CHAR(10) NULLIF (METHOD_AC = "NULL")')
    ]
    tasks.append(
        Task(
            fn=utils.dump_and_load,
            args=(user, passwd, db, 'INTERPRO', table, columns, os.path.join(workdir, table + '.dat')),
            kwargs=dict(exclude=deleted, idx=1),
            lsf=dict(mem=4000, tmp=100000, name=table, queue=queue)
        )
    )

    # MV_ENTRY2PROTEIN
    table = 'MV_ENTRY2PROTEIN'
    columns = [
        ('ENTRY_AC', 'CHAR(9)'),
        ('PROTEIN_AC', 'CHAR(15)'),
        ('MATCH_COUNT', 'INTEGER EXTERNAL')
    ]
    tasks.append(
        Task(
            fn=utils.dump_and_load,
            args=(user, passwd, db, 'INTERPRO', table, columns, os.path.join(workdir, table + '.dat')),
            kwargs=dict(exclude=deleted, idx=1),
            lsf=dict(mem=4000, tmp=100000, name=table, queue=queue)
        )
    )

    # MV_ENTRY2PROTEIN_TRUE
    table = 'MV_ENTRY2PROTEIN_TRUE'
    tasks.append(
        Task(
            fn=utils.dump_and_load,
            args=(user, passwd, db, 'INTERPRO', table, columns, os.path.join(workdir, table + '.dat')),
            kwargs=dict(exclude=deleted, idx=1),
            lsf=dict(mem=4000, tmp=100000, name=table, queue=queue)
        )
    )

    # MV_METHOD2PROTEIN
    table = 'MV_METHOD2PROTEIN'
    columns = [
        ('METHOD_AC', 'CHAR(25)'),
        ('PROTEIN_AC', 'CHAR(15)'),
        ('MATCH_COUNT', 'INTEGER EXTERNAL')
    ]
    tasks.append(
        Task(
            fn=utils.dump_and_load,
            args=(user, passwd, db, 'INTERPRO', table, columns, os.path.join(workdir, table + '.dat')),
            kwargs=dict(exclude=deleted, idx=1),
            lsf=dict(mem=4000, tmp=100000, name=table, queue=queue)
        )
    )

    # PROTEIN_ACCPAIR
    table = 'PROTEIN_ACCPAIR'
    columns = [
        ('PROTEIN_AC', 'CHAR(15)'),
        ('SECONDARY_AC', 'CHAR(15)'),
        ('USERSTAMP', 'CHAR(30)'),
        ('TIMESTAMP', 'DATE "YYYY-MM-DD HH24:MI:SS"')
    ]
    tasks.append(
        Task(
            fn=utils.dump_and_load,
            args=(user, passwd, db, 'INTERPRO', table, columns, os.path.join(workdir, table + '.dat')),
            kwargs=dict(exclude=deleted, idx=0),
            lsf=dict(mem=4000, tmp=100000, name=table, queue=queue)
        )
    )

    # PROTEIN_ACCPAIR_NEW
    table = 'PROTEIN_ACCPAIR_NEW'
    columns = [
        ('PROTEIN_AC', 'CHAR(15)'),
        ('SECONDARY_AC', 'CHAR(15)'),
        ('TIMESTAMP', 'DATE "YYYY-MM-DD HH24:MI:SS"')
    ]
    tasks.append(
        Task(
            fn=utils.dump_and_load,
            args=(user, passwd, db, 'INTERPRO', table, columns, os.path.join(workdir, table + '.dat')),
            kwargs=dict(exclude=deleted, idx=0),
            lsf=dict(mem=4000, tmp=100000, name=table, queue=queue)
        )
    )

    # # PROTEIN_IDA
    # table = 'PROTEIN_IDA'
    # columns = [
    #     ('PROTEIN_AC', 'CHAR(15)'),
    #     ('IDA', 'CHAR(2500)')
    # ]
    # tasks.append(
    #     Task(
    #         fn=utils.dump_and_load,
    #         args=(user, passwd, db, 'INTERPRO', table, columns, os.path.join(workdir, table + '.dat')),
    #         kwargs=dict(exclude=deleted, idx=0),
    #         lsf=dict(mem=4000, tmp=100000, name=table, queue=queue)
    #     )
    # )

    # PROTEIN
    table = 'PROTEIN'
    columns = [
        ('PROTEIN_AC', 'CHAR(15)'),
        ('NAME', 'CHAR(26)'),
        ('DBCODE', 'CHAR(1)'),
        ('CRC64', 'CHAR(16)'),
        ('LEN', 'INTEGER EXTERNAL'),
        ('TIMESTAMP', 'DATE "YYYY-MM-DD HH24:MI:SS"'),
        ('USERSTAMP', 'CHAR(30)'),
        ('FRAGMENT', 'CHAR(1)'),
        ('STRUCT_FLAG', 'CHAR(1)'),
        ('TAX_ID', 'INTEGER EXTERNAL')
    ]
    tasks.append(
        Task(
            fn=utils.dump_and_load,
            args=(user, passwd, db, 'INTERPRO', table, columns, os.path.join(workdir, table + '.dat')),
            kwargs=dict(exclude=deleted, idx=0),
            lsf=dict(mem=4000, tmp=100000, name=table, queue=queue)
        )
    )

    # SITE_MATCH
    table = 'SITE_MATCH'
    columns = [
        ('PROTEIN_AC', 'CHAR(15)'),
        ('METHOD_AC', 'CHAR(255)'),
        ('LOC_START', 'INTEGER EXTERNAL'),
        ('LOC_END', 'INTEGER EXTERNAL'),
        ('DESCRIPTION', 'CHAR(255)'),
        ('RESIDUE', 'CHAR(5)'),
        ('RESIDUE_START', 'INTEGER EXTERNAL'),
        ('RESIDUE_END', 'INTEGER EXTERNAL'),
        ('NUM_SITES', 'INTEGER EXTERNAL'),
        ('DBCODE', 'CHAR(1)')
    ]
    tasks.append(
        Task(
            fn=utils.dump_and_load,
            args=(user, passwd, db, 'INTERPRO', table, columns, os.path.join(workdir, table + '.dat')),
            kwargs=dict(exclude=deleted, idx=0),
            lsf=dict(mem=4000, tmp=100000, name=table, queue=queue)
        )
    )

    # SITE_MATCH_NEW
    table = 'SITE_MATCH_NEW'
    tasks.append(
        Task(
            fn=utils.dump_and_load,
            args=(user, passwd, db, 'INTERPRO', table, columns, os.path.join(workdir, table + '.dat')),
            kwargs=dict(exclude=deleted, idx=0),
            lsf=dict(mem=4000, tmp=100000, name=table, queue=queue)
        )
    )

    # SUPERMATCH
    table = 'SUPERMATCH'
    columns = [
        ('PROTEIN_AC', 'CHAR(15)'),
        ('ENTRY_AC', 'CHAR(15)'),
        ('POS_FROM', 'INTEGER EXTERNAL'),
        ('POS_TO', 'INTEGER EXTERNAL')
    ]
    tasks.append(
        Task(
            fn=utils.dump_and_load,
            args=(user, passwd, db, 'INTERPRO', table, columns, os.path.join(workdir, table + '.dat')),
            kwargs=dict(exclude=deleted, idx=0),
            lsf=dict(mem=4000, tmp=100000, name=table, queue=queue)
        )
    )

    batch = Batch(tasks, dir=workdir)
    if not batch.start().wait().is_done():
        logging.critical('one or more tasks failed')
        exit(1)

    # Post-deletion count
    logging.info('post-deletion count')
    tasks = []
    for owner, table, constraint in tables:
        tasks.append(
            Task(
                fn=_count_proteins_to_delete,
                args=(user, passwd, db, owner, table),
                lsf=dict(name=table, queue=queue),
                log=False
            )
        )

    tasks.append(
        Task(
            fn=_count_proteins_to_delete,
            args=(user, passwd, db, 'INTERPRO', 'PROTEIN'),
            lsf=dict(name='PROTEIN', queue=queue),
            log=False
        )
    )

    batch = Batch(tasks, dir=workdir)
    if not batch.start().wait().is_done():
        logging.critical('one or more tasks failed')
        exit(1)

    counts = dict(zip(
        [table for owner, table, constraint in tables] + ['PROTEIN'],  # key: name of table
        batch.results                                                  # val: num of proteins to delete, should be 0
    ))

    for table in sorted(counts):
        logging.info('  {:<25}: {:>10} proteins to deleted'.format(table, counts[table]))

    if any(counts.values()):  # unexpected counts: some rows were not deleted
        logging.critical('one or more tables still contain deleted proteins')
        exit(1)

    # Enable all constraints of protein table
    logging.info('enabling constraints for PROTEIN tables, and child tables')
    utils.enable_table_constraints(user, passwd, db, 'INTERPRO', 'PROTEIN')

    # Then enable ALL constraints of the child tables
    # (non-foreign key constraints might have been disabled when running sqlldr)
    tasks = []
    for owner, table, constraint in tables:
        tasks.append(
            Task(
                fn=utils.enable_table_constraints,
                args=(user, passwd, db, owner, table),
                lsf=dict(name=table, queue=queue),
                log=False
            )
        )

    batch = Batch(tasks, dir=workdir)
    if not batch.start().wait().is_done():
        logging.critical('one or more tasks failed')
        exit(1)


def update(user, passwd, db, delete_merged=False):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        if delete_merged:
            # Delete merged entries -- Moved to proteins.delete()
            logging.info('deleting merged entries')
            cur.execute("DELETE /*+ PARALLEL */ FROM INTERPRO.PROTEIN P "
                        "WHERE EXISTS ("
                        "  SELECT * "
                        "  FROM INTERPRO.PROTEIN_CHANGES C "
                        "  WHERE C.FLAG = 'M' "
                        "  AND C.OLD_PROTEIN_AC = P.PROTEIN_AC"
                        ")")

            con.commit()

        # Update sequence changes
        logging.info('updating sequences')
        cur.execute("UPDATE /*+ PARALLEL */ INTERPRO.PROTEIN P "
                    "SET ("
                    "  NAME, "
                    "  DBCODE, "
                    "  FRAGMENT, "
                    "  CRC64, "
                    "  LEN, "
                    "  TAX_ID, "
                    "  TIMESTAMP, "
                    "  USERSTAMP"
                    ") = ("
                    "  SELECT "
                    "    NAME, "
                    "    DBCODE, "
                    "    FRAGMENT, "
                    "    CRC64, "
                    "    LEN, "
                    "    TAX_ID, "
                    "    SYSDATE, "
                    "    USER "
                    "  FROM "
                    "    INTERPRO.PROTEIN_NEW N, "
                    "    INTERPRO.PROTEIN_CHANGES C "
                    "  WHERE C.FLAG = 'S' "
                    "  AND C.OLD_PROTEIN_AC = P.PROTEIN_AC "
                    "  AND C.NEW_PROTEIN_AC = N.PROTEIN_AC"
                    ") "
                    "WHERE EXISTS ("
                    "  SELECT * "
                    "  FROM INTERPRO.PROTEIN_CHANGES C2 "
                    "  WHERE C2.FLAG = 'S' "
                    "  AND C2.OLD_PROTEIN_AC = P.PROTEIN_AC"
                    ")")

        con.commit()

        # Update annotation changes
        logging.info('updating annotations')
        cur.execute("UPDATE /*+ PARALLEL */ INTERPRO.PROTEIN P "
                    "SET ("
                    "  NAME, DBCODE, FRAGMENT, CRC64, LEN, TAX_ID, USERSTAMP"
                    ") = ("
                    "  SELECT NAME, DBCODE, FRAGMENT, CRC64, LEN, TAX_ID, USER "
                    "  FROM INTERPRO.PROTEIN_NEW N, INTERPRO.PROTEIN_CHANGES C "
                    "  WHERE C.FLAG = 'A' "
                    "  AND C.OLD_PROTEIN_AC = P.PROTEIN_AC "
                    "  AND C.NEW_PROTEIN_AC = N.PROTEIN_AC"
                    ") "
                    "WHERE EXISTS ("
                    "  SELECT  * "
                    "  FROM INTERPRO.PROTEIN_CHANGES C2 "
                    "  WHERE C2.FLAG = 'A' "
                    "  AND C2.OLD_PROTEIN_AC = P.PROTEIN_AC"
                    ")")

        con.commit()

        # Insert new entries
        logging.info('adding new entries')
        cur.execute("INSERT /*+ PARALLEL */ INTO INTERPRO.PROTEIN ("
                    "  PROTEIN_AC, "
                    "  NAME, "
                    "  DBCODE, "
                    "  CRC64, "
                    "  LEN, "
                    "  TIMESTAMP, "
                    "  USERSTAMP, "
                    "  FRAGMENT, "
                    "  STRUCT_FLAG, "
                    "  TAX_ID"
                    ") "
                    "SELECT "
                    "  PROTEIN_AC, "
                    "  NAME, "
                    "  DBCODE, "
                    "  CRC64, "
                    "  LEN, "
                    "  SYSDATE, "
                    "  USER, "
                    "  FRAGMENT, "
                    "  'N', "
                    "  TAX_ID "
                    "FROM "
                    "  INTERPRO.PROTEIN_NEW N, "
                    "  INTERPRO.PROTEIN_CHANGES C "
                    "WHERE C.FLAG = 'N' "
                    "AND C.NEW_PROTEIN_AC = N.PROTEIN_AC")

        con.commit()

        # Delete deleted secondary accessions
        logging.info('deleting deleted secondary accessions')
        cur.execute("DELETE /*+ PARALLEL */ "
                    "FROM INTERPRO.PROTEIN_ACCPAIR P "
                    "WHERE NOT EXISTS ("
                    "  SELECT * FROM INTERPRO.PROTEIN_ACCPAIR_NEW N "
                    "  WHERE P.PROTEIN_AC = N.PROTEIN_AC "
                    "  AND P.SECONDARY_AC = N.SECONDARY_AC"
                    ")")

        con.commit()

        # Insert new secondary accessions
        logging.info('adding new secondary accessions')
        cur.execute("INSERT /*+ PARALLEL */ INTO INTERPRO.PROTEIN_ACCPAIR ("
                    "  PROTEIN_AC, "
                    "  SECONDARY_AC, "
                    "  USERSTAMP, "
                    "  TIMESTAMP"
                    ") "
                    "SELECT PROTEIN_AC, SECONDARY_AC, USER, SYSDATE "
                    "FROM INTERPRO.PROTEIN_ACCPAIR_NEW N "
                    "WHERE NOT EXISTS ("
                    "  SELECT * "
                    "  FROM INTERPRO.PROTEIN_ACCPAIR P "
                    "  WHERE P.PROTEIN_AC = N.PROTEIN_AC "
                    "  AND P.SECONDARY_AC = N.SECONDARY_AC"
                    ")")

        con.commit()


def update_db_info(user, passwd, db, cnt_swiss, cnt_trembl, rel_version, rel_date):
    if isinstance(rel_date, str):
        rel_date = datetime.datetime.strptime(rel_date, '%d-%b-%Y')

    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        # Update UniProt
        cnt_unpirot = cnt_swiss + cnt_trembl
        cur.execute(
            'UPDATE INTERPRO.DB_VERSION '
            'SET '
            '  VERSION=:1, '
            '  ENTRY_COUNT=:2, '
            '  FILE_DATE=:3, '
            '  LOAD_DATE=SYSDATE '
            'WHERE DBCODE=:4', (rel_version, cnt_unpirot, rel_date, 'u')
        )

        # Update SwissProt
        cur.execute(
            'UPDATE INTERPRO.DB_VERSION '
            'SET '
            '  VERSION=:1, '
            '  ENTRY_COUNT=:2, '
            '  FILE_DATE=:3, '
            '  LOAD_DATE=SYSDATE '
            'WHERE DBCODE=:4', (rel_version, cnt_swiss, rel_date, 'S')
        )

        # Update TrEMBL
        cur.execute(
            'UPDATE INTERPRO.DB_VERSION '
            'SET '
            '  VERSION=:1, '
            '  ENTRY_COUNT=:2, '
            '  FILE_DATE=:3, '
            '  LOAD_DATE=SYSDATE '
            'WHERE DBCODE=:4', (rel_version, cnt_trembl, rel_date, 'T')
        )

        con.commit()


def check_crc64(user, passwd, db):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        cur.execute("SELECT COUNT(*) "
                    "FROM UNIPARC.XREF UX "
                    "INNER JOIN INTERPRO.PROTEIN P ON UX.AC = P.PROTEIN_AC "
                    "INNER JOIN UNIPARC.PROTEIN UP ON UX.UPI = UP.UPI "
                    "WHERE UX.DELETED = 'N' AND UP.CRC64 != P.CRC64")
        cnt = cur.fetchone()[0]

        logging.info('{} mismatched CRC64 in the PROTEIN table'.format(cnt))

        if cnt:
            logging.info('deletes entries for which the CRC64 (protein) does not match with Uniparc')
            cur.callproc('INTERPRO.XREF_SUMMARY_BUILD.DEL_UNIPARC_CRC_MISMATCHES')
            con.commit()

    return cnt
