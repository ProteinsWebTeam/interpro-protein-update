#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import logging
import os
import tempfile
from subprocess import Popen, PIPE

import cx_Oracle


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s: %(levelname)s: %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)


def test_con(user, password, host):
    success = False
    try:
        con = cx_Oracle.connect(user, password, host)
    except cx_Oracle.DatabaseError:
        pass
    else:
        con.close()
        success = True
    finally:
        return success


def toggle_constraint(cursor, owner, table, name, enable=True, credentials=()):
    if cursor is None:
        con = cx_Oracle.connect(*credentials)
        con.autocommit = 0
        cur = con.cursor()
    else:
        cur = cursor

    stmt = 'ALTER TABLE {}.{} {} CONSTRAINT {}'.format(owner, table, 'ENABLE' if enable else 'DISABLE', name)

    try:
        cur.execute(stmt)
    except cx_Oracle.DatabaseError:
        success = False
    else:
        success = True

    logging.info('{} constraint {} in table {}.{}: {}'.format(
        'enabled' if enable else 'disabled', name, owner, table, 'done' if success else 'failed'
    ))

    if cursor is None:
        cur.close()
        con.commit()
        con.close()

    return success


def get_constraints(cursor, owner, table):
    cursor.execute("SELECT UPPER(CONSTRAINT_NAME), UPPER(STATUS) "
                   "FROM USER_CONSTRAINTS "
                   "WHERE UPPER(OWNER)=:1 "
                   "AND UPPER(TABLE_NAME)=:2", (owner.upper(), table.upper()))

    return [dict(zip(['name', 'status'], row)) for row in cursor]


def enable_table_constraints(user, passwd, db, owner, table):
    success = True
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()
        constraints = get_constraints(cur, owner, table)
        for c in constraints:
            if not toggle_constraint(cur, owner, table, c['name'], enable=True):
                success = False

    return success


def dump_table(user, passwd, db, owner, table, columns, pathname, **kwargs):
    exclude = kwargs.get('exclude', [])
    idx = kwargs.get('idx', 0)
    buffersize = kwargs.get('buffersize', 1000000)
    separator = kwargs.get('separator', '|')

    fmt = separator.join(['{}' for _ in range(len(columns))]) + '\n'

    with cx_Oracle.connect(user, passwd, db) as con, open(pathname, 'wt') as fh:
        data = []
        cur = con.cursor()

        cur.execute('SELECT {} FROM {}.{}'.format(', '.join(columns), owner, table))
        cnt1 = 0
        cnt2 = 0
        for row in cur:
            cnt1 += 1
            if row[idx] in exclude:
                continue
            cnt2 += 1

            # Convert datetime columns to string "YYYY-MM-HH HH:MM:SS" format
            _row = []

            for col in row:
                if isinstance(col, datetime.datetime):
                    _row.append(col.strftime('%Y-%m-%d %H:%M:%S'))
                elif col is None:
                    _row.append('NULL')
                else:
                    _row.append(col)
            data.append(_row)

            if not cnt2 % buffersize:
                fh.write(''.join([fmt.format(*row) for row in data]))
                data = []
                logging.info('{} entries dumped'.format(cnt2))

        if data:
            fh.write(''.join([fmt.format(*row) for row in data]))
            logging.info('{} entries dumped'.format(cnt2))

    return cnt1, cnt2


def sqlldr(user, passwd, db, owner, table, columns, data_file, separator='|', nrows=None):
    lines = [
        "LOAD DATA",
        "APPEND",
        "INTO TABLE {}.{}".format(owner, table),
        "FIELDS TERMINATED BY '{}'".format(separator),
        '(',
        ',\n'.join('{:<20}{}'.format(col_name, col_type) for col_name, col_type in columns),
        ')',
        ''
    ]

    fd, ctl_file = tempfile.mkstemp(suffix='.ctl')
    os.close(fd)
    fd, log_file = tempfile.mkstemp(suffix='.log')
    os.close(fd)
    fd, bad_file = tempfile.mkstemp(suffix='.bad')
    os.close(fd)
    fd, discard_file = tempfile.mkstemp(suffix='.dis')
    os.close(fd)

    with open(ctl_file, 'wt') as fh:
        fh.write('\n'.join(lines))

    # SQL*Loader does not handle well the connection string host:port/service: use only the service
    args = [
        'sqlldr',
        '{}@{}'.format(user, db.split('/')[-1]),
        'CONTROL={}'.format(ctl_file),
        'LOG={}'.format(log_file),
        'BAD={}'.format(bad_file),
        'DISCARD={}'.format(discard_file),
        'DATA={}'.format(data_file),
        'SILENT=ALL',
        'DIRECT=TRUE',
        'ERRORS=0'
    ]

    if nrows:
        args.append('ROWS={}'.format(nrows))

    # logging.info('\t' + ' '.join(args))

    p1 = Popen(['echo', passwd], stdout=PIPE)
    p2 = Popen(args, stdin=p1.stdout, stdout=PIPE, stderr=PIPE)
    out, err = p2.communicate()

    with open(log_file, 'rt') as fh:
        log = fh.read()

    with open(bad_file, 'rt') as fh:
        bad = fh.read()

    with open(discard_file, 'rt') as fh:
        discard = fh.read()

    for f in (ctl_file, log_file, bad_file, discard_file):
        os.unlink(f)

    return err, log, bad, discard


def dump_and_load(user, passwd, db, owner, table, columns, pathname, **kwargs):
    exclude = kwargs.get('exclude', [])
    idx = kwargs.get('idx', 0)
    buffersize = kwargs.get('buffersize', 1000000)
    separator = kwargs.get('separator', '|')

    # columns: [(name, type), ...]

    logging.info('dumping data from {}.{} to {}'.format(owner, table, pathname))
    n1, n2 = dump_table(
        user, passwd, db,
        owner, table, [col_name for col_name, col_type in columns],
        pathname,
        exclude=exclude, idx=idx, buffezsize=buffersize, separator=separator
    )

    logging.info('{} entries out of {} dumped'.format(n2, n1))

    logging.info('truncating {}.{}'.format(owner, table))
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()
        cur.execute('TRUNCATE TABLE {}.{}'.format(owner, table))
        con.commit()

    if n2:
        logging.info('loading data to {}.{}'.format(owner, table))
        err, log, bad, discard = sqlldr(user, passwd, db, owner, table, columns, pathname, separator=separator)
    else:
        err = ''
        log = ''
        bad = ''
        discard = ''

    logging.info(err)
    logging.info(log)
    logging.info(bad)
    logging.info(discard)

    os.unlink(pathname)

    return err, log, bad, discard


def refresh_materialized_view(user, passwd, db, table, method='F'):
    with cx_Oracle.connect(user, passwd, db) as con:
        con.autocommit = 0
        cur = con.cursor()

        logging.info('refreshing {}'.format(table))

        cur.execute('SELECT COUNT(*) FROM {}'.format(table))
        cnt_before = cur.fetchone()[0]

        cur.callproc('DBMS_MVIEW.REFRESH', (table, method, '', True, False, 0, 0, 0, False, False))
        con.commit()

        cur.execute('SELECT COUNT(*) FROM {}'.format(table))
        cnt_after = cur.fetchone()[0]

        logging.info('refresh complete (before: {}, after: {})'.format(cnt_before, cnt_after))


def get_indexes(cursor, owner, table):
    cursor.execute("SELECT UPPER(INDEX_NAME), UPPER(STATUS) "
                   "FROM USER_INDEXES "
                   "WHERE UPPER(TABLE_OWNER)=:1 "
                   "AND UPPER(TABLE_NAME)=:2 "
                   "AND INDEX_TYPE='NORMAL'", (owner.upper(), table.upper()))

    return [dict(zip(['name', 'status'], row)) for row in cursor]


def rebuild_index(cursor, owner, name, hint=''):
    try:
        cursor.execute('ALTER INDEX {}.{} REBUILD {}'.format(owner, name, hint))
    except cx_Oracle.DatabaseError:
        return False
    else:
        return True