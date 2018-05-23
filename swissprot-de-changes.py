#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import configparser
import logging

import ipu.iprscan

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s: %(levelname)s: %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)


def main():
    parser = argparse.ArgumentParser(description='Report Swiss-Prot description changes after a member database update')
    parser.add_argument('config', metavar='config.ini', help='configuration file')
    parser.add_argument('--databases', nargs='+', required=True,
                        help='databases (format: dbcode, last_analysis_id, new_analysis_id)')
    parser.add_argument('-o', '--output', default='swiss_de_report_', help='output file prefix')
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)

    user, passwd = config['database']['user_pro'].split('/', 1)
    host = config['database']['host']

    if len(args.databases) % 3:
        logging.critical('invalid --databases argument')
        exit(1)

    updates = []
    for i in range(len(args.databases) // 3):
        updates.append((args.databases[i*3], args.databases[i*3+1], args.databases[i*3+2]))

    ipu.iprscan.report_swissprot_changes(user, passwd, host, updates, prefix=args.output)


if __name__ == '__main__':
    main()
