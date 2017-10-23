#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pickle
import struct
import sys
sys.path.append(os.getcwd())


def main():
    with open(sys.argv[1], 'rb') as fh:
        strlen, = struct.unpack('<I', fh.read(4))

        if strlen:
            path = struct.unpack('{}s'.format(strlen), fh.read(strlen))[0].decode('utf8')
            sys.path.append(path)

        fn, args, kwargs = pickle.loads(fh.read())

    try:
        result = fn(*args, **kwargs)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        sys.stderr.write('{}, line {}: {}\n'.format(exc_type, exc_tb.tb_lineno, e))
        result = None
        status = 1
    else:
        status = 0

    with open(sys.argv[2], 'wb') as fh:
        pickle.dump(result, fh)

    exit(status)


if __name__ == '__main__':
    main()
