#!/usr/bin/env python
# -*- coding: utf-8 -*-

import importlib
import pickle
import struct
import sys


def main():
    with open(sys.argv[1], 'rb') as fh:
        k, l, = struct.unpack('<2I', fh.read(8))

        dirname = fh.read(k).decode()
        module_name = fh.read(l).decode()

        sys.path.append(dirname)
        importlib.import_module(module_name)

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
