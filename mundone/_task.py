#!/usr/bin/env python
# -*- coding: utf-8 -*-

import inspect
import os
import pickle
import struct
import sys
import tempfile

from subprocess import Popen, PIPE, DEVNULL

from mundone import _runner

STATUS_PENDING = None
STATUS_RUNNING = 1
STATUS_SUCCESS = 0
STATUS_ERROR = 2


def mktemp(prefix=None, suffix=None, dir=None, isdir=False):
    """Convenient wrapper around Python's ``tempfile.mkdtemp()`` and ``tempfile.mkstemp()``.
    Creates a temporary file or directory.

    :param prefix: if *suffix* is not ``None``, the file or directory name will end with that suffix; otherwise there will be no suffix.
    :param suffix: if *prefix* is not ``None``, the file or directory name will begin with that prefix; otherwise, a default prefix is used.
    :param dir: if *dir* is not ``None``, the file or directory will be created in that directory; otherwise, a default directory is used.
    :param isdir: if *isdir* is ``True``, a temporary directory is created; otherwise, a temporary file is created.
    :return: the path name of the file/directory created.
    :rtype: str
    """
    if isdir:
        pathname = tempfile.mkdtemp(suffix=suffix, prefix=prefix, dir=dir)
    else:
        fd, pathname = tempfile.mkstemp(prefix=prefix, suffix=suffix, dir=dir)
        os.close(fd)

    return pathname


class Task(object):
    """A task object represents a job that must be completed.

    :param fn: function to run.
    :param args: arguments to pass to :py:attr:`fn`.
    :param kwargs: keywords arguments to pass to :py:attr:`fn`.
    :type kwargs: dict
    :param _kwargs: see below

    :Keyword arguments:
        * **name** -- string identifier of the task. Mandatory for :py:class:`Workflow`; otherwise, optional.
        * **requires** -- list of task names that must be successfully completed for this task to start.
        * **input** -- list of task names whose results are passed to :py:attr:`fn`.
        * **lsf** -- dictionary of LSF parameters (*queue*, *mem*, *cpu*, *tmp*).
        * **skip** -- if ``True``, the step is skipped when running the entire workflow.
        * **log** -- if a file path, logs *stdout* and *stderr* in files having *log* as prefix; if ``False``, disables the logging.


    """

    def __init__(self, fn, args=list(), kwargs=dict(), **_kwargs):
        if isinstance(args, tuple):
            args = list(args)
        elif not isinstance(args, list):
            args = [args]

        if not isinstance(kwargs, dict):
            kwargs = dict()

        self.fn = fn
        self.args = args
        self.kwargs = kwargs

        self.lsf_job_id = None
        self.proc = None
        self.status = STATUS_PENDING
        self.output = None

        self.name = _kwargs.get('name')

        if _kwargs.get('requires'):
            obj = _kwargs['requires']
            if isinstance(obj, tuple):
                obj = list(obj)
            elif not isinstance(obj, list):
                obj = [obj]

            self.requires = obj
        else:
            self.requires = []

        if _kwargs.get('input'):
            obj = _kwargs['input']
            if isinstance(obj, tuple):
                obj = list(obj)
            elif not isinstance(obj, list):
                obj = [obj]

            self.input = obj
        else:
            self.input = []

        self.lsf = _kwargs['lsf'] if _kwargs.get('lsf') and isinstance(_kwargs['lsf'], dict) else {}
        self.skip = _kwargs.get('skip', False)

        if _kwargs.get('log') and isinstance(_kwargs['log'], str):
            self.log = (_kwargs['log'] + '.out', _kwargs['log'] + '.err')
        elif _kwargs.get('log') is False:
            self.log = False
        else:
            self.log = None

        self.infile = None
        self.outfile = None

    def pack(self, input_args=list(), workdir=None):
        """

        :param input_args:
        :param workdir:
        :return:
        """
        args = input_args + self.args if isinstance(input_args, list) else self.args

        try:
            os.makedirs(workdir)
        except FileExistsError:
            pass
        except (AttributeError, PermissionError, TypeError):
            workdir = None

        self.infile = mktemp(suffix='.in.p', dir=workdir)
        self.outfile = mktemp(suffix='.out.p', dir=workdir)

        with open(self.infile, 'wb') as fh:
            module = inspect.getmodule(self.fn)
            module_path = module.__file__
            module_name = module.__name__

            for _ in range(len(module_name.split('.'))):
                module_path = os.path.dirname(module_path)

            p = pickle.dumps((self.fn, args, self.kwargs))

            if module.__name__ == '__main__':
                p = p.replace(b'c__main__', b'c' + module_name.encode())

            fh.write(struct.pack(
                '<2I{}s{}s'.format(len(module_path), len(module_name)),
                len(module_path), len(module_name), module_path.encode(), module_name.encode()
            ))

            fh.write(p)

    def start(self, **kwargs):
        """Start a task.

        :param kwargs: keyword arguments (*input*: list of additional parameters to pass to :py:attr:`fn`; *dir*: workdir directory)
        """
        input_args = kwargs.get('input', list())
        workdir = kwargs.get('dir')

        self.pack(input_args, workdir)

        if self.lsf:
            args = ['bsub']

            if self.lsf.get('queue') and isinstance(self.lsf['queue'], str):
                args += ['-q', self.lsf['queue']]

            if self.lsf.get('name') and isinstance(self.lsf['name'], str):
                args += ['-J', self.lsf['name']]
            elif self.name and isinstance(self.name, str):
                args += ['-J', self.name]

            if self.lsf.get('cpu') and isinstance(self.lsf['cpu'], int):
                args += ['-n', str(self.lsf['cpu'])]

            try:
                mem = int(self.lsf['mem'])
            except (KeyError, TypeError, ValueError):
                mem = 100
            finally:
                args += [
                    '-R', 'rusage[mem={}]'.format(mem),
                    '-M', str(mem)
                ]

            if self.lsf.get('tmp') and isinstance(self.lsf['tmp'], int):
                args += ['-R', 'rusage[tmp={}]'.format(self.lsf['tmp'])]

            if self.log is False:
                args += [
                    '-o', '/dev/null',
                    '-e', '/dev/null',
                ]
            elif self.log is not None:
                out, err = self.log
                args += [
                    '-o', out,
                    '-e', err,
                ]

            args += [
                sys.executable,
                os.path.realpath(_runner.__file__),
                self.infile,
                self.outfile
            ]

            output = Popen(args, stdout=PIPE).communicate()[0].strip().decode()

            try:
                # Expected format: Job <job_id> is submitted to queue <queue>.
                job_id = int(output.split('<')[1].split('>')[0])
            except (IndexError, ValueError):
                self.status = STATUS_ERROR
            else:
                self.lsf_job_id = job_id
                self.status = STATUS_RUNNING
        else:
            args = [
                sys.executable,
                os.path.realpath(_runner.__file__),
                self.infile,
                self.outfile
            ]

            if self.log is False:
                out = err = DEVNULL
            elif self.log is None:
                out = err = None
            else:
                out = open(self.log[0], 'wt')
                err = open(self.log[1], 'wt')
                self.log = (out, err)

            self.proc = Popen(args, stdout=out, stderr=err)
            self.status = STATUS_PENDING

    def stop(self):
        """Stops the task by killing the running process.

        """
        if self.proc is not None:
            self.proc.kill()
        elif self.lsf_job_id is not None:
            Popen(['bkill', str(self.lsf_job_id)], stdout=PIPE).communicate()[0].strip().decode()

        self.status = STATUS_ERROR
        self.clean()

    def has_terminated(self):
        """Return `True` if the task has terminated (i.e. is not running anymore).

        :return: `True` or `False`
        :rtype: bool
        """
        if self.status in (STATUS_PENDING, STATUS_RUNNING):
            self._update_status()

        return self.status in (STATUS_SUCCESS, STATUS_ERROR)

    def is_done(self):
        """Return `True` if the task has successfully terminated.

        :return: `True` or `False`
        :rtype: bool
        """
        return self.status == STATUS_SUCCESS

    def collect(self):
        """Loads the results from the output Pickle file.

        :return: result of :py:attr:`fn`.
        """
        if self.log:
            out, err = self.log
            try:
                out.close()
            except AttributeError:
                pass

            try:
                err.close()
            except AttributeError:
                pass

        try:
            with open(self.outfile, 'rb') as fh:
                result = pickle.load(fh)
        except FileNotFoundError:
            pass
        else:
            self.output = result
        finally:
            self.clean()
            return self.output

    def clean(self):
        """Deletes the input and output Pickle files. Called by :py:meth:`collect`.

        """
        try:
            os.unlink(self.infile)
        except (FileNotFoundError, TypeError):
            pass
        finally:
            self.infile = None

        try:
            os.unlink(self.outfile)
        except (FileNotFoundError, TypeError):
            pass
        finally:
            self.outfile = None

    def _update_status(self):
        """Checks the current status of the task.

        """
        if self.proc is not None:
            returncode = self.proc.poll()

            if returncode is None:
                self.status = STATUS_RUNNING
            elif returncode == 0:
                self.status = STATUS_SUCCESS
            else:
                self.status = STATUS_ERROR
        elif self.lsf_job_id is not None:
            output = Popen(['bjobs', str(self.lsf_job_id)], stdout=PIPE, stderr=PIPE).communicate()[0].strip().decode()

            status = None
            try:
                status = output.splitlines()[1].split()[2]
            except IndexError:
                pass
            finally:
                self.status = {
                    'PEND': STATUS_PENDING,
                    'RUN': STATUS_RUNNING,
                    'EXIT': STATUS_ERROR,
                    'DONE': STATUS_SUCCESS
                }.get(status, STATUS_ERROR)
                # else:
                #     self.status = _STATUS_PENDING
