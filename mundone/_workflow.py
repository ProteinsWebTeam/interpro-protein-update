#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
import sqlite3
import time

import mundone._task as tsk


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s: %(levelname)s: %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)


class Workflow:
    """A workflow object represent a collection of :py:class:`Task` that might depend on each other.

    :param tasks: tasks to run.
    :param kwargs: keyword arguments (*dir*: working directory; *db*: file path of the SQLite database that contains results).

    """
    def __init__(self, tasks, **kwargs):
        self.db = kwargs.get('db')
        self.workdir = kwargs.get('dir')
        self.tasks = {}

        try:
            os.makedirs(self.workdir)
        except FileExistsError:
            pass
        except (AttributeError, PermissionError, TypeError):
            self.workdir = None

        if isinstance(self.db, str):
            if os.path.isfile(self.db):
                with open(self.db, 'rb') as fh:
                    if fh.read(16).decode() != 'SQLite format 3\x00':
                        # Not an SQLite file
                        self.db = tsk.mktemp(suffix='.db', dir=self.workdir)
            else:
                try:
                    open(self.db, 'w').close()
                except (FileNotFoundError, PermissionError):
                    # Cannot create the file here
                    self.db = tsk.mktemp(suffix='.db', dir=self.workdir)
                else:
                    os.unlink(self.db)
        else:
            self.db = tsk.mktemp(suffix='.db', dir=self.workdir)

        logging.info('working directory: ' + os.path.dirname(self.db))  # throws a TypeError if workdir is None
        logging.info('step/run database: ' + self.db)

        self.tasks = self._init_db(self.db, tasks)
        self.active = True

    @staticmethod
    def _init_db(db, tasks):
        """
        
        :param db: 
        :param tasks: 
        :return: 
        """
        if not all([isinstance(t.name, str) for t in tasks]):
            logging.critical('missing or invalid name for at least one task (excepts a string)')
            exit(1)
        elif len(tasks) != len(set([t.name for t in tasks])):
            logging.critical('multiple tasks with the same name')
            exit(1)

        con = sqlite3.connect(db)
        cur = con.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS task (
              id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL UNIQUE
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS run (
              task_id INTEGER NOT NULL,
              active INTEGER NOT NULL DEFAULT 1,
              status INTEGER DEFAULT NULL,
              infile TEXT DEFAULT NULL,
              outfile TEXT DEFAULT NULL,
              result TEXT DEFAULT NULL,
              create_time TEXT NOT NULL,
              start_time TEXT DEFAULT NULL,
              end_time TEXT DEFAULT NULL,
              FOREIGN KEY(task_id) REFERENCES task(id)
            )
            """
        )

        cur.execute('SELECT id, name FROM task')
        db_tasks = {name: task_id for task_id, name in cur}
        d_tasks = {}

        # Add step to DB
        for t in tasks:
            if t.name in db_tasks:
                task_id = db_tasks[t.name]
            else:
                cur.execute('INSERT INTO task (name) VALUES (?)', (t.name,))
                task_id = cur.lastrowid

            d_tasks[task_id] = t

        con.commit()
        cur.close()
        con.close()

        return d_tasks

    def run(self, task_names=list(), rerun=False, secs=60, process=True, incdep=True):
        """Run the workflow.
        
        :param task_names: task names to execute.
        :type task_names: list or tuple
        :param rerun:
        :type rerun: bool
        :param secs: number of seconds to wait between checks.
        :type secs: int
        :param process:
        :type process: bool
        :param incdep:
        :type incdep: bool
        """
        task_ids = self._init_runs(task_names, rerun=rerun, incdep=incdep, commit=process)

        if not process:
            logging.info(
                'tasks about to be processed: {}'.format(
                    ', '.join(sorted([self.tasks[task_id].name for task_id in task_ids]))
                )
            )
            return

        names2ids = {task.name: task_id for task_id, task in self.tasks.items()}

        while self.active:
            runs = self._get_runs()

            runs_started = []
            runs_terminated = []
            keep_running = False

            for task_id, task in self.tasks.items():
                try:
                    run = runs[task_id]
                except KeyError:
                    continue  # task does not have an active run

                if run['status'] == tsk.STATUS_RUNNING:
                    keep_running = True

                    if task.has_terminated():
                        if task.is_done():
                            logging.info("task '{}' has terminated".format(task.name))
                        else:
                            logging.error("task '{}' has failed".format(task.name))

                        runs_terminated.append((task_id, task.status, task.collect()))
                elif run['status'] == tsk.STATUS_PENDING:
                    keep_running = True
                    flag = 0

                    if incdep:
                        dependencies = task.requires + task.input
                    else:
                        # Even if not considering dependencies, a task cannot start if its input comes from other tasks
                        dependencies = task.input

                    for dependency_name in dependencies:
                        try:
                            dependency_run = runs[names2ids[dependency_name]]
                        except KeyError:
                            '''
                            Possible reasons:
                                - unknown dependency (should NEVER happen, as DB is populate before)
                                - no active run for dependency
                                        e.g.    first time running the workflow and ``incdep`` is ``False``
                                                this is an issue as the task will never be submitted <- TODO fix
                            '''
                            flag |= 1
                            break
                        else:
                            if dependency_run['status'] == tsk.STATUS_ERROR:
                                flag |= 2
                            elif dependency_run['status'] is None or dependency_run['status'] != tsk.STATUS_SUCCESS:
                                '''
                                Dependency is pending or running.
                                If status is `None`, it means the dependency task was not submitted yet,
                                therefore it can be considered as pending.
                                '''
                                flag |= 4

                    if flag & 1:
                        continue
                    elif flag & 2:
                        # step cannot run because one or more dependencies failed: flag this run as failed too
                        runs_terminated.append((task_id, tsk.STATUS_ERROR, None))
                    elif not flag & 4:
                        # ready to be submitted
                        args = []

                        for dependency_name in task.input:
                            dependency_run = runs[names2ids[dependency_name]]  # todo: fix since it raises a KeyError if there is no run for the task
                            args += dependency_run['output']  # output is always a list

                        task.start(input=args, dir=self.workdir)
                        logging.info("task '{}' is now running".format(task.name))
                        runs_started.append((task_id, task.infile, task.outfile))

            self.active = keep_running

            if runs_started or runs_terminated:
                self._update_runs(runs_started, runs_terminated)

            time.sleep(secs)

    def _update_runs(self, runs_started, runs_terminated):
        """
        
        :param runs_started: 
        :param runs_terminated: 
        """
        con = sqlite3.connect(self.db)
        cur = con.cursor()

        for task_id, infile, outfile in runs_started:
            cur.execute(
                "UPDATE run "
                "SET status = ?, infile = ?, outfile = ?, start_time = strftime('%Y-%m-%d %H:%M:%S') "
                "WHERE task_id = ? AND active = 1",
                (tsk.STATUS_RUNNING, infile, outfile, task_id)
            )

        for task_id, status, result in runs_terminated:
            cur.execute(
                "UPDATE run "
                "SET status = ?, result = ?, end_time = strftime('%Y-%m-%d %H:%M:%S') "
                "WHERE task_id = ? AND active = 1",
                (status, json.dumps(result), task_id)
            )

        cur.close()
        con.commit()
        con.close()

    def _get_runs(self):
        """

        :return: active runs.
        :rtype: dict
        """
        runs = {}
        con = sqlite3.connect(self.db)
        cur = con.cursor()
        cur.execute('SELECT task.id, run.status, run.result '
                    'FROM task '
                    'INNER JOIN run ON task.id = run.task_id '
                    'WHERE run.active = 1')

        for task_id, status, result in cur:
            try:
                result = json.loads(result)
            except TypeError:
                result = []
            else:
                if isinstance(result, tuple):
                    result = list(result)
                else:
                    result = [result]
            finally:
                runs[task_id] = dict(status=status, output=result)

        cur.close()
        con.close()

        return runs

    def _init_runs(self, to_run_names, rerun=False, incdep=True, commit=True):
        """
        
        :param to_run_names: 
        :param rerun:
        :type rerun: bool
        :param incdep:
        :type incdep: bool
        :param commit:
        :type commit: bool
        :return:
        :rtype: list
        """
        con = sqlite3.connect(self.db)
        cur = con.cursor()

        # Get the id/name of all existing tasks
        task_names = []
        task_ids = []
        cur.execute('SELECT id, name FROM task')
        for task_id, name in cur:
            task_ids.append(task_id)
            task_names.append(name)

        # Get the 'active' runs
        tasks_done = []
        tasks_running = []
        cur.execute(
            'SELECT task_id, status '
            'FROM run '
            'WHERE active = 1'
        )
        for task_id, status in cur:
            if status == tsk.STATUS_SUCCESS:
                tasks_done.append(task_id)
            elif status == tsk.STATUS_RUNNING:
                tasks_running.append(task_id)

        if to_run_names and isinstance(to_run_names, list) or isinstance(to_run_names, tuple):
            # todo move cast at the beginning of the method + support simple strings
            if isinstance(to_run_names, tuple):
                to_run_names = list(to_run_names)

            if not all([isinstance(name, str) for name in to_run_names]):
                logging.info('invalid name of at least one task (excepts a string)')
                exit(1)

            # Create a list of task IDs to run from the list of task names
            to_run_ids = []
            for task_id, name in zip(task_ids, task_names):
                try:
                    i = to_run_names.index(name)
                except ValueError:
                    pass
                else:
                    to_run_names.pop(i)
                    to_run_ids.append(task_id)

            if to_run_names:
                # Association name->ID incomplete
                logging.critical('unknown task names: {}'.format(', '.join(to_run_names)))
                exit(1)

            # Add dependencies
            _to_run_ids = to_run_ids
            dependencies = []

            while incdep:
                tmp = []

                for task_id in _to_run_ids:
                    task = self.tasks[task_id]

                    for name in (task.requires + task.input):
                        try:
                            i = task_names.index(name)
                        except ValueError:
                            logging.critical("task '{}' requires an unknown task ('')".format(task.name, name))
                            exit(1)
                        else:
                            dependency_id = task_ids[i]

                            if dependency_id == task_id:
                                logging.critical("task '{}' cannot requires itself".format(task.name))
                                exit(1)
                            elif dependency_id not in tasks_done or rerun:
                                tmp.append(dependency_id)

                if tmp:
                    dependencies += tmp
                    _to_run_ids = tmp
                else:
                    break

            to_run_ids = list(set(to_run_ids + dependencies))

            if set(to_run_ids) & set(tasks_running):
                logging.critical('one or more tasks are already running: {}'.format(
                    ', '.join([self.tasks[task_id].name for task_id in set(to_run_ids) & set(tasks_running)]))
                )
                exit(1)

            cur.execute(
                'UPDATE run '
                'SET active = 0 '
                'WHERE task_id IN ({})'.format(','.join(['?' for _ in to_run_ids])),
                to_run_ids
            )
        else:
            # Run all tasks, except those with the "skip" flag on
            to_run_ids = [task_id for task_id, task in self.tasks.items() if not task.skip]

            # Set as inactive all active runs (since all tasks are going to run)
            cur.execute('UPDATE run SET active = 0')

        # Add an active run for each task to execute
        cur.executemany(
            "INSERT INTO run (task_id, create_time) "
            "VALUES (?, strftime('%Y-%m-%d %H:%M:%S'))",
            [(task_id,) for task_id in to_run_ids]
        )

        cur.close()
        if commit:
            con.commit()
        con.close()
        return to_run_ids
