#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import time


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s: %(levelname)s: %(message)s',
    datefmt='%y-%m-%d %H:%M:%S'
)


class Batch(object):
    """A batch objects represent a collection of independent :py:class:`Task`.

    :param tasks: tasks to run.
    :type tasks: list or tuple
    :param kwargs: keyword arguments (*dir*: working directory)
    """

    def __init__(self, tasks, **kwargs):
        self.tasks = tasks
        self.results = []
        self.workdir = kwargs.get('dir')

    def start(self):
        """Start all tasks.
        
        :return: self
        """
        for t in self.tasks:
            if t.name:
                logging.info("task '{}' is now running".format(t.name))

            t.start(dir=self.workdir)

        return self

    def wait(self, secs=60):
        """Blocks until all tasks have terminated.

        :param secs: number of seconds to wait between checks.
        :type secs: int
        :return: self
        """
        resuts = []
        terminated = [False] * len(self.tasks)

        while len(resuts) < len(self.tasks):
            time.sleep(secs)
            resuts = []

            for i, task in enumerate(self.tasks):
                if task.has_terminated():
                    resuts.append(task.collect())

                    if not terminated[i] and task.name:
                        terminated[i] = True

                        if task.is_done():
                            logging.info("task '{}' has terminated".format(task.name))
                        else:
                            logging.error("task '{}' has failed".format(task.name))

        self.results = resuts
        return self

    def is_done(self):
        """Verifies whether all tasks have successfully terminated.

        :return: `True` or `False`.
        :rtype: bool
        """
        return all([t.is_done() for t in self.tasks])
