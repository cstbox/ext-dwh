#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'


class PendingJobsQueue(object):
    """ A persistent list holding the jobs which status is pending.
    """
    DEFAULT_PATH = "/var/db/cstbox/openrj.jobs"

    def __init__(self, path=DEFAULT_PATH):
        """
        :param str path: the path of the storage file. Will be created if not present
        """
        if not path:
            raise ValueError("path argument cannot be empty")

        self._job_ids = []

        self._path = path
        if os.path.exists(self._path):
            self.load()
        else:
            # creates a new empty list on disk
            self.save()

    def load(self):
        """ Loads the list from disk
        """
        self._job_ids = []
        for job_id in file(self._path, 'rt'):
            self._job_ids.append(job_id.strip())

    def save(self):
        """ Saves the list to disk
        """
        with file(self._path, 'wt') as fp:
            fp.writelines('\n'.join((job_id for job_id in self._job_ids)))

    def append(self, job_id):
        """ Appends a job id to the list and saves it.

        :param job_id: the id to be added
        """
        self._job_ids.append(str(job_id))
        self.save()

    def remove(self, job_id):
        """ Removes a job id from the list and saves it.

        :param job_id: the id to be removed

        :raises: ValueError if not in the list
        """
        self._job_ids.remove(str(job_id))
        self.save()

    def clear(self):
        """ Guess what...
        """
        self._job_ids = []
        self.save()

    def is_empty(self):
        return len(self._job_ids) == 0

    def items(self):
        return self._job_ids[:]

    def __contains__(self, job_id):
        return str(job_id) in self._job_ids

    def __len__(self):
        return len(self._job_ids)

    def __str__(self):
        return str(self._job_ids)