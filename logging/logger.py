# Log class that writes to elastic search

__author__ = "William Gazeley"
__email__ = "william.gazeley@loratechai.com"

import json
from elasticsearch import Elasticsearch
from pkg_resources import resource_filename
import logging
import os
import uuid
from datetime import datetime, timedelta


class ESLogger():
    """
    Logger object. One per instance of droid.
    """

    def __init__(self):
        """
        Initialises a connection to the ES cluster and gets loads the log template.
        """
        try:
            self.es = self.connect_to_es()
        except Exception as e:
            logging.exception(e)

        with open(resource_filename(__name__, 'log.json')) as f:
            self.log_template = json.load(f)
            self.log_template["id"] = str(uuid.uuid4())

    def log(self, task, subtask, start_time, ticker, currency, end_time=None, severity=None, error_message=None):
        """
        Builds and sends a log to the ES cluster. Times are stored in UTC+8.

        Args:
            task (str): The task that's being done. (e.g. Ingestion)
            subtask (str): The subtask that's being done. (e.g. update_data_dsws_from_dsws)
            start_time (float): Time of when the subtask started in iso format.
            ticker (str): Ticker.
            currency (str): Currency type.
            end_time (float, optional): Datetime of when the subtask ended in epoch time. If None, task never finished. Defaults to None.
            severity (int, optional): Severity of error. 1-4. Defaults to None.
            error_message (str, optional): Error message. Defaults to None.
        """
        # Build the log
        start_time = (datetime.utcfromtimestamp(start_time) + timedelta(hours=8)).isoformat()
        if end_time != None: end_time = (datetime.utcfromtimestamp(end_time) + timedelta(hours=8)).isoformat()
        log = self.log_template
        log.update(pair for pair in locals().items() if pair[0] in log.keys())
        print(json.dumps(log, indent=4))

        # Put log into elastic search
        # NOTE: Input validation will be handled by elastic search
        try:
            res = self.es.index(index='droidv2-logs', document=log)
        except Exception as e:
            logging.exception(e)
            return
        
        logging.info(res)
        return(res)

    def connect_to_es(self):
        """
        Builds and returns a connection to ElasticSearch

        Args:
            service (str): cloud service hosting the server

        Returns:
            Elastic search connection obj: Pretty descriptive.
        """
        try:
            es = Elasticsearch(hosts=[os.getenv("ES_HOST")],
                               http_auth=(os.getenv("ES_USERNAME"),
                                          os.getenv("ES_PASSWORD")))
        except Exception as e:
            raise (e)

        return es
