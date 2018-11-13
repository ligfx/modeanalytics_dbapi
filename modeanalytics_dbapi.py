# codec: utf-8

import csv
import io
import logging
import re
import requests
import shutil
import time

_logger = logging.getLogger(__name__)

def _find_only_one(cond, seq):
    matching = [x for x in seq if cond(x)]
    if len(matching) == 0:
        return None
    if len(matching) == 1:
        return matching[0]
    raise Exception("Multiple matching values found: {}".format(matching))

def _up_to_n_times(n):
    i = 0
    while i < n:
        yield i
    i += 1
    raise Exception("Loop exceeded maximum times ({})".format(n))

def _prettify_query_run_error_message(query_run):
    msg = query_run['error_message']
    position_match = re.search(r'(?i)Position: (\d+)', msg)
    if not position_match:
        return msg + "(couldn't prettify error position)"

    position = int(position_match.group(1))
    raw_source = query_run['raw_source']
    p = 0
    charno = 0
    lineno = 0
    while p < position:
        if raw_source[p] == '\n':
            lineno += 1
            charno = 0
        else:
            charno += 1
        p += 1
    offending_line = raw_source.split("\n")[lineno]

    return (
        query_run['error_message'][:position_match.start(0)].strip() +
        "\nOn line {}:\n{}\n{}\n".format(lineno + 1, offending_line, " " * (charno - 1) + "^") +
        query_run['error_message'][position_match.end(0):].strip()
    )

class _ModeAnalyticsAPI:
    def __init__(self, **kwargs):
        self._access_token = kwargs.pop('access_token')
        self._access_token_password = kwargs.pop('access_token_password')
        self._organization = kwargs.pop('organization')

        if kwargs:
            raise Exception("Unknown arguments: {}".format(kwargs))

    def _get_absolute(self, path, **kwargs):
        _logger.debug('GET ' + path)
        return requests.get(
            'https://modeanalytics.com/' + path,
            auth=(self._access_token, self._access_token_password),
            **kwargs
        )

    def _get(self, path):
        path = 'api/{}/'.format(self._organization) + path
        return self._get_absolute(path)

    def _post(self, path, json=None):
        path = 'api/{}/'.format(self._organization) + path
        _logger.debug('POST ' + path)
        return requests.post(
            'https://modeanalytics.com/' + path,
            auth=(self._access_token, self._access_token_password),
            json=json
        )

    def _patch(self, path, json=None):
        path = 'api/{}/'.format(self._organization) + path
        _logger.debug('PATCH ' + path)
        return requests.patch(
            'https://modeanalytics.com/' + path,
            auth=(self._access_token, self._access_token_password),
            json=json
        )

    def get_spaces(self):
        return self._get('spaces').json()['_embedded']['spaces']

    def get_reports(self, space_token):
        return self._get('spaces/{}/reports'.format(space_token)).json()['_embedded']['reports']

    def get_space_token(self, space_name):
        space = _find_only_one(
            lambda x: x['name'] == space_name,
            self.get_spaces()
        )
        if not space:
            raise Exception("couldn't find space `{}`".format(space_name))
        return space['token']

    def get_report_token_raw(self, space_token, report_name):
        report = _find_only_one(
            lambda x: x['name'] == report_name,
            self.get_reports(space_token)
        )
        if not report:
            raise Exception("couldn't find report `{}`".format(report_name))
        return report['token']

    def get_report_token(self, report_path):
        (space_name, report_name) = report_path.split('/', 1)
        space_token = self.get_space_token(space_name)
        report_token = self.get_report_token_raw(space_token, report_name)
        _logger.info("Mapped report '{}' to '{}'".format(report_path, report_token))
        return report_token

    def get_report_queries(self, report_token):
        queries = []
        for q in self._get('reports/{}/queries'.format(report_token)).json()['_embedded']['queries']:
            nq = {}
            for (k, v) in q.items():
                if k.startswith('_'): continue
                nq[k] = v
            queries.append(nq)
        _logger.info("Got queries for report '{}': {}".format(report_token, [_["token"] for _ in queries]))
        return queries

    def get_report_single_query(self, report_token):
        queries = self.get_report_queries(report_token)
        if len(queries) != 1:
            raise Exception("Expected to find a single query object, but got {}".format(len(queries)))
        return queries[0]

    def patch_query(self, report_token, query_object):
        query_token = query_object['token']
        resp = self._patch('reports/{}/queries/{}'.format(report_token, query_token), {"query":query_object})
        if not resp.ok:
            raise Exception(resp, resp.text)
        _logger.info("Patched query '{}' for report '{}'".format(query_token, report_token))

    def create_report_run(self, report_token):
        resp = self._post('reports/{}/runs'.format(report_token))
        _logger.info("Created new run for report '{}', got run token '{}'".format(report_token, resp.json()['token']))
        return (report_token, resp.json()['token'])

    def get_report_run_status(self, token_tuple):
        (report_token, report_run_token) = token_tuple
        resp = self._get('reports/{}/runs/{}'.format(report_token, report_run_token))
        return resp.json()

    def report_run_status_get_csv_raw(self, report_run_status):
        href = report_run_status['_links']['content']['href']
        return self._get_absolute(href).text

    def report_run_status_get_csv_streaming(self, report_run_status):
        href = report_run_status['_links']['content']['href']
        return self._get_absolute(href, stream=True)

    def wait_for_completed_report_run_status(self, report_run_token):
        for _ in _up_to_n_times(10):
            status = self.get_report_run_status(report_run_token)
            if not status['state'] in ('enqueued', 'processing'):
                break
            _logger.info("Waiting on {}...".format(report_run_token[1]))
            time.sleep(1)
        if status['state'] == 'succeeded':
            return status
        else:
            error_messages = []
            query_runs_href = status['_links']['query_runs']['href']
            query_runs = self._get_absolute(query_runs_href).json()['_embedded']['query_runs']
            for qr in query_runs:
                error_messages.append(_prettify_query_run_error_message(qr))
            raise Exception("\n".join(error_messages))

class _ModeAnalyticsConnection:
    def __init__(self, api, placeholder_report_token, query_object):
        self._api = api
        self._placeholder_report_token = placeholder_report_token
        self._query_object = query_object

    def cursor(self):
        return _ModeAnalyticsCursor(self._api, self._placeholder_report_token, self._query_object)

    def close(self):
        return

class _ModeAnalyticsCursor:
    def __init__(self, api, placeholder_report_token, query_object):
        self._api = api
        self._placeholder_report_token = placeholder_report_token
        self._query_object = query_object

        self.description = None
        self._last_report_run_status = None
        self.rowcount = -1
        self._data = None

    def execute(self, operation):
        self._query_object['raw_query'] = operation
        self._api.patch_query(self._placeholder_report_token, self._query_object)
        self._last_report_run_status = self._api.wait_for_completed_report_run_status(
            self._api.create_report_run(self._placeholder_report_token)
        )
        self.rowcount = -1
        self.description = None # not compliant

    def fetchcsv(self, f):
        with self._api.report_run_status_get_csv_streaming(self._last_report_run_status) as r:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, f)

    def fetchall(self):
        csv_text = self._api.report_run_status_get_csv_raw(self._last_report_run_status)
        if csv_text:
            reader = csv.reader(io.StringIO(csv_text))
            self.description = [
                (colname, None, None, None, None, None, None) for colname in next(reader)
            ]
            data = list(reader)
            self.rowcount = len(data)
            return data
        else:
            self.description = []
            self.rowcount = 0
            return []

    def close(self):
        return


def connect(**kwargs):
    placeholder_report_name = kwargs.pop('placeholder_report_name')
    api = _ModeAnalyticsAPI(**kwargs)
    placeholder_report_token = api.get_report_token(placeholder_report_name)
    query_object = api.get_report_single_query(placeholder_report_token)
    return _ModeAnalyticsConnection(api, placeholder_report_token, query_object)

