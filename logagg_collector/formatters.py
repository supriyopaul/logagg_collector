import re
import ujson as json
import datetime

class RawLog(dict): pass

#FIXME: cannot do both returns .. should it?
def docker_file_log_driver(line):
    log = json.loads(json.loads(line)['msg'])
    if 'formatter' in log.get('extra'):
        return RawLog(dict(formatter=log.get('extra').get('formatter'),
                            raw=log.get('message'),
                            host=log.get('host'),
                            timestamp=log.get('timestamp'),
                            )
                        )
    return dict(timestamp=log.get('timestamp'), data=log, type='log')

def nginx_access(line):
    '''
    >>> import pprint
    >>> input_line1 = '{ \
                    "remote_addr": "127.0.0.1","remote_user": "-","timestamp": "1515144699.201", \
                    "request": "GET / HTTP/1.1","status": "200","request_time": "0.000", \
                    "body_bytes_sent": "396","http_referer": "-","http_user_agent": "python-requests/2.18.4", \
                    "http_x_forwarded_for": "-","upstream_response_time": "-" \
                        }'
    >>> output_line1 = nginx_access(input_line1)
    >>> pprint.pprint(output_line1)
    {'data': {'body_bytes_sent': 396.0,
              'http_referer': '-',
              'http_user_agent': 'python-requests/2.18.4',
              'http_x_forwarded_for': '-',
              'remote_addr': '127.0.0.1',
              'remote_user': '-',
              'request': 'GET / HTTP/1.1',
              'request_time': 0.0,
              'status': '200',
              'timestamp': '2018-01-05T09:31:39.201000',
              'upstream_response_time': 0.0},
     'event': 'nginx_event',
     'timestamp': '2018-01-05T09:31:39.201000',
     'type': 'metric'}

    >>> input_line2 = '{ \
                    "remote_addr": "192.158.0.51","remote_user": "-","timestamp": "1515143686.415", \
                    "request": "POST /mpub?topic=heartbeat HTTP/1.1","status": "404","request_time": "0.000", \
                    "body_bytes_sent": "152","http_referer": "-","http_user_agent": "python-requests/2.18.4", \
                    "http_x_forwarded_for": "-","upstream_response_time": "-" \
                       }'
    >>> output_line2 = nginx_access(input_line2)
    >>> pprint.pprint(output_line2)
    {'data': {'body_bytes_sent': 152.0,
              'http_referer': '-',
              'http_user_agent': 'python-requests/2.18.4',
              'http_x_forwarded_for': '-',
              'remote_addr': '192.158.0.51',
              'remote_user': '-',
              'request': 'POST /mpub?topic=heartbeat HTTP/1.1',
              'request_time': 0.0,
              'status': '404',
              'timestamp': '2018-01-05T09:14:46.415000',
              'upstream_response_time': 0.0},
     'event': 'nginx_event',
     'timestamp': '2018-01-05T09:14:46.415000',
     'type': 'metric'}
    '''
#TODO Handle nginx error logs
    log = json.loads(line)
    timestamp_iso = datetime.datetime.utcfromtimestamp(float(log['timestamp'])).isoformat()
    log.update({'timestamp':timestamp_iso})
    if '-' in log.get('upstream_response_time'):
        log['upstream_response_time'] = 0.0
    log['body_bytes_sent'] = float(log['body_bytes_sent'])
    log['request_time'] = float(log['request_time'])
    log['upstream_response_time'] = float(log['upstream_response_time'])

    return dict(
        timestamp=log.get('timestamp',' '),
        data=log,
        type='metric',
        event='nginx_event',
    )

def mongodb(line):
    '''
    >>> import pprint
    >>> input_line1 = '2017-08-17T07:56:33.489+0200 I REPL     [signalProcessingThread] shutting down replication subsystems'
    >>> output_line1 = mongodb(input_line1)
    >>> pprint.pprint(output_line1)
    {'data': {'component': 'REPL',
              'context': '[signalProcessingThread]',
              'message': 'shutting down replication subsystems',
              'severity': 'I',
              'timestamp': '2017-08-17T07:56:33.489+0200'},
     'timestamp': '2017-08-17T07:56:33.489+0200',
     'type': 'log'}

    >>> input_line2 = '2017-08-17T07:56:33.515+0200 W NETWORK  [initandlisten] No primary detected for set confsvr_repl1'
    >>> output_line2 = mongodb(input_line2)
    >>> pprint.pprint(output_line2)
    {'data': {'component': 'NETWORK',
              'context': '[initandlisten]',
              'message': 'No primary detected for set confsvr_repl1',
              'severity': 'W',
              'timestamp': '2017-08-17T07:56:33.515+0200'},
     'timestamp': '2017-08-17T07:56:33.515+0200',
     'type': 'log'}
    '''

    keys = ['timestamp', 'severity', 'component', 'context', 'message']
    values = re.split(r'\s+', line, maxsplit=4)
    mongodb_log = dict(zip(keys,values))

    return dict(
        timestamp=values[0],
        data=mongodb_log,
        type='log',
    )

def basescript(line):
    '''
    >>> import pprint
    >>> input_line = '{"level": "warning", "timestamp": "2018-02-07T06:37:00.297610Z", "event": "exited via keyboard interrupt", "type": "log", "id": "20180207T063700_4d03fe800bd111e89ecb96000007bc65", "_": {"ln": 58, "file": "/usr/local/lib/python2.7/dist-packages/basescript/basescript.py", "name": "basescript.basescript", "fn": "start"}}'
    >>> output_line1 = basescript(input_line)
    >>> pprint.pprint(output_line1)
    {'data': {'_': {'file': '/usr/local/lib/python2.7/dist-packages/basescript/basescript.py',
                    'fn': 'start',
                    'ln': 58,
                    'name': 'basescript.basescript'},
              'event': 'exited via keyboard interrupt',
              'id': '20180207T063700_4d03fe800bd111e89ecb96000007bc65',
              'level': 'warning',
              'timestamp': '2018-02-07T06:37:00.297610Z',
              'type': 'log'},
     'event': 'exited via keyboard interrupt',
     'id': '20180207T063700_4d03fe800bd111e89ecb96000007bc65',
     'level': 'warning',
     'timestamp': '2018-02-07T06:37:00.297610Z',
     'type': 'log'}
    '''
    log = json.loads(line)

    return dict(
        timestamp=log['timestamp'],
        data=log,
        id=log['id'],
        type=log['type'],
        level=log['level'],
        event=log['event']
    )

def elasticsearch(line):
    '''
    >>> import pprint
    >>> input_line = '[2017-08-30T06:27:19,158] [WARN ][o.e.m.j.JvmGcMonitorService] [Glsuj_2] [gc][296816] overhead, spent [1.2s] collecting in the last [1.3s]'
    >>> output_line = elasticsearch(input_line)
    >>> pprint.pprint(output_line)
    {'data': {'garbage_collector': 'gc',
              'gc_count': 296816.0,
              'level': 'WARN',
              'message': 'o.e.m.j.JvmGcMonitorService',
              'plugin': 'Glsuj_2',
              'query_time_ms': 1200.0,
              'resp_time_ms': 1300.0,
              'timestamp': '2017-08-30T06:27:19,158'},
     'event': 'o.e.m.j.JvmGcMonitorService',
     'level': 'WARN ',
     'timestamp': '2017-08-30T06:27:19,158',
     'type': 'metric'}

    Case 2:
    [2017-09-13T23:15:00,415][WARN ][o.e.i.e.Engine           ] [Glsuj_2] [filebeat-2017.09.09][3] failed engine [index]
    java.nio.file.FileSystemException: /home/user/elasticsearch/data/nodes/0/indices/jsVSO6f3Rl-wwBpQyNRCbQ/3/index/_0.fdx: Too many open files
            at sun.nio.fs.UnixException.translateToIOException(UnixException.java:91) ~[?:?]
    '''

    # TODO we need to handle case2 logs
    elasticsearch_log = line
    actuallog = re.findall(r'(\[\d+\-+\d+\d+\-+\d+\w+\d+:\d+:\d+,+\d\d\d+\].*)', elasticsearch_log)
    if len(actuallog) == 1:
        keys = ['timestamp','level','message','plugin','garbage_collector','gc_count','query_time_ms', 'resp_time_ms']
        values = re.findall(r'\[(.*?)\]', actuallog[0])
        for index, i in enumerate(values):
            if not isinstance(i, str):
                continue
            if len(re.findall(r'.*ms$', i)) > 0 and 'ms' in re.findall(r'.*ms$', i)[0]:
                num = re.split('ms', i)[0]
                values[index]  = float(num)
                continue
            if len(re.findall(r'.*s$', i)) > 0 and 's' in re.findall(r'.*s$', i)[0]:
                num = re.split('s', i)[0]
                values[index] = float(num) * 1000
                continue

        data = dict(zip(keys,values))
        if 'level' in data and data['level'][-1] == ' ':
            data['level'] = data['level'][:-1]
        if 'gc_count' in data:
            data['gc_count'] = float(data['gc_count'])
        event = data['message']
        level=values[1]
        timestamp=values[0]

        return dict(
                timestamp=timestamp,
                level=level,
                type='metric',
                data=data,
                event=event
        )

    else:
        return dict(
                timestamp=datetime.datetime.isoformat(datetime.datetime.now()),
                data={'raw': line}
        )

LOG_BEGIN_PATTERN = [re.compile(r'^\s+\['), re.compile(r'^\[')]

def elasticsearch_ispartial_log(line):
    '''
    >>> line1 = '  [2018-04-03T00:22:38,048][DEBUG][o.e.c.u.c.QueueResizingEsThreadPoolExecutor] [search17/search]: there were [2000] tasks in [809ms], avg task time [28.4micros], EWMA task execution [790nanos], [35165.36 tasks/s], optimal queue is [35165], current capacity [1000]'
    >>> line2 = '  org.elasticsearch.ResourceAlreadyExistsException: index [media_corpus_refresh/6_3sRAMsRr2r63J6gbOjQw] already exists'
    >>> line3 = '   at org.elasticsearch.cluster.metadata.MetaDataCreateIndexService.validateIndexName(MetaDataCreateIndexService.java:151) ~[elasticsearch-6.2.0.jar:6.2.0]'
    >>> elasticsearch_ispartial_log(line1)
    False
    >>> elasticsearch_ispartial_log(line2)
    True
    >>> elasticsearch_ispartial_log(line3)
    True
    '''
    match_result = []

    for p in LOG_BEGIN_PATTERN:
        if re.match(p, line) != None:
            return False
    return True

elasticsearch.ispartial = elasticsearch_ispartial_log
