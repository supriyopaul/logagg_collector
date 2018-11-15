import sys
import time
import glob
import uuid
import socket
import datetime
from operator import attrgetter
import traceback
from six import string_types as basestring
import six.moves.queue as queue
import re
import os
from os.path import isfile, join
from hashlib import md5
import shutil
import tempfile

import requests
import ujson as json
from diskdict import DiskDict
from deeputil import AttrDict, load_object
from deeputil import keeprunning
from pygtail import Pygtail
from logagg_utils import utils
from logagg_utils import NSQSender

from .formatters import RawLog

def load_formatter_fn(formatter):
    '''
    >>> load_formatter_fn('logagg_collector.formatters.basescript') #doctest: +ELLIPSIS
    <function basescript at 0x...>
    '''
    obj = load_object(formatter)
    if not hasattr(obj, 'ispartial'):
        obj.ispartial = utils.ispartial
    return obj


class LogCollector():
    DESC = 'Collects the log information and sends to NSQTopic'
    NAMESPACE = 'collector'
    REGISTER_URL = 'http://{master_address}/logagg/v1/register_component?namespace={namespace}&cluster_name={cluster_name}&cluster_passwd={cluster_passwd}&host={host}&port={port}'
    GET_CLUSTER_INFO_URL = 'http://{host}:{port}/logagg/v1/get_cluster_info?cluster_name={cluster_name}&cluster_passwd={cluster_passwd}'

    QUEUE_MAX_SIZE = 2000 # Maximum number of messages in in-mem queue
    MAX_NBYTES_TO_SEND = 4.5 * (1024**2) # Number of bytes from in-mem queue minimally required to push
    MIN_NBYTES_TO_SEND = 512 * 1024 # Minimum number of bytes to send to nsq in mpub
    MAX_SECONDS_TO_PUSH = 1 # Wait till this much time elapses before pushing
    LOG_FILE_POLL_INTERVAL = 0.25 # Wait time to pull log file for new lines added
    QUEUE_READ_TIMEOUT = 1 # Wait time when doing blocking read on the in-mem q
    PYGTAIL_ACK_WAIT_TIME = 0.05 # TODO: Document this
    SCAN_FPATTERNS_INTERVAL = 30 # How often to scan filesystem for files matching fpatterns
    HEARTBEAT_RESTART_INTERVAL = 30 # Wait time if heartbeat sending stops
    LOGAGGFS_FPATH_PATTERN = re.compile("[a-fA-F\d]{32}") # MD5 hash pattern
    SERVERSTATS_FPATH = '/var/log/serverstats/serverstats.log' # Path to serverstats log file
    DOCKER_FORMATTER = 'logagg_collector.formatters.docker_file_log_driver' # Formatter name for docker_file_log_driver

    LOG_STRUCTURE = {
        'id': basestring,
        'timestamp': basestring,
        'file' : basestring,
        'host': basestring,
        'formatter' : basestring,
        'raw' : basestring,
        'type' : basestring,
        'level' : basestring,
        'event' : basestring,
        'data' : dict,
        'error' : bool,
        'error_tb' : basestring,
    }


    def __init__(self, host, port, master, data_dir, logaggfs_dir, log=utils.DUMMY):

        self.host = host
        self.port = port
        self.master = master

        # For storing state
        data_path = os.path.abspath(os.path.join(data_dir, 'logagg-data'))
        self.data_path = utils.ensure_dir(data_path)

        self.master = master

        self.log = log

        # For remembering the state of files
        self.state = DiskDict(self.data_path)

        # Initialize logaggfs paths
        self.logaggfs = self._init_logaggfs_paths(logaggfs_dir)

        # Log fpath to thread mapping
        self.log_reader_threads = {}
        # Handle name to formatter fn obj map
        self.formatters = {}
        self.queue = queue.Queue(maxsize=self.QUEUE_MAX_SIZE)

        # Add initial files i.e. serverstats to state
        if not self.state['fpaths']:
            self.log.info('init_fpaths')
            self._init_fpaths()

        # Create nsq_sender
        self.nsq_sender_logs, self.nsq_sender_heartbeat = self._init_nsq_sender()

        self._ensure_trackfiles_sync()


    def register_to_master(self):
        '''
        Request authentication with master details
        
        Sample url: http://localhost:1088/logagg/v1/register_component?namespace=master
                    &cluster_name=logagg&passwd=ad9379b4&host=localhost&port=1088
        '''
        #TODO: test case
        master = self.master
        url = self.REGISTER_URL.format(master_address=master.host+':'+master.port,
                namespace=self.NAMESPACE,
                cluster_name=master.cluster_name,
                cluster_passwd=master.cluster_passwd,
                host=self.host,
                port=self.port)

        try:
            register = requests.get(url)
            register_result = json.loads(register.content.decode('utf-8'))
            return register_result
        except requests.exceptions.ConnectionError:
            err_msg = 'Could not reach master, url: {}'.format(url)
            raise Exception(err_msg)

    def _init_fpaths(self):
        '''
        Files to be collected by default

        >>> import tempfile
        >>> temp_dir = tempfile.TemporaryDirectory()
        >>> temp_dir_path = temp_dir.name
        >>> trackfile = open(temp_dir_path+'/trackfiles.txt', 'w+'); trackfile.close()
        >>> lc = collector = LogCollector('localhost', '1088', None, temp_dir_path, temp_dir_path)

        >>> lc._init_fpaths()
        [{'fpath': '/var/log/serverstats/serverstats.log', 'formatter': 'logagg_collector.formatters.docker_file_log_driver'}]

        >>> temp_dir.cleanup()
        '''
        self.state['fpaths'] = [{'fpath':self.SERVERSTATS_FPATH,
            'formatter':self.DOCKER_FORMATTER}]
        self.state.flush()
        return self.state['fpaths']


    def _init_nsq_sender(self):
        '''
        Initialize nsq_sender on startup
        >>> import tempfile
        >>> temp_dir = tempfile.TemporaryDirectory()
        >>> temp_dir_path = temp_dir.name
        >>> trackfile = open(temp_dir_path+'/trackfiles.txt', 'w+'); trackfile.close()
        >>> lc = collector = LogCollector('localhost', '1088', None, temp_dir_path, temp_dir_path)

        >>> lc._init_nsq_sender() # doctest: +ELLIPSIS
        (<deeputil.misc.Dummy object at ...>, <deeputil.misc.Dummy object at ...>)

        >>> temp_dir.cleanup()
        '''
        #TODO: test cases for master mode

        # Check if running on master mode or not
        if self.master is None:
            self.log.warn('nsq_not_set', msg='will send formatted logs to stdout')
            return utils.DUMMY, utils.DUMMY

        # Prepare to request NSQ details from master
        else:
            url = self.GET_CLUSTER_INFO_URL.format(host=self.master.host,
                                                    port=self.master.port,
                                                    cluster_name=self.master.cluster_name,
                                                    cluster_passwd=self.master.cluster_passwd)
            try:
                get_cluster_info = requests.get(url)
                get_cluster_info_result = json.loads(get_cluster_info.content.decode('utf-8'))

            except requests.exceptions.ConnectionError:
                err_msg = 'Could not reach master, url: {}'.format(url)
                raise Exception(err_msg)

            if get_cluster_info_result['result'].get('success'):
                nsqd_http_address = get_cluster_info_result['result']['cluster_info']['nsqd_http_address']
                heartbeat_topic = get_cluster_info_result['result']['cluster_info']['heartbeat_topic']
                logs_topic = get_cluster_info_result['result']['cluster_info']['logs_topic']
                nsq_depth_limit = get_cluster_info_result['result']['cluster_info']['nsq_depth_limit']
                
                # Create NSQSender object for sending logs and heartbeats
                nsq_sender_heartbeat = NSQSender(nsqd_http_address,
                                                    heartbeat_topic,
                                                    self.log)
                nsq_sender_logs = NSQSender(nsqd_http_address,
                                                logs_topic,
                                                self.log)
                return nsq_sender_logs, nsq_sender_heartbeat

            else:
                err_msg = get_cluster_info_result['result'].get('details')
                raise Exception(err_msg)


    def _init_logaggfs_paths(self, logaggfs_dir):
        '''
        Logaggfs directories and file initialization

        >>> test_dir = utils.ensure_dir('/tmp/xyz')
        >>> trackfile = open(test_dir+'/trackfiles.txt', 'w+'); trackfile.close()
        >>> lc = collector = LogCollector('localhost', '1088', None, test_dir, test_dir)

        >>> lc._init_logaggfs_paths(test_dir)
        AttrDict({'logcache': '/tmp/xyz', 'logs_dir': '/tmp/xyz/logs', 'trackfiles': '/tmp/xyz/trackfiles.txt'})

        >>> import shutil; shutil.rmtree(test_dir)
        '''
        logaggfs = AttrDict()
        logaggfs.logcache = logaggfs_dir
        logaggfs.logs_dir = os.path.abspath(os.path.join(logaggfs.logcache, 'logs'))
        logaggfs.trackfiles = os.path.abspath(os.path.join(logaggfs.logcache, 'trackfiles.txt'))
        return logaggfs


    def _ensure_trackfiles_sync(self):
        '''
        Make sure fpaths in logagg state-file are present in logaggfs trackfiles on start up

        >>> import tempfile
        >>> temp_dir = tempfile.TemporaryDirectory()
        >>> temp_dir_path = temp_dir.name
        >>> trackfile = open(temp_dir_path+'/trackfiles.txt', 'w+'); trackfile.close()
        >>> lc = collector = LogCollector('localhost', '1088', None, temp_dir_path, temp_dir_path)

        >>> with open(lc.logaggfs.trackfiles) as f: f.read()
        '/var/log/serverstats/serverstats.log\\n'
        >>> lc.state['fpaths'] = [{'fpath': '/var/log/some.log'}]; lc.state.flush()
        >>> lc._ensure_trackfiles_sync()
        >>> with open(lc.logaggfs.trackfiles) as f: f.read()
        '/var/log/serverstats/serverstats.log\\n/var/log/some.log\\n'

        >>> temp_dir.cleanup()
        '''
        # If all the files are present in trackfiles
        for f in self.state['fpaths']:
            if not self._fpath_in_trackfiles(f['fpath']):
                self.add_to_logaggfs_trackfile(f['fpath'])

    def _remove_redundancy(self, log):
        '''
        Removes duplicate data from 'data' inside log dictionary and brings it out

        >>> import tempfile
        >>> temp_dir = tempfile.TemporaryDirectory()
        >>> temp_dir_path = temp_dir.name
        >>> trackfile = open(temp_dir_path+'/trackfiles.txt', 'w+'); trackfile.close()
        >>> lc = collector = LogCollector('localhost', '1088', None, temp_dir_path, temp_dir_path)

        >>> log = {'id' : 46846876, 'type' : 'log',
        ...         'data' : {'a' : 1, 'b' : 2, 'type' : 'metric'}}
        >>> from pprint import pprint
        >>> pprint(lc._remove_redundancy(log))
        {'data': {'a': 1, 'b': 2}, 'id': 46846876, 'type': 'metric'}

        >>> temp_dir.cleanup()
        '''
        for key in log:
            if key in log and key in log['data']:
                log[key] = log['data'].pop(key)
        return log


    def _validate_log_format(self, log):
        '''
        Assert if the formatted log is of the same structure as specified

        >>> import tempfile
        >>> temp_dir = tempfile.TemporaryDirectory()
        >>> temp_dir_path = temp_dir.name
        >>> trackfile = open(temp_dir_path+'/trackfiles.txt', 'w+'); trackfile.close()
        >>> lc = collector = LogCollector('localhost', '1088', None, temp_dir_path, temp_dir_path)

        >>> incomplete_log = {'data' : {'x' : 1, 'y' : 2},
        ...                     'raw' : 'Not all keys present'}
        >>> lc._validate_log_format(incomplete_log)
        'failed'

        >>> redundant_log = {'one_invalid_key' : 'Extra information',
        ...  'data': {'x' : 1, 'y' : 2},
        ...  'error': False,
        ...  'error_tb': '',
        ...  'event': 'event',
        ...  'file': '/path/to/file.log',
        ...  'formatter': 'logagg.formatters.mongodb',
        ...  'host': 'deepcompute-ThinkPad-E470',
        ...  'id': '0112358',
        ...  'level': 'debug',
        ...  'raw': 'some log line here',
        ...  'timestamp': '2018-04-07T14:06:17.404818',
        ...  'type': 'log'}
        >>> lc._validate_log_format(redundant_log)
        'failed'

        >>> correct_log = {'data': {'x' : 1, 'y' : 2},
        ...  'error': False,
        ...  'error_tb': '',
        ...  'event': 'event',
        ...  'file': '/path/to/file.log',
        ...  'formatter': 'logagg.formatters.mongodb',
        ...  'host': 'deepcompute-ThinkPad-E470',
        ...  'id': '0112358',
        ...  'level': 'debug',
        ...  'raw': 'some log line here',
        ...  'timestamp': '2018-04-07T14:06:17.404818',
        ...  'type': 'log'}
        >>> lc._validate_log_format(correct_log)
        'passed'

        >>> temp_dir.cleanup()
        '''

        keys_in_log = set(log)
        keys_in_log_structure = set(self.LOG_STRUCTURE)

        # Check keys
        try:
            assert (keys_in_log == keys_in_log_structure)
        except AssertionError as e:
            self.log.warning('formatted_log_structure_rejected' ,
                                key_not_found = list(keys_in_log_structure-keys_in_log),
                                extra_keys_found = list(keys_in_log-keys_in_log_structure),
                                num_logs=1,
                                type='metric')
            return 'failed'

        # Check datatype of values
        for key in log:
            try:
                assert isinstance(log[key], self.LOG_STRUCTURE[key])
            except AssertionError as e:
                self.log.warning('formatted_log_structure_rejected' ,
                                    key_datatype_not_matched = key,
                                    datatype_expected = type(self.LOG_STRUCTURE[key]),
                                    datatype_got = type(log[key]),
                                    num_logs=1,
                                    type='metric')
                return 'failed'

        return 'passed'


    def _full_from_frags(self, frags):
        '''
        Join partial lines to full lines
        '''
        full_line = '\n'.join([l for l, _ in frags])
        line_info = frags[-1][-1]
        return full_line, line_info


    def _iter_logs(self, freader, fmtfn):
        '''
        Iterate on log lines and identify full lines from full ones
        >>> import tempfile
        >>> temp_dir = tempfile.TemporaryDirectory()
        >>> temp_dir_path = temp_dir.name
        >>> trackfile = open(temp_dir_path+'/trackfiles.txt', 'w+'); trackfile.close()
        >>> lc = collector = LogCollector('localhost', '1088', None, temp_dir_path, temp_dir_path)

        >>> def fmtfn(line):
        ...     return line
        >>> def ispartial(line):
        ...     if line.startswith('--->'): return True
        ...     else: return False
        >>> fmtfn.ispartial = ispartial
        >>> log_dir = tempfile.TemporaryDirectory()

        >>> log_dir_path = log_dir.name
        >>> log_file_path = log_dir_path + '/log_file.log'
        >>> loglines = 'Traceback (most recent call last):\\n--->File "<stdin>", line 1, in <module>\\n--->NameError: name "spam" is not defined'
        >>> with open(log_file_path, 'w+') as logfile: w = logfile.write(loglines)
        >>> sample_freader = Pygtail(log_file_path)

        >>> for log in lc._iter_logs(sample_freader, fmtfn): print(log[0])
	Traceback (most recent call last):
        --->File "<stdin>", line 1, in <module>
        --->NameError: name "spam" is not define

        >>> temp_dir.cleanup()
        >>> log_dir.cleanup()
        '''
        # FIXME: does not handle partial lines at the start of a file properly

        frags = []

        for line_info in freader:
            # Remove new line char at the end
            line = line_info['line'][:-1]
            if not fmtfn.ispartial(line) and frags:
                yield self._full_from_frags(frags)
                frags = []

            frags.append((line, line_info))

        if frags:
            yield self._full_from_frags(frags)


    def _assign_default_log_values(self, fpath, line, formatter):
        '''
        Fills up default data into one log record
        >>> import tempfile
        >>> temp_dir = tempfile.TemporaryDirectory()
        >>> temp_dir_path = temp_dir.name
        >>> trackfile = open(temp_dir_path+'/trackfiles.txt', 'w+'); trackfile.close()
        >>> lc = collector = LogCollector('localhost', '1088', None, temp_dir_path, temp_dir_path)
        >>> from pprint import pprint

        >>> formatter = 'logagg.formatters.mongodb'
        >>> fpath = '/var/log/mongodb/mongodb.log'
        >>> line = 'some log line here'

        >>> default_log = lc._assign_default_log_values(fpath, line, formatter)
        >>> pprint(default_log) #doctest: +ELLIPSIS
        {'data': {},
         'error': False,
         'error_tb': '',
         'event': 'event',
         'file': '/var/log/mongodb/mongodb.log',
         'formatter': 'logagg.formatters.mongodb',
         'host': '...',
         'id': None,
         'level': 'debug',
         'raw': 'some log line here',
         'timestamp': '...',
         'type': 'log'}

        >>> temp_dir.cleanup()
        '''
        return dict(
            id=None,
            file=fpath,
            host=self.host,
            formatter=formatter,
            event='event',
            data={},
            raw=line,
            timestamp=datetime.datetime.utcnow().isoformat(),
            type='log',
            level='debug',
            error= False,
            error_tb='',
          )


    def _delete_file(self, fpath):
        '''
        Move log file from logaggfs 'logs' directory
        >>> import tempfile
        >>> temp_dir = tempfile.TemporaryDirectory()
        >>> temp_dir_path = temp_dir.name
        >>> trackfile = open(temp_dir_path+'/trackfiles.txt', 'w+'); trackfile.close()
        >>> lc = collector = LogCollector('localhost', '1088', None, temp_dir_path, temp_dir_path)

        >>> log_file_dir = tempfile.TemporaryDirectory()
        >>> log_file_path = log_file_dir.name + '/log_file.log'
        >>> offset_file_path = log_file_path + '.offset'
        >>> log_file = open(log_file_path, 'w+'); trackfile.close()
        >>> offset_file = open(offset_file_path, 'w+'); offset_file.close() 
        >>> import os
        >>> os.path.isfile(log_file_path)
        True
        >>> os.path.isfile(offset_file_path)
        True
        >>> lc._delete_file(log_file_path)
        >>> os.path.isfile(log_file_path)
        False
        >>> os.path.isfile(offset_file_path)
        False

        >>> temp_dir.cleanup()
        >>> log_file_dir.cleanup()
        '''
        os.remove(fpath)
        os.remove(fpath+'.offset')


    @keeprunning(LOG_FILE_POLL_INTERVAL, on_error=utils.log_exception)
    def _collect_log_files(self, log_files):
        '''
        Collect from log files in logaggfs 'logs' one by one
        '''

        L = log_files
        # Sorted list of all the files for one pattern
        fpaths = glob.glob(join(self.logaggfs.logs_dir, L['fpattern']))
        fpaths = sorted(fpaths)

        for f in fpaths:
            log_files.update({'fpath': f})
            # If last file in the list keep polling until next file arrives
            self._collect_log_lines(log_files)
            if not f == fpaths[-1]:
                self.log.debug('deleting_file', f=f)
                self._delete_file(f)
        time.sleep(1)


    def _collect_log_lines(self, log_file):
        '''
        Collects logs from logfiles, formats and puts in queue
        '''
        L = log_file
        fpath = L['fpath']
        fmtfn = L['formatter_fn']
        formatter = L['formatter']

        freader = Pygtail(fpath)
        for line, line_info in self._iter_logs(freader, fmtfn):
            log = self._assign_default_log_values(fpath, line, formatter)

            try:
                _log = fmtfn(line)
                # Identify logs inside a log
                # Like process logs inside docker logs
                if isinstance(_log, RawLog):
                    formatter, raw_log = _log['formatter'], _log['raw']
                    log.update(_log)
                    # Give them to actual formatters
                    _log = load_formatter_fn(formatter)(raw_log)

                log.update(_log)
            except (SystemExit, KeyboardInterrupt) as e: raise
            except:
                log['error'] = True
                log['error_tb'] = traceback.format_exc()
                self.log.exception('error_during_handling_log_line', log=log['raw'])

            if log['id'] == None:
                log['id'] = uuid.uuid1().hex

            log = self._remove_redundancy(log)
            if self._validate_log_format(log) == 'failed': continue

            self.queue.put(dict(log=json.dumps(log),
                                freader=freader, line_info=line_info))
            self.log.debug('tally:put_into_self.queue', size=self.queue.qsize())

        while not freader.is_fully_acknowledged():
            t = self.PYGTAIL_ACK_WAIT_TIME
            self.log.debug('waiting_for_pygtail_to_fully_ack', wait_time=t)
            time.sleep(t)


    def _get_msgs_from_queue(self, msgs, timeout):
        msgs_pending = []
        read_from_q = False
        ts = time.time()

        msgs_nbytes = sum(len(m['log']) for m in msgs)

        while 1:
            try:
                msg = self.queue.get(block=True, timeout=self.QUEUE_READ_TIMEOUT)
                read_from_q = True
                self.log.debug("tally:get_from_self.queue")

                _msgs_nbytes = msgs_nbytes + len(msg['log'])
                _msgs_nbytes += 1 # for newline char

                if _msgs_nbytes > self.MAX_NBYTES_TO_SEND:
                    msgs_pending.append(msg)
                    self.log.debug('msg_bytes_read_mem_queue_exceeded')
                    break

                msgs.append(msg)
                msgs_nbytes = _msgs_nbytes

                #FIXME condition never met
                if time.time() - ts >= timeout and msgs:
                    self.log.debug('msg_reading_timeout_from_mem_queue_got_exceeded')
                    break
                    # TODO: What if a single log message itself is bigger than max bytes limit?

            except queue.Empty:
                self.log.debug('queue_empty')
                time.sleep(self.QUEUE_READ_TIMEOUT)
                if not msgs:
                    continue
                else:
                    return msgs_pending, msgs_nbytes, read_from_q

        self.log.debug('got_msgs_from_mem_queue')
        return msgs_pending, msgs_nbytes, read_from_q


    @keeprunning(0, on_error=utils.log_exception) # FIXME: what wait time var here?
    def _send_to_nsq(self, state):
        msgs = []
        should_push = False

        while not should_push:
            cur_ts = time.time()
            self.log.debug('should_push', should_push=should_push)
            time_since_last_push = cur_ts - state.last_push_ts

            msgs_pending, msgs_nbytes, read_from_q = self._get_msgs_from_queue(msgs,
                                                                        self.MAX_SECONDS_TO_PUSH)

            have_enough_msgs = msgs_nbytes >= self.MIN_NBYTES_TO_SEND
            is_max_time_elapsed = time_since_last_push >= self.MAX_SECONDS_TO_PUSH

            should_push = len(msgs) > 0 and (is_max_time_elapsed or have_enough_msgs)
            self.log.debug('deciding_to_push', should_push=should_push,
                            time_since_last_push=time_since_last_push,
                            msgs_nbytes=msgs_nbytes)

        try:
            if isinstance(self.nsq_sender_logs, type(utils.DUMMY)):
                for m in msgs:
                    self.log.info('final_log_format', log=m['log'])
            else:
                self.log.debug('trying_to_push_to_nsq', msgs_length=len(msgs))
                self.nsq_sender_logs.handle_logs(msgs)
                self.log.debug('pushed_to_nsq', msgs_length=len(msgs))
            self._confirm_success(msgs)
            msgs = msgs_pending
            state.last_push_ts = time.time()
        except (SystemExit, KeyboardInterrupt): raise
        finally:
            if read_from_q: self.queue.task_done()


    def _confirm_success(self, msgs):
        ack_fnames = set()

        for msg in reversed(msgs):
            freader = msg['freader']
            fname = freader.filename

            if fname in ack_fnames:
                continue

            ack_fnames.add(fname)
            freader.update_offset_file(msg['line_info'])


    def _compute_md5_fpatterns(self, fpath):
        '''
        For a filepath in logaggfs logs directory compute 'md5*.log' pattern
        '''
        fpath = fpath.encode("utf-8")
        d = self.logaggfs.logs_dir
        dir_contents = [f for f in os.listdir(d) if bool(self.LOGAGGFS_FPATH_PATTERN.match(f)) and isfile(join(d, f))]
        dir_contents = set(dir_contents)
        for c in dir_contents:
            if md5(fpath).hexdigest() == c.split('.')[0]:
                return md5(fpath).hexdigest() + '*' + '.log'


    @keeprunning(SCAN_FPATTERNS_INTERVAL, on_error=utils.log_exception)
    def _scan_fpatterns(self, state):
        '''
        For a list of given fpatterns or a logaggfs directory,
        this starts a thread collecting log lines from file

        >>> os.path.isfile = lambda path: path == '/path/to/log_file.log'
        >>> lc = LogCollector('file=/path/to/log_file.log:formatter=logagg.formatters.basescript', 30)

        >>> print(lc.fpaths)
        file=/path/to/log_file.log:formatter=logagg.formatters.basescript

        >>> print('formatters loaded:', lc.formatters)
        {}
        >>> print('log file reader threads started:', lc.log_reader_threads)
        {}
        >>> state = AttrDict(files_tracked=list())
        >>> print('files bieng tracked:', state.files_tracked)
        []


        >>> if not state.files_tracked:
        >>>     lc._scan_fpatterns(state)
        >>>     print('formatters loaded:', lc.formatters)
        >>>     print('log file reader threads started:', lc.log_reader_threads)
        >>>     print('files bieng tracked:', state.files_tracked)
        '''
        for f in self.state['fpaths']:

            # For supporting file patterns rather than file paths
            for fpath in glob.glob(f['fpath']):

                # Compute 'md5(filename)*.log' fpattern for fpath
                fpattern, formatter = self._compute_md5_fpatterns(fpath), f['formatter']
                # When no md5 pattern filenames are found for the fpath in logaggfs logs directory
                if fpattern == None: continue
                self.log.debug('_scan_fpatterns', fpattern=fpattern, formatter=formatter)
                try:
                    formatter_fn = self.formatters.get(formatter,
                                  load_formatter_fn(formatter))
                    self.log.debug('found_formatter_fn', fn=formatter)
                    self.formatters[formatter] = formatter_fn
                except (SystemExit, KeyboardInterrupt): raise
                except (ImportError, AttributeError):
                    self.log.exception('formatter_fn_not_found', fn=formatter)
                    sys.exit(-1)
                # Start a thread for every filepattern
                log_f = dict(fpattern=fpattern,
                                formatter=formatter, formatter_fn=formatter_fn)
                log_key = (f['fpath'], fpattern, formatter)
                if log_key not in self.log_reader_threads:

                    self.log.info('starting_collect_log_files_thread', log_key=log_key)
                    # There is no existing thread tracking this log file, start one.
                    log_reader_thread = utils.start_daemon_thread(self._collect_log_files, (log_f,))
                    self.log_reader_threads[log_key] = log_reader_thread

        time.sleep(self.SCAN_FPATTERNS_INTERVAL)


    @keeprunning(HEARTBEAT_RESTART_INTERVAL, on_error=utils.log_exception)
    def _send_heartbeat(self, state):

        # Sends continuous heartbeats to a seperate topic in nsq
        if self.log_reader_threads:
            files_tracked = [k for k in  self.log_reader_threads.keys()]
        else:
            files_tracked = ''

        heartbeat_payload = {'namespace': self.NAMESPACE,
                            'host': self.host,
                            'port': self.port,
                            'cluster_name': self.master.cluster_name,
                            'files_tracked': files_tracked,
                            'heartbeat_number': state.heartbeat_number,
                            'timestamp': time.time(),
                            }
        self.nsq_sender_heartbeat.handle_heartbeat(heartbeat_payload)
        state.heartbeat_number += 1
        time.sleep(self.HEARTBEAT_RESTART_INTERVAL)


    def collect(self):

        # start tracking files and put formatted log lines into queue
        state = AttrDict(files_tracked=list())
        utils.start_daemon_thread(self._scan_fpatterns, (state,))

        # start extracting formatted logs from queue and send to nsq
        state = AttrDict(last_push_ts=time.time())
        utils.start_daemon_thread(self._send_to_nsq, (state,))

        # start sending heartbeat to "Hearbeat" topic
        state = AttrDict(heartbeat_number=0)
        self.log.info('init_heartbeat')
        th_heartbeat = utils.start_daemon_thread(self._send_heartbeat, (state,))


    def _fpath_in_trackfiles(self, fpath):
        '''
        Check the presence of fpath is in logaggfs trackfiles.txt
        '''

        # List of files in trackfiles.txt
        with open(self.logaggfs.trackfiles, 'r') as f:
            tf = f.readlines()

        for path in tf:
            if path[:-1] == fpath: return True
        return False

    def add_to_logaggfs_trackfile(self, fpath):
        '''
        Given a fpath add it to logaggfs trackfiles.txt via moving
        '''
        fd, tmpfile = tempfile.mkstemp()

        with open(self.logaggfs.trackfiles, 'r') as f:
            old = f.read()
            new = fpath
            # Write previous files and add the new file
            if not self._fpath_in_trackfiles(new):
                with open(tmpfile, 'w') as t: t.write((old+new+'\n'))
                shutil.move(tmpfile, self.logaggfs.trackfiles)


    def remove_from_logaggfs_trackfile(self, fpath):
        '''
        Given a fpath remove it from logaggfs trackfiles.txt via moving
        '''
        fd, tmpfile = tempfile.mkstemp()

        with open(self.logaggfs.trackfiles, 'r') as f:
            paths = [line[:-1] for line in f.readlines()]

        for p in paths:
            if p == fpath:
                pass
            else:
                with open(tmpfile, 'w+')as t:
                    t.write((p+'\n'))

        shutil.move(tmpfile, self.logaggfs.trackfiles)


class CollectorService():
    def __init__(self, collector, log):
        self.collector = collector
        self.log = log
        self.collector.collect()


    def start(self) -> dict:
        '''
        Sample url: 'http://localhost:6600/collector/v1/start'
        '''
        self.collector.collect()

        return dict(self.collector.state)

    def stop(self) -> dict:
        '''
        Sample url: 'http://localhost:6600/collector/v1/stop'
        '''
        sys.exit(0)


    def add_file(self, fpath:str, formatter:str) -> list:
        '''
        Sample url: 'http://localhost:6600/collector/v1/add_file?fpath="/var/log/serverstats.log"&formatter="logagg_collector.formatters.docker_file_log_driver"'
        '''
        f = {"fpath":fpath, "formatter":formatter}

        # Add new file to logaggfs trackfiles.txt
        self.collector.add_to_logaggfs_trackfile(f['fpath'])

        # Add new file to state
        state = self.collector.state['fpaths']
        # If fpath already present; remove and update
        if f in state: state.remove(f)
        state.append(f)
        self.collector.state['fpaths'] = self.collector.fpaths = state
        self.collector.state.flush()

        return self.collector.state['fpaths']


    def get_files(self) -> list:
        '''
        List of file patterns to be tracked by collector
        '''
        return self.collector.state['fpaths']


    def _get_nsq(self):
        '''
        Returns NSQ details
        '''
        return dict(nsqd_http_address = self.collector.nsq_sender_logs.nsqd_http_address,
                topic_name = self.collector.nsq_sender_logs.topic_name)


    def set_nsq(self, nsqd_http_address:str, topic_name:str) -> dict:
        '''
        Takes details of NSQ to which formatted logs are to be sent
        '''
        nsq_sender_logs = NSQSender(nsqd_http_address, topic_name, self.log)
        self.collector.nsq_sender_logs = nsq_sender_logs

        return self._get_nsq()


    def get_nsq(self) -> dict:
        '''
        Returns NSQ details
        '''
        return self._get_nsq()


    def get_active_log_collectors(self) -> list:

        # return the files on which active threads are running to collect logs
        collectors = self.collector.log_reader_threads
        c = [t[0] for t in collectors if collectors[t].isAlive()]
        return c


    def remove_file(self, fpath:str) -> dict:
        '''
        Sample url: 'http://localhost:6600/collector/v1/add_file?fpath="/var/log/serverstats.log"'
        '''
        #FIXME: stops the program after removing

        # Remove fpath from logaggfs trackfile.txt
        self.collector.remove_from_logaggfs_trackfile(fpath)

        # Remove fpath from state
        s = list()
        for f in self.collector.state['fpaths']:
            if f['fpath'] == fpath: pass
            else: s.append(f)

        self.collector.state['fpaths'] = s
        self.collector.state.flush()
        self.log.info('exiting', fpaths=self.collector.state['fpaths'])
        self.log.info('restart_for_changes_to_take_effect')
        return self.collector.state['fpaths']
        sys.exit(0)

