import os
import socket

from basescript import BaseScript
from deeputil import AttrDict
import tornado.ioloop
import tornado.web
from kwikapi.tornado import RequestHandler
from kwikapi import API
from logagg_utils import start_daemon_thread

from .collector import LogCollector, CollectorService

class LogaggCollectorCommand(BaseScript):
    DESC = 'Logagg command line tool'

    def parse_master_args(self, master_arg):
        '''
        Parse master arguments
        '''
        # Collector running with or without master
        if not self.args.no_master:
            master = AttrDict()
            try:
                m = self.args.master.split(':')
                # So that order of keys is not a factor
                for a in m:
                    a = a.split('=')
                    if a[0] == 'host': master.host = a[-1]
                    elif a[0] == 'port': master.port = a[-1]
                    elif a[0] == 'cluster_name': master.cluster_name = a[-1]
                    elif a[0] == 'cluster_passwd': master.cluster_passwd = a[-1]
                    else: raise ValueError

            except ValueError:
                raise Exception('Invalid argument arg: {}'.format(self.args.master))
            return master

        else:
            return None

    def collect(self):
        '''
        Start collector service after parsing arguments
        '''
        master = self.parse_master_args(self.args.master)
        # Create collector object
        collector = LogCollector(self.args.host,
                                    self.args.port,
                                    master,
                                    self.args.data_dir,
                                    self.args.logaggfs_dir,
                                    self.log)
        # Request authentication master details 
        register_response = collector.register_to_master()

        # Start server
        if register_response['result']['success']:
            collector_api = CollectorService(collector, self.log)
            api = API()
            api.register(collector_api, 'v1')
            try:
                app = tornado.web.Application([
                    (r'^/collector/.*', RequestHandler, dict(api=api)),
                    ])
                app.listen(self.args.port)
                tornado.ioloop.IOLoop.current().start()
            except:
                self.log.info('exiting')
        else:
            err_msg = register_response['result']['details']
            raise Exception(err_msg)


    def define_subcommands(self, subcommands):
        '''
        Subcommands for the CLI
        '''
        super(LogaggCollectorCommand, self).define_subcommands(subcommands)

        collect_cmd = subcommands.add_parser('runserver',
                help='Collects the logs from different files and sends to nsq')

        collect_cmd.set_defaults(func=self.collect)
        collect_cmd.add_argument(
                '--host', '-i', default=socket.gethostname(),
                help='Hostname of this service for other components to contact to, default: %(default)s')
        collect_cmd.add_argument(
                '--port', '-p', default=1099,
                help='Port to run logagg collector service on, default: %(default)s')
        collect_cmd.add_argument(
                '--master', '-m',
                help= 'Master service details, format: host=<hostname>:port=<port>:cluster_name=<name>:cluster_passwd=<cluster_passwd>')
        collect_cmd.add_argument(
                '--no-master', action='store_true',
                help= 'If collector is to run independently, witout a master service')
        collect_cmd.add_argument(
                '--data-dir', '-d', default=os.getcwd()+'/logagg-data',
                help= 'Data path for logagg, default: %(default)s')
        collect_cmd.add_argument(
                '--logaggfs-dir', '-l', default='/logcache',
                help= 'LogaggFS directory, default: %(default)s')


def main():
    LogaggCollectorCommand().start()

if __name__ == '__main__':
    main()
