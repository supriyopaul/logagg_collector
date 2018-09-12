import os

from basescript import BaseScript
from deeputil import AttrDict
import tornado.ioloop
import tornado.web
from kwikapi.tornado import RequestHandler
from kwikapi import API

from .collector import LogCollector, CollectorService
from .exceptions import InvalidArgument

class LogaggCollectorCommand(BaseScript):
    DESC = 'Logagg command line tool'

    def collect(self):
        if not self.args.no_master:
            master = AttrDict()
            try:
                m = self.args.master.split(':')
                # So that order of keys is not a factor
                for a in m:
                    a = a.split('=')
                    if a[0] == 'host': master.host = a[-1]
                    elif a[0] == 'port': master.port = a[-1]
                    elif a[0] == 'key': master.key = a[-1]
                    elif a[0] == 'secret': master.secret = a[-1]
                    else: raise ValueError

            except ValueError:
                raise InvalidArgument(self.args.master)

        else:
            master = None

        # Create collector object
        collector = LogCollector(
            self.args.data_dir,
            self.args.logaggfs_dir,
            master,
            self.log)

        collector_api = CollectorService(collector, self.log)
        api = API()
        api.register(collector_api, 'v1')

        app = tornado.web.Application([
            (r'^/collector/.*', RequestHandler, dict(api=api)),
                ])

        app.listen(self.args.port)
        tornado.ioloop.IOLoop.current().start()

    def define_subcommands(self, subcommands):
        super(LogaggCollectorCommand, self).define_subcommands(subcommands)

        collect_cmd = subcommands.add_parser('runserver',
                help='Collects the logs from different files and sends to nsq')

        collect_cmd.set_defaults(func=self.collect)
        collect_cmd.add_argument(
                '--port', '-p', default=1099,
                help='port to run logagg collector service on, default: %(default)s')
        collect_cmd.add_argument(
                '--master', '-m',
                help= 'Master service details, format: <host=localhost:port=1100:key=xyz:secret=xxxx>')
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
