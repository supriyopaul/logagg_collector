# logagg-collector
**Log collector for logagg**

Collects all the logs from the server and parses it for making a common schema for all the logs and sends to NSQ.


----------
## Prerequisites
* We expect users to follow [Best practices](https://github.com/deep-compute/logagg/issues/85) for logging their application.
* Most importantly, do structured logging. Since, parsing/formatting logs is way easier that way.
* Set-up [logagg-fs](https://github.com/supriyopaul/logagg_fs) beforehand.

----------
## Components/Architecture/Terminology

* `files` : Log files which are being tracked by logagg
* `node` : The server(s) where the log `files` reside
* `formatters` : The parser function that the `collector` uses to format the log lines to put it the common format.
* `nsq` : The central location where logs are sent by `collector`(s) after formatting as messages.

----------


## Features

* Guaranteed delivery of each log line from files to `nsq`
* Reduced latency between a log being generated an being present in the `nsq`
* Options to add custom `formatters`
* File poll if log file not yet generated
* Works on rotational log files
* Custom `formatters` to support parsing of any log file.
* Output format of processed log lines (dictionary)
    * `id` (str) - A unique id per log with time ordering. Useful to avoid storing duplicates.
    * `timestamp` (str) - ISO Format time. eg:
    * `data` (dict) - Parsed log data
    * `raw` (str) - Raw log line read from the log file
    * `host` (str) - Hostname of the node where this log was generated
    * `formatter` (str) - name of the formatter that processed the raw log line
    * `file` (str) - Full path of the file in the host where this log line came from
    * `type` (str) - One of "log", "metric" (Is there one more type?)
    * `level` (str) - Log level of the log line.
    * `event` (str) - LOG event
    * `error` (bool) - True if collection handler failed during processing
    * `error_tb` (str) - Error traceback


---

## Installation
> Prerequisites: Python3.5
### Setup

####  [Install](http://nsq.io/deployment/installing.html) the `nsq` package, at where we need to bring up the `nsq` server.
- Run the following commands to install `nsq`:
    ```BASH
    $ sudo apt-get install libsnappy-dev
    $ wget https://s3.amazonaws.com/bitly-downloads/nsq/nsq-1.0.0-compat.linux-amd64.go1.8.tar.gz
    $ tar zxvf nsq-1.0.0-compat.linux-amd64.go1.8.tar.gz
    $ sudo cp nsq-1.0.0-compat.linux-amd64.go1.8/bin/* /usr/local/bin
    ```

#### [Install](https://docs.docker.com/install/linux/docker-ce/ubuntu/#extra-steps-for-aufs) the Docker package, at both `collector` nodes.
- Run the following commands to install :
    ```
    $ sudo apt-get update
    $ sudo apt-get install \
        apt-transport-https \
        ca-certificates \
        curl \
        software-properties-common
    $ curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
    $ sudo add-apt-repository \
       "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
       $(lsb_release -cs) \
       stable"
    $ sudo apt-get update
    $ sudo apt-get install docker-ce
    ```
- Check Docker version >= 17.12.1
    ```
    $ sudo docker -v
    Docker version 18.03.1-ce, build 9ee9f40
    ```
- Run serverstats to store server metrics in /var/log/serverstats
    ```
    $ docker plugin install deepcompute/docker-file-log-driver:1.0 --alias file-log-driver
    $ docker run --hostname $HOSTNAME --name serverstats --restart unless-stopped --label formatter=logagg_collector.formatters.basescript --log-driver file-log-driver --log-opt labels=formatter --log-opt fpath=/serverstats/serverstats.log --detach deepcompute/serverstats:latest
    ```

### Install the `logagg-collector` package, at where we collect the logs and at where we forward the logs:
- Run the following command to **pip** install `logagg`: 
    ```
    $ pip3 install https://github.com/deep-compute/pygtail/tarball/master/#egg=pygtail-0.6.1
    $ pip3 install git+https://github.com/supriyopaul/logagg_utils.git
    $ pip3 install git+https://github.com/supriyopaul/logagg_collector.git
    ```
---

## Basic Usage

### Bring up the `nsq` instances at the required server with following commands:
- **NOTE:** Run each command in a seperate Terminal window
- nsqlookupd
    ```
    $ nsqlookupd
    ```
- nsqd -lookupd-tcp-address **<ip-addr or DNS>**:4160
    ```
    $ nsqd -lookupd-tcp-address localhost:4160
    ```
- nsqadmin -lookupd-http-address **<ip-addr or DNS>**:4161
    ```
    $ nsqadmin -lookupd-http-address localhost:4161
    ```
### Run logagg-collector service
```bash=
$ logagg-collector runserver --no-master --data-dir /tmp/ --logaggfs-dir /logcache/
```
    
#### APIs
* To see files collector is supposed to collect
```bash=
curl -i 'http://localhost:1099/collector/v1/get_files'
```
* To set NSQ details
```bash=
curl -i 'http://localhost:1099/collector/v1/set_nsq?nsqd_http_address=<nsq-server-ip-or-DNS>:4151&topic_name=logagg'
```
* To start collecting
```bash=
curl -i 'http://localhost:1099/collector/v1/collect
```

**Note**: Replace **<nsq-server-ip-or-DNS>** with the ip of `nsq` server eg.: **192.168.0.211**
**Note**: **--volume** argument is to mount local directory of log file into `Docker` `container`
**Note**: **--hostname** argument is to use the same hostname and not the docker container hostname
- You can check message traffic at `nsq` by going through the link:
        **http://<nsq-server-ip-or-DNS>:4171/** for **localhost** see [here](http://localhost:4171/)
- You can see the collected logs in realtime using the following command:
    ```bash
    $ nsq_tail --topic=logagg --channel=test --lookupd-http-address=<nsq-server-ip-or-DNS>:4161
    ```

### Types of handlers we support
| Formatter-name | Comments |
| -------- | -------- |
|   logagg_collector.formatters.nginx_access   | See Configuration [here](https://github.com/deep-compute/logagg/issues/61)    |
|logagg_collector.formatters.mongodb||
|logagg_collector.formatters.basescript||
|logagg_collector.formatters.docker_log_file_driver|See example [here](https://github.com/deep-compute/serverstats/issues/6)|


---

## Advanced usage
### How to create and use custom formatters for log files
#### Step 1: make a directory and append it's path to evironment variable $PYTHONPATH
```bash
$ echo $PYTHONPATH

$ mkdir customformatters
$ #Now append the path to $PYTHONPATH
$ export PYTHONPATH=$PYTHONPATH:/home/path/to/customformatters/

$ echo $PYTHONPATH
:/home/path/to/customformatters
```
#### Step 2: Create a another directory and put your formatter file(s) inside it.

```bash
$ cd customformatters/
$ mkdir myformatters
$ cd myformatters/
$ touch formatters.py
$ touch __init__.py
$ echo 'import formatters' >> __init__.py
$ #Now write your formatter functions inside the formatters.py file
```
#### Step 3: Write your formatter functions inside the formatters.py file

**Important:** 
1. Only **python standard modules** can be imported in formatters.py file
2. A formatter function should return a **dict()** `datatype`
3. The 'dict()' should only contain keys which are mentioned in the above [log structure](https://github.com/deep-compute/logagg#features).
4. Sample formatter functions:
    ```python
    import json 
    import re

    sample_log_line = '2018-02-07T06:37:00.297610Z [Some_event] [Info] [Hello_there]'

    def sample_formatter(log_line):
        log = re.sub('[\[+\]]', '',log_line).split(' ')
        timestamp = log[0]
        event = log[1]
        level = log[2]
        data = dict({'message': log[3]})

        return dict(timestamp = timestamp,
                     event = event,
                     level = level,
                     data = data,
                    )
     ```
  To see more examples, look [here](https://github.com/deep-compute/logagg/blob/master/logagg/formatters.py) 

5. Check if the custom handler works in `python interpreter` like for logagg.
    ```python
    >>> import myformatters
    >>> sample_log_line = '2018-02-07T06:37:00.297610Z [Some_event] [Info] [Hello_there]'
    >>> output = myformatters.formatters.sample_formatter(sample_log_line)
    >>> from pprint import pprint
    >>> pprint(output)
    {'data': {'message': 'Hello_there'},
     'event': 'Some_event',
     'level': 'Info',
     'timestamp': '2018-02-07T06:37:00.297610Z'}
    ```
6. Pseudo logagg collect commands:
    ```bash
    $ sudo logagg collect --file file=logfile.log:myformatters.formatters.sample_formatter --nsqtopic logagg --nsqd-http-address localhost:4151
    ```
---

### Debugging
You can store logagg collector/forwarder logs into files using [basescript](https://github.com/deep-compute/basescript) --log-file argument or [docker file log driver](https://github.com/supriyopaul/docker-file-log-driver)
```bash
$ sudo logagg --log-file /var/log/logagg/collector.log collect file=/var/log/serverstats/serverstats.log:formatter=logagg.formatters.basescript --nsqtopic logagg --nsqd-http-address <nsq-server-ip-or-DNS>:4151
```

If there are multiple files being tracked by multiple collectors on multiple nodes, the collector information can be seen in "Heartbeat" topic of NSQ.
Every running collector sends a hearbeat to this topic (default interval = 30 seconds). The heartbeat format is as follows:
* `timestamp` : Timestamp of the recieved heartbeat.
* `heartbeat_number` : The heartbeat number since the collector started running.
* `host` : Hostname of the node on which the collector is running.
* `nsq_topic` : The nsq topic which the collector is using.
* `files_tracked` : list of files that are being tracked by the collector followed by the fomatter.

You can run the following command to see the information:
```bash
$ nsq_tail --topic=Heartbeat --channel=test --lookupd-http-address=<nsq-server-ip-or-DNS>:4161
```
## Build on logagg

You're more than welcome to hack on this:-)

```bash
$ git clone https://github.com/deep-compute/logagg
$ cd logagg
$ sudo python setup.py install
$ docker build -t logagg .
```
-------------------------------------------------------------------

