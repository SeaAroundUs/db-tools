import re
import sys
import os
import traceback
import multiprocessing
import optparse
import time
from sqlalchemy import *
from sqlalchemy.pool import NullPool

parser = optparse.OptionParser()
parser.add_option("-b", "--dbtype", help="database type [sqlserver | postgres]", default="postgres")
parser.add_option("-s", "--server", help="database server ip or address", default="localhost")
parser.add_option("-d", "--dbname", help="database name", default="sau")
parser.add_option("-p", "--port", help="port to connect. use 5432 to connect to postgres", type="int", default="5432")
parser.add_option("-u", "--username", help="database user name")
parser.add_option("-w", "--password", help="database user password")
parser.add_option("-t", "--threads", help="number of concurrent threads", type="int", default="4")
parser.add_option("-f", "--sqlfile", help="file containing one or more sql queries to process")
parser.add_option("-c", "--sqlcmd", help="single sql query command to process")


class DBPostgres:
    def __init__(self, opts):
        self.engine = create_engine(
                        'postgresql://{0}{1}@{2}:{3}/{4}'.format(opts.username, opts.password, opts.server, opts.port, opts.dbname),
                        encoding='utf-8',
                        poolclass=NullPool)
        self.conn = self.engine.connect()
        self.conn.connection.set_isolation_level(0)

    def execute(self, sql_cmd):
        return self.conn.execution_options(autocommit=True).execute(text(sql_cmd))

    def close(self):
        self.conn.close()


class DBSqlServer:
    def __init__(self, opts):
        self.engine = create_engine(
                        'mssql+pymssql://{0}{1}@{2}/{3}?charset=utf8'.format(opts.username, opts.password, opts.server, opts.dbname),
                        poolclass=NullPool)
        self.conn = self.engine.connect()

    def execute(self, sql_cmd):
        return self.conn.execute(text(sql_cmd))

    def close(self):
        self.conn.close()


def get_db_engine(opts):
    if opts.dbtype == 'postgres':
        # This database type requires that psycopg2 db driver is already installed
        return DBPostgres(opts)
    elif opts.dbtype == 'sqlserver':
        # This database type requires that pymssql db driver is already installed
        return DBSqlServer(opts)


class Processor(multiprocessing.Process):
    def __init__(self, opts, cmd_queue):
        multiprocessing.Process.__init__(self)
        self.options = opts
        self.cmd_queue = cmd_queue

    def run(self):
        dbconn = get_db_engine(self.options)
        proc_name = self.name

        while True:
            next_cmd = self.cmd_queue.get()
            if next_cmd is None:
                # Poison pill means we should exit
                print('%s: Exiting' % proc_name)
                break
            print('%s: %s' % (proc_name, next_cmd))
            dbconn.execute(next_cmd)
        return

        dbconn.close()

def wait_till_cmd_queue_empty():
    while true:
        if cmd_queue.qsize() == 0:
            break
        time.sleep(2)

def process(opts):
    if not opts.dbname:
        print("dbname is a required input parameter")
        parser.print_help()
        exit(1)

    if not opts.username:
        print("username is a required input parameter")
        parser.print_help()
        exit(1)

    if not opts.password:
        opts.password = ''
    else:
        opts.password = ':' + opts.password

    #Start timing
    start = time.clock()

    # Establish communication queues
    global cmd_queue
    cmd_queue = multiprocessing.Queue()

    # Start SQL processors
    print('Creating %d SQL processors' % opts.threads)
    processors = [Processor(opts, cmd_queue)
                  for i in range(opts.threads)]

    for p in processors:
        p.start()

    # Create a DBAPI connection and fetch SQL commands from db server
    db_connection = get_db_engine(opts)

    # Enqueue SQL commands
    if opts.sqlfile:
        with open(opts.sqlfile) as fileObj:
            sql_cmd_buffer = ''
            for line in fileObj:
                for ch in line:
                    if ch == ';':
                        sql_cmds = db_connection.execute(sql_cmd_buffer + ch)
                        wait_till_cmd_queue_empty()
                        for cmd in sql_cmds:
                            cmd_queue.put(cmd[0])
                        sql_cmd_buffer = ''
                    else:
                        sql_cmd_buffer += ch

    wait_till_cmd_queue_empty()

    if not opts.sqlcmd:
        db_connection.close()
    else:
        sql_cmds = db_connection.execute(opts.sqlcmd)
        for cmd in sql_cmds:
            cmd_queue.put(cmd[0])
        db_connection.close()


    # Add a poison pill for each consumer
    for i in range(opts.threads):
        cmd_queue.put(None)

    for p in processors:
        p.join()

    # Stop timing and report duration
    end = time.clock()
    duration = end - start
    hours, remainder = divmod(duration, 3600)
    minutes, seconds = divmod(remainder, 60)
    print('Completed in %d:%d:%f' % (hours, minutes, seconds))

def caller():
    options = {}
    options['dbtype'] = 'postgres'
    options['username'] = 'sau'
    options['sqlfile'] = 'vacuum_analyze_web_schema.sql'
    options['dbname'] = 'sau'
    options['server'] = 'pb-p1.corp.vnw.com'
    options['sqlcmd'] = None
    options['threads'] = 4
    options['password'] = None
    options['port'] = 5432
    process(optparse.Values(options))


# ===============================================================================================
# ----- MAIN
def main():
    (options, args) = parser.parse_args()
    process(options)

global options

if __name__ == "__main__":
    try:
        multiprocessing.freeze_support()
        main()
    except SystemExit as x:
        sys.exit(x)
    except Exception:
        strace = traceback.extract_tb(sys.exc_info()[2])[-1:]
        lno = strace[0][1]
        print('Unexpected Exception on line: {0}'.format(lno))
        print(sys.exc_info())
        sys.exit(1)
