from sqlalchemy import *
from sqlalchemy.pool import NullPool

# This module expects an option object with the following fields
#   dbtype   => database type either, currently, either sqlserver or postgres
#   server   => database server ip or address
#   dbname   => database name
#   port     => port to connect. use 5432 to connect to postgres
#   username => database user name
#   password => database user password


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

def getDbConnection(opts):
    if not opts.password:
        opts.password = ''
    elif opts.password != '':
        opts.password = ':' + opts.password

    if not opts.dbtype or opts.dbtype == 'postgres':
        # This database type requires that psycopg2 db driver is already installed
        return DBPostgres(opts)
    elif opts.dbtype == 'sqlserver':
        # This database type requires that pymssql db driver is already installed
        return DBSqlServer(opts)

