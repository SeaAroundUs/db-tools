import optparse
from sqlalchemy import *
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
from tkinter import *

# This module expects an option object with the following fields
#   dbtype   => database type either, currently, either sqlserver or postgres
#   server   => database server ip or address
#   dbname   => database name
#   port     => port to connect. use 5432 to connect to postgres
#   username => database user name
#   password => database user password

Base = declarative_base()

class DBPostgres:
    def __init__(self, opts):
        self.engine = create_engine(
                        'postgresql://{0}{1}@{2}:{3}/{4}'.format(opts.username, opts.password, opts.server, opts.port, opts.dbname),
                        encoding='utf-8',
                        poolclass=NullPool,
                        isolation_level="AUTOCOMMIT"
        )
        self.conn = self.engine.connect()
        self.conn.connection.set_isolation_level(0)
        self.Session = sessionmaker(bind=self.engine, autocommit=True)

    def getSession(self):
        return self.Session()

    def execute(self, sql_cmd):
        return self.conn.execution_options(autocommit=True).execute(text(sql_cmd))

    def close(self):
        self.conn.close()


class DBSqlServer:
    def __init__(self, opts):
        self.engine = create_engine(
                        'mssql+pymssql://{0}{1}@{2}/{3}?charset=utf8'.format(opts.username, opts.password, opts.server, opts.dbname),
                        poolclass=NullPool
        )
        self.conn = self.engine.connect()
        self.conn.autocommit = True
        self.Session = sessionmaker(bind=self.engine)

    def getSession(self):
        return self.Session()

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


class DBConnectionPane(tk.Frame):
    def __init__(self, parent, title, include_threads=False, include_sqlfile=False):
        super(DBConnectionPane, self).__init__()
        self.pane = ttk.Labelframe(parent, text=title, width=400, height=200)
        self.pane.grid(column=0, row=0, sticky=(N, W, E, S))
        self.pane.columnconfigure(0, weight=1)
        self.pane.rowconfigure(0, weight=1)

        self.db_type = StringVar()
        self.db_server = StringVar()
        self.db_port = IntVar()
        self.db_name = StringVar()
        self.db_username = StringVar()
        self.db_password = StringVar()
        self.db_sqlfile = StringVar()
        self.db_sqlcmd = StringVar()
        self.db_threads = IntVar()

        self.entry_row = 0
        self.db_type.set("postgres")
        self.add_data_entry(self.pane, self.db_type, "db_type", 10)
        self.add_data_entry(self.pane, self.db_server, "db_server", 55)
        self.db_port.set(5432)
        self.add_data_entry(self.pane, self.db_port, "db_port", 5)
        self.add_data_entry(self.pane, self.db_name, "db_name", 30)
        self.add_data_entry(self.pane, self.db_username, "db_username", 30)
        self.add_data_entry(self.pane, self.db_password, "db_password", 30)

        if include_sqlfile:
            self.add_data_entry(self.pane, self.db_sqlfile, "db_sqlfile", 80)
            self.add_data_entry(self.pane, self.db_sqlcmd, "db_sqlcmd", 80)

        if include_threads:
            self.db_threads.set(4)
            self.add_data_entry(self.pane, self.db_threads, "db_threads", 3)
        else:
            self.db_threads.set(0)

        self.entry_row += 1
        tk.Button(self.pane, text=" Test connection ", fg="red", command=self.testConnection).grid(column=0, row=self.entry_row, sticky=W)

        for child in self.pane.winfo_children(): child.grid_configure(padx=5, pady=5)

        parent.add(self.pane)

        # Below are to detect if a connection has been tested successfully
        self.connectionTested = False
        for entry in (self.db_type, self.db_server, self.db_port, self.db_name, self.db_username, self.db_password):
            entry.trace_variable('w', self.resetConnectionTested)

    def add_data_entry(self, panel, entry_var, entry_text, entry_len):
        self.entry_row += 1
        tk.Label(panel, text=entry_text).grid(column=0, row=self.entry_row, sticky=W)
        data_entry = tk.Entry(panel, width=entry_len, textvariable=entry_var)
        data_entry.grid(column=1, row=self.entry_row, sticky=W)

    def getDbOptions(self):
        options = dict()
        options['dbtype'] = self.db_type.get()
        options['server'] = self.db_server.get()
        options['port'] = self.db_port.get()
        options['dbname'] = self.db_name.get()
        options['username'] = self.db_username.get()
        options['password'] = self.db_password.get()
        options['threads'] = self.db_threads.get()
        return options

    def isConnectionTestedSuccessfully(self):
        return self.connectionTested

    def resetConnectionTested(self, *args):
        # If db_type is set to sqlserver we should change the default value for the port parameter accordingly
        if self.db_type.get() == "sqlserver":
            self.db_port.set(1433)
        else:
            self.db_port.set(5432)

        self.connectionTested = False

    def testConnection(self):
        conn = getDbConnection(optparse.Values(self.getDbOptions()))
        conn.close()

        self.connectionTested = True

        messagebox.showinfo("Test connection", "Connection successfully made!")