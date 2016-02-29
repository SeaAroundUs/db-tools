import optparse
import traceback
import multiprocessing
import tkinter as tk
from tkinter import ttk
from tkinter import *
import sqlprocessor as sp
from functools import partial
from db import getDbConnection

root = tk.Tk()
root.title("Aggregation")

db_type = StringVar()
db_server = StringVar()
db_port = IntVar()
db_name = StringVar()
db_username = StringVar()
db_password = StringVar()
db_sqlfile = StringVar()
db_sqlcmd = StringVar()
db_threads = IntVar()

options = {}

def loadOptionsFromWidgets():
    options['dbtype'] = db_type.get()
    options['server'] = db_server.get()
    options['port'] = db_port.get()
    options['dbname'] = db_name.get()
    options['username'] = db_username.get()
    options['password'] = db_password.get()
    options['threads'] = db_threads.get()

def postAggregationOperations():
    loadOptionsFromWidgets()
    dbConn = getDbConnection(optparse.Values(options))

    print("Merging Unknown fishing entity in catch data...")
    dbConn.execute("UPDATE web.v_fact_data SET fishing_entity_id = 213 WHERE fishing_entity_id = 223")

    print("Vacuuming v_fact_data afterward...")
    dbConn.execute("vacuum analyze web.v_fact_data")

    # And now refresh all materialized views as most are dependent on data in the v_fact_data table
    options['sqlfile'] = None
    options['sqlcmd'] = "SELECT 'refresh materialized view web.' || table_name FROM matview_v('web') WHERE table_name NOT LIKE 'TOTALS%'"
    sp.process(optparse.Values(options))
    options['sqlcmd'] = "SELECT 'vacuum analyze web.' || table_name FROM matview_v('web') WHERE table_name NOT LIKE 'TOTALS%'"
    sp.process(optparse.Values(options))

def kickoffSqlProcessor(sqlFileName, isPostOpsRequired=True):
    try:
        loadOptionsFromWidgets()
        options['sqlcmd'] = None
        options['sqlfile'] = sqlFileName
        sp.process(optparse.Values(options))

        if isPostOpsRequired:
            postAggregationOperations()
    except ValueError:
        pass

def aggregateAll():
    try:
        kickoffSqlProcessor('sql/aggregate_eez.sql', False)
        kickoffSqlProcessor('sql/aggregate_high_seas.sql', False)
        kickoffSqlProcessor('sql/aggregate_lme.sql', False)
        kickoffSqlProcessor('sql/aggregate_rfmo.sql', False)
        kickoffSqlProcessor('sql/aggregate_global.sql', False)
        postAggregationOperations()
    except ValueError:
        pass

class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)
        self.mainPane = ttk.Panedwindow(root, orient=VERTICAL)

        ##### First pane: for db connection stuff
        self.dbFrame = ttk.Labelframe(self.mainPane, text='DB Connection', width=100, height=100)
        self.dbFrame.grid(column=0, row=0, sticky=(N, W, E, S))
        self.dbFrame.columnconfigure(0, weight=1)
        self.dbFrame.rowconfigure(0, weight=1)

        self.entry_row = 0
        db_type.set("postgres")
        self.add_data_entry(db_type, "db_type", 10)
        self.add_data_entry(db_server, "db_server", 60)
        db_port.set(5432)
        self.add_data_entry(db_port, "db_port", 5)
        self.add_data_entry(db_name, "db_name", 30)
        self.add_data_entry(db_username, "db_username", 30)
        self.add_data_entry(db_password, "db_password", 30)
        db_threads.set(4)
        self.add_data_entry(db_threads, "db_threads", 3)

        for child in self.dbFrame.winfo_children(): child.grid_configure(padx=5, pady=5)

        ##### Second pane: for command buttons
        self.cmdFrame = ttk.Labelframe(self.mainPane, text='Aggregate', width=100, height=100)
        #self.cmdFrame.grid(column=0, row=0, sticky=(N, W, E, S))
        self.cmdFrame.grid(column=0, row=0, sticky=(W))
        self.cmdFrame.columnconfigure(0, weight=1)
        self.cmdFrame.rowconfigure(0, weight=1)

        tk.Button(self.cmdFrame, text="All Area Types", fg="red", command=aggregateAll).grid(column=0, row=1, sticky=W)
        tk.Button(self.cmdFrame, text="EEZ", fg="blue", command=partial(kickoffSqlProcessor, 'sql/aggregate_eez.sql')).grid(column=1, row=1, sticky=W)
        tk.Button(self.cmdFrame, text="Highseas", fg="blue", command=partial(kickoffSqlProcessor, 'sql/aggregate_high_seas.sql')).grid(column=2, row=1, sticky=W)
        tk.Button(self.cmdFrame, text="LME", fg="blue", command=partial(kickoffSqlProcessor, 'sql/aggregate_lme.sql')).grid(column=3, row=1, sticky=W)
        tk.Button(self.cmdFrame, text="RFMO", fg="blue", command=partial(kickoffSqlProcessor, 'sql/aggregate_rfmo.sql')).grid(column=4, row=1, sticky=W)
        tk.Button(self.cmdFrame, text="Global", fg="blue", command=partial(kickoffSqlProcessor, 'sql/aggregate_global.sql')).grid(column=5, row=1, sticky=W)

        ##### Add all child frames to main and pack it up
        self.mainPane.add(self.dbFrame)
        self.mainPane.add(self.cmdFrame)
        self.mainPane.pack(expand=1, fill='both')

    def add_data_entry(self, entry_var, entry_text, entry_len):
        self.entry_row += 1
        tk.Label(self.dbFrame, text=entry_text).grid(column=1, row=self.entry_row, sticky=W)
        data_entry = tk.Entry(self.dbFrame, width=entry_len, textvariable=entry_var)
        data_entry.grid(column=2, row=self.entry_row, sticky=W)


# ===============================================================================================
# ----- MAIN
def main():
    app = Application(master=root)
    app.mainloop()

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
