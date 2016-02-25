import optparse
import traceback
import multiprocessing
import tkinter as tk
from tkinter import *
import sqlprocessor as sp

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


def kickoffSqlProcessor(sqlFileName):
    try:
        options = {}
        options['dbtype'] = db_type.get()
        options['server'] = db_server.get()
        options['port'] = db_port.get()
        options['dbname'] = db_name.get()
        options['username'] = db_username.get()
        options['password'] = db_password.get()
        options['threads'] = db_threads.get()
        options['sqlcmd'] = "select 'vacuum analyze web.v_fact_data'"
        options['sqlfile'] = sqlFileName
        sp.process(optparse.Values(options))
    except ValueError:
        pass

def aggregateEEZ():
    try:
        kickoffSqlProcessor('sql/aggregate_eez.sql')
    except ValueError:
        pass

def aggregateHighseas():
    try:
        kickoffSqlProcessor('sql/aggregate_high_seas.sql')
    except ValueError:
        pass

def aggregateLME():
    try:
        kickoffSqlProcessor('sql/aggregate_lme.sql')
    except ValueError:
        pass

def aggregateRFMO():
    try:
        kickoffSqlProcessor('sql/aggregate_rfmo.sql')
    except ValueError:
        pass

def aggregateGlobal():
    try:
        kickoffSqlProcessor('sql/aggregate_global.sql')
    except ValueError:
        pass


class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)
        self.mainframe = tk.Frame(root)
        self.mainframe.grid(column=0, row=0, sticky=(N, W, E, S))
        self.mainframe.columnconfigure(0, weight=1)
        self.mainframe.rowconfigure(0, weight=1)

        self.entry_row = 0

        db_type.set("postgres")
        self.add_data_entry(db_type, "db_type", 10)
        self.add_data_entry(db_server, "db_server", 80)
        db_port.set(5432)
        self.add_data_entry(db_port, "db_port", 5)
        self.add_data_entry(db_name, "db_name", 30)
        self.add_data_entry(db_username, "db_username", 30)
        self.add_data_entry(db_password, "db_password", 30)
        db_threads.set(4)
        self.add_data_entry(db_threads, "db_threads", 3)

        self.entry_row += 1
        tk.Button(self.mainframe, text="Aggregate EEZ", fg="red", command=aggregateEEZ).grid(column=1, row=self.entry_row, sticky=W)
        tk.Button(self.mainframe, text="Aggregate Highseas", fg="red", command=aggregateHighseas).grid(column=2, row=self.entry_row, sticky=W)
        self.entry_row += 1
        tk.Button(self.mainframe, text="Aggregate LME", fg="red", command=aggregateLME).grid(column=1, row=self.entry_row, sticky=W)
        tk.Button(self.mainframe, text="Aggregate RFMO", fg="red", command=aggregateRFMO).grid(column=2, row=self.entry_row, sticky=W)
        self.entry_row += 1
        tk.Button(self.mainframe, text="Aggregate Global", fg="red", command=aggregateGlobal).grid(column=1, row=self.entry_row, sticky=W)

        for child in self.mainframe.winfo_children(): child.grid_configure(padx=5, pady=5)

    def add_data_entry(self, entry_var, entry_text, entry_len):
        self.entry_row += 1
        tk.Label(self.mainframe, text=entry_text).grid(column=1, row=self.entry_row, sticky=W)
        data_entry = tk.Entry(self.mainframe, width=entry_len, textvariable=entry_var)
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
