import optparse
import traceback
import multiprocessing
from functools import partial

import sqlprocessor as sp
from db import DBConnectionPane

import tkinter as tk
from tkinter import ttk
from tkinter import *
from tkinter import messagebox

class SqlProcessorGuiCommandPane(tk.Frame):
    def __init__(self, parent, dbPane, promptForSqlFile=False):
        tk.Frame.__init__(self, parent)
        self.dbPane = dbPane
        self.dbConn = None

        self.promptForSqlFile = promptForSqlFile

        if promptForSqlFile:
            self.sqlFrame = ttk.Labelframe(parent, text="Sql", width=400, height=50)
            self.sqlFrame.grid(column=0, row=0, sticky=(N, W, E, S))
            self.sqlFrame.columnconfigure(0, weight=1)
            self.sqlFrame.rowconfigure(0, weight=1)

            self.db_sqlfile = StringVar()
            self.db_sqlcmd = StringVar()
            self.db_threads = IntVar()

            self.entry_row = 0
            self.add_data_entry(self.sqlFrame, self.db_sqlfile, "db_sqlfile", 80)
            self.add_data_entry(self.sqlFrame, self.db_sqlcmd, "db_sqlcmd", 80)
            self.db_threads.set(4)
            self.add_data_entry(self.sqlFrame, self.db_threads, "db_threads", 3)

            for child in self.sqlFrame.winfo_children(): child.grid_configure(padx=5, pady=5)

            parent.add(self.sqlFrame)
            
        self.processFrame = ttk.Labelframe(parent, text="Process", width=400, height=50)
        self.processFrame.grid(column=0, row=0, sticky=(N, W, E, S))
        self.processFrame.columnconfigure(0, weight=1)
        self.processFrame.rowconfigure(0, weight=1)
         
        pb = tk.Button(self.processFrame, text="  Process  ", fg="red", command=self.process)
        pb.place(relx=0.5, rely=0.5, anchor=CENTER)

        parent.add(self.processFrame)

    def process(self):
        if not self.dbPane.isConnectionTestedSuccessfully():
            messagebox.showinfo("Connection not yet tested",
                                "The DB Connection has not been tested successfully.\n" +\
                                "Once the DB Connection has been tested successfully, you can click the Process button again.")
            return
            
        dbOpts = self.dbPane.getDbOptions()

        if self.promptForSqlFile:
            dbOpts['sqlfile'] = self.db_sqlfile.get()
            dbOpts['threads'] = self.db_threads.get()
            dbOpts['sqlcmd'] = self.db_sqlcmd.get()

        sp.process(optparse.Values(dbOpts))

    def add_data_entry(self, panel, entry_var, entry_text, entry_len, hidden=False):
        self.entry_row += 1
        tk.Label(panel, text=entry_text).grid(column=0, row=self.entry_row, sticky=W)
        if hidden == True:
            data_entry = tk.Entry(panel, width=entry_len, textvariable=entry_var, show="*")
        else:
            data_entry = tk.Entry(panel, width=entry_len, textvariable=entry_var)
        data_entry.grid(column=1, row=self.entry_row, sticky=W)


class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)

        mainPane = ttk.Panedwindow(master, orient=VERTICAL)
        dbPane = DBConnectionPane(parent=mainPane, title="DB Connection", include_threads=True, include_sqlfile=True)
        SqlProcessorGuiCommandPane(mainPane, dbPane)
        mainPane.pack(expand=1, fill='both')

# ===============================================================================================
# ----- MAIN
def main():
    root = tk.Tk()
    root.title("SqlProcessor GUI")
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
