import optparse
import traceback
import multiprocessing
from functools import partial

import sqlprocessor as sp
from db import DBConnectionPane
from db import getDbConnection

import tkinter as tk
from tkinter import ttk
from tkinter import *
from tkinter import messagebox


class CacheDataCommandPane(tk.Frame):
    def __init__(self, parent, dbPane):
        tk.Frame.__init__(self, parent)
        self.dbPane = dbPane

        self.processFrame = ttk.Labelframe(parent, text="Cache Data Generation/Refresh", width=400, height=100)
        self.processFrame.grid(column=0, row=0, sticky=(N, W, E, S))
        self.processFrame.columnconfigure(0, weight=1)
        self.processFrame.rowconfigure(0, weight=1)

        self.command_row = 0
        self.add_command(self.processFrame, "Generate/Refresh RFMO csv cache", "RFMO csv cache", self.process, 4)

        parent.add(self.processFrame)

        for child in self.processFrame.winfo_children(): child.grid_configure(padx=5, pady=5)

    def add_command(self, panel, label_text, cmd_text, cmd, cmd_param):
        self.command_row += 1
        tk.Button(panel, text=cmd_text, fg="blue", command=partial(cmd, cmd_param)).grid(column=0, row=self.command_row, sticky=W)
        tk.Label(panel, text=label_text).grid(column=1, row=self.command_row, sticky="W")

    def process(self, entity_layer_id):
        if not self.dbPane.isConnectionTestedSuccessfully():
            messagebox.showinfo("Connection not yet tested",
                                "The DB Connection has not been tested successfully.\n" +\
                                "Once the DB Connection has been tested successfully, you can click the Process button again.")
            return

        dbOpts = self.dbPane.getDbOptions()
        dbSession = getDbConnection(optparse.Values(dbOpts)).getSession()

        dbSession.execute("SELECT * FROM web_cache.maintain_catch_csv_partition(%s)" % entity_layer_id)

        dbOpts['sqlfile'] = "sql/populate_catch_data_in_csv.sql"
        sp.process(optparse.Values(dbOpts))


class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)
        mainPane = ttk.Panedwindow(master, orient=VERTICAL)
        dbPane = DBConnectionPane(mainPane, "DB Connection", include_threads=True, include_sqlfile=False)
        CacheDataCommandPane(mainPane, dbPane)
        mainPane.pack(expand=1, fill='both')

# ===============================================================================================
# ----- MAIN
def main():
    root = tk.Tk()
    root.title("Cache Data Generator")
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
