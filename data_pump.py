import optparse
import traceback
import multiprocessing
import tkinter as tk
from tkinter import ttk
from tkinter import *
import sqlprocessor as sp
from db import getDbConnection
from db import DBConnectionPane
from aggregate import AggregateCommandPane
from pull_data import PullAllocationDataCommandPane

root = tk.Tk()
root.title("SAU Data Pump")


def process():
    try:
        options = {}
        options['dbtype'] = db_type.get()
        options['server'] = db_server.get()
        options['port'] = db_port.get()
        options['dbname'] = db_name.get()
        options['username'] = db_username.get()
        options['password'] = db_password.get()
        options['sqlfile'] = db_sqlfile.get()
        options['sqlcmd'] = db_sqlcmd.get()
        options['threads'] = db_threads.get()
        sp.process(optparse.Values(options))
    except ValueError:
        pass


class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)

        mainNB = ttk.Notebook(root, width=500, height=450)

        dbPane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        mainDB = DBConnectionPane(dbPane, 'Main DB')
        sourceDB = DBConnectionPane(dbPane, 'Source DB')

        # first tab
        pullDataPane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        PullAllocationDataCommandPane(pullDataPane, mainDB, sourceDB)
        # Also added a filler pane to purely improve look and feel only
        pullDataPane.add(ttk.Panedwindow(pullDataPane, orient=VERTICAL))

        # second tab
        summarizePane = ttk.Panedwindow(mainNB, orient=VERTICAL)

        # third tab
        aggregatePane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        AggregateCommandPane(
            aggregatePane,
            mainDB,
            True,
            ['Aggregrate data for marine layers 1, 2, 3, 4 and 6',
             'Aggregrate data for marine layer 1',
             'Aggregrate data for marine layer 2',
             'Aggregrate data for marine layer 3',
             'Aggregrate data for marine layer 4',
             'Aggregrate data for marine layer 6']
        )
        # Also added a filler pane to purely improve look and feel only
        aggregatePane.add(ttk.Panedwindow(aggregatePane, orient=VERTICAL))

        # fourth tab
        cellCatchPane = ttk.Panedwindow(mainNB, orient=VERTICAL)

        # fifth tab
        cacheDataPane = ttk.Panedwindow(mainNB, orient=VERTICAL)

        mainNB.add(dbPane, text='DB Connection')
        mainNB.add(pullDataPane, text='Pull Data')
        mainNB.add(summarizePane, text='Summarize')
        mainNB.add(aggregatePane, text='Aggregate')
        mainNB.add(cellCatchPane, text='Cell Catch')
        mainNB.add(cacheDataPane, text='Cache Data')

        mainNB.pack(expand=1, fill='both')


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
