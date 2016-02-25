import optparse
import traceback
import multiprocessing
import tkinter as tk
from tkinter import ttk
from tkinter import *
import sqlprocessor as sp

root = tk.Tk()
root.title("SqlProcessor GUI")

db_type = StringVar()
db_server = StringVar()
db_port = IntVar()
db_name = StringVar()
db_username = StringVar()
db_password = StringVar()
db_sqlfile = StringVar()
db_sqlcmd = StringVar()
db_threads = IntVar()


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
        # self.mainframe = tk.Frame(root)
        # self.mainframe.grid(column=0, row=0, sticky=(N, W, E, S))
        # self.mainframe.columnconfigure(0, weight=1)
        # self.mainframe.rowconfigure(0, weight=1)

        n = ttk.Notebook(width=500, height=300)
        f1 = tk.Frame(n)   # first page, which would get widgets gridded into it
        f2 = tk.Frame(n)   # second page
        f3 = tk.Frame(n)   # second page
        f4 = tk.Frame(n)   # second page
        n.add(f1, text='DB Connection')
        n.add(f2, text='Sync')
        n.add(f3, text='Summarize')
        n.add(f4, text='Aggregate')
        n.pack(expand=1, fill='both')

        # Setting up db connection tab
        #p = ttk.Panedwindow(f1, orient=VERTICAL)
        # first pane, which would get widgets gridded into it:
        mainDBPane = ttk.Labelframe(f1, text='Main DB', width=400, height=200)
        sourceDBPane = ttk.Labelframe(f1, text='Source DB', width=400, height=200)   # second pane
        #f1.add(mainDBPane)
        #f1.add(sourceDBPane)

        self.entry_row = 0
        db_type.set("postgres")
        self.add_data_entry(mainDBPane, db_type, "db_type", 10)
        self.add_data_entry(mainDBPane, db_server, "db_server", 80)
        db_port.set(5432)
        self.add_data_entry(mainDBPane, db_port, "db_port", 5)
        self.add_data_entry(mainDBPane, db_name, "db_name", 30)
        self.add_data_entry(mainDBPane, db_username, "db_username", 30)
        self.add_data_entry(mainDBPane, db_password, "db_password", 30)

        self.entry_row += 1
        tk.Button(mainDBPane, text="Test connection", fg="red", command=process).grid(column=2, row=self.entry_row, sticky=W)

        for child in mainDBPane.winfo_children(): child.grid_configure(padx=5, pady=5)

    def add_data_entry(self, panel, entry_var, entry_text, entry_len):
        self.entry_row += 1
        tk.Label(panel, text=entry_text).grid(column=1, row=self.entry_row, sticky=W)
        data_entry = tk.Entry(panel, width=entry_len, textvariable=entry_var)
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
