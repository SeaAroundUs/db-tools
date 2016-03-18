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
import tkinter.filedialog as fdlg


def process(dbPane):
    if not dbPane.isConnectionTestedSuccessfully():
        messagebox.showinfo("Connection not yet tested",
                            "The DB Connection has not been tested successfully.\n" +\
                            "Once the DB Connection has been tested successfully, you can click the Process button again.")
        return

    sp.process(optparse.Values(dbPane.getDbOptions()))


class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)
        self.mainframe = ttk.Panedwindow(master, orient=VERTICAL)
        self.mainframe.grid(column=0, row=0, sticky=(N, W, E, S))
        self.mainframe.columnconfigure(0, weight=1)
        self.mainframe.rowconfigure(0, weight=1)

        self.dbPane = DBConnectionPane(self.mainframe, "DB Connection", True, True)

        self.processFrame = ttk.Labelframe(self.mainframe, text="Process", width=400, height=50)
        self.processFrame.grid(column=0, row=0, sticky=(N, W, E, S))
        self.processFrame.columnconfigure(0, weight=1)
        self.processFrame.rowconfigure(0, weight=1)
        pb = tk.Button(self.processFrame, text="  Process  ", fg="red", command=partial(process, self.dbPane))
        pb.place(relx=0.5, rely=0.5, anchor=CENTER)

        self.mainframe.add(self.processFrame)

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
