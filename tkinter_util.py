import multiprocessing
import traceback
import tkinter as tk
from tkinter import ttk
from tkinter import *
from tkinter import messagebox
from db import *


class Application(tk.Frame):
    def __init__(self, app_title, command_pane_class=None, **kargs):
        self.root = tk.Tk()
        self.root.title(app_title)

        if command_pane_class:
            tk.Frame.__init__(self, self.root)

            self.mainPane = ttk.Panedwindow(self.root, orient=VERTICAL)

            if "include_threads" in kargs:
                include_threads = kargs["include_threads"]
            else:
                include_threads = False

            if "include_sqlfile" in kargs:
                include_sqlfile = kargs["include_sqlfile"]
            else:
                include_sqlfile = False

            self.mainDbPane = DBConnectionPane(self.mainPane, "Main DB Connection", include_threads, include_sqlfile)

            include_threads = False, include_sqlfile

            if "include_source_db" in kargs and kargs["include_source_db"]:
                self.sourceDbPane = DBConnectionPane(self.mainPane, "Source DB Connection", include_threads, include_sqlfile)
                command_pane_class(self.mainPane, self.mainDbPane, self.sourceDbPane, **kargs)
            else:
                command_pane_class(self.mainPane, self.mainDbPane, **kargs)

            self.mainPane.pack(expand=1, fill='both')

    def run(self, execute_instead=None):
        try:
            multiprocessing.freeze_support()
            if execute_instead:
                execute_instead.mainloop()
            else:
                self.mainloop()

        except SystemExit as x:
            sys.exit(x)

        except Exception:
            strace = traceback.extract_tb(sys.exc_info()[2])[-1:]
            lno = strace[0][1]
            print('Unexpected Exception on line: {0}'.format(lno))
            print(sys.exc_info())
            sys.exit(1)


class DBConnectionPane(tk.Frame):
    def __init__(self, parent, title, include_threads=False, include_sqlfile=False):
        super(DBConnectionPane, self).__init__()

        self.pane = add_label_frame(parent, title, 400, 200)

        self.db_type = StringVar()
        self.db_server = StringVar()
        self.db_port = IntVar()
        self.db_name = StringVar()
        self.db_username = StringVar()
        self.db_password = StringVar()
        self.db_sqlfile = StringVar()
        self.db_sqlcmd = StringVar()  
        self.db_threads = IntVar()

        self.db_type.set("postgres")
        entry_row = add_data_entry(self.pane, 0, self.db_type, "db_type", 10)
        entry_row = add_data_entry(self.pane, entry_row, self.db_server, "db_server", 55)
        self.db_port.set(5432)
        entry_row = add_data_entry(self.pane, entry_row, self.db_port, "db_port", 5)
        entry_row = add_data_entry(self.pane, entry_row, self.db_name, "db_name", 30)
        entry_row = add_data_entry(self.pane, entry_row, self.db_username, "db_username", 30)
        entry_row = add_data_entry(self.pane, entry_row, self.db_password, "db_password", 30, hidden=True)

        if include_sqlfile == True:
            entry_row = add_data_entry(self.pane, entry_row, self.db_sqlfile, "db_sqlfile", 80)
            entry_row = add_data_entry(self.pane, entry_row, self.db_sqlcmd, "db_sqlcmd", 80)

        if include_threads == True:
            self.db_threads.set(4)
            entry_row = add_data_entry(self.pane, entry_row, self.db_threads, "db_threads", 3)
        else:
            self.db_threads.set(0)

        self.tcbt = tk.Button(self.pane, text=" Test connection ", fg="red", command=self.testConnection)
        self.tcbt.grid(column=0, row=entry_row, sticky=W)
        self.tcbt.bind("<Return>", (lambda event: self.testConnection()))

        grid_panel(self.pane)

        parent.add(self.pane)

        # Below are to detect if a connection has been tested successfully
        self.connectionTested = False
        for entry in (self.db_type, self.db_server, self.db_port, self.db_name, self.db_username, self.db_password):
            entry.trace_variable('w', self.resetConnectionTested)  
     
    def getDbOptions(self):
        options = dict()
        options['dbtype'] = self.db_type.get()
        options['server'] = self.db_server.get()
        options['port'] = self.db_port.get()
        options['dbname'] = self.db_name.get()
        options['username'] = self.db_username.get()
        options['password'] = self.db_password.get()
        options['threads'] = self.db_threads.get()
        options['sqlfile'] = self.db_sqlfile.get()
        options['sqlcmd'] = self.db_sqlcmd.get()
        return options

    def isConnectionTestedSuccessfully(self):
        return self.connectionTested

    def resetConnectionTested(self, *args):
        # If db_type is set to sqlserver we should change the default value for the port parameter accordingly
        if self.db_type.get() == "postgres":
            self.db_port.set(5432)
        elif self.db_type.get() == "sqlserver":
            self.db_port.set(1433)
        elif self.db_type.get() == "mysql":
            self.db_port.set(3306)

        self.connectionTested = False

    def testConnection(self):
        conn = getDbConnection(optparse.Values(self.getDbOptions()))
        conn.close()

        self.connectionTested = True

        messagebox.showinfo("Test connection", "Connection successfully made!")


def add_label_frame(parent, title, frame_width, frame_height):
    frame = ttk.Labelframe(parent, text=title, width=frame_width, height=frame_height)
    frame.grid(column=0, row=0, sticky=(N, W, E, S))
    frame.columnconfigure(0, weight=1)
    frame.rowconfigure(0, weight=1)
    return frame


def add_data_entry(panel, entry_row, entry_var, entry_text, entry_len, hidden=False, readonly=False):
    tk.Label(panel, text=entry_text).grid(column=0, row=entry_row, sticky=W)

    # if entry_var is a widget already, we just need to grid it. otherwise, we create an input Entry to wrap it.
    if isinstance(entry_var, Widget):
        data_entry = entry_var
    else:
        if hidden == True:
            data_entry = tk.Entry(panel, width=entry_len, textvariable=entry_var, show="*")
        else:
            data_entry = tk.Entry(panel, width=entry_len, textvariable=entry_var)

    if readonly == True:
        data_entry.bind("<Key>", lambda e: "break")

    data_entry.grid(column=1, row=entry_row, sticky=W)

    return (entry_row + 1)


def add_command(panel, command_row, label_text, entry_var, cmd_text=None, cmd=None, readonly=False):
    tk.Label(panel, text=label_text).grid(column=0, row=command_row, sticky=W)

    # if entry_var is a widget already, we just need to grid it. otherwise, we create an input Entry to wrap it.
    if isinstance(entry_var, Widget):
        data_entry = entry_var
    else:
        data_entry = tk.Entry(panel, width=60, textvariable=entry_var)

    if readonly == True:
        data_entry.bind("<Key>", lambda e: "break")

    data_entry.grid(column=0, row=command_row + 1, sticky=W)

    if cmd:
        tk.Button(panel, text=cmd_text, command=cmd).grid(column=1, row=command_row+1, sticky=W)

    return (command_row + 2)


def add_check_box(panel, chkbox_row, label_text, entry_var):
    chkBox = Checkbutton(panel, variable=entry_var)
    add_data_entry(panel, chkbox_row, chkBox, label_text, None)

    return (chkbox_row + 1)


def add_pane(parent, db_pane, command_pane_class, add_filler_pane=False):
    pane = ttk.Panedwindow(parent, orient=VERTICAL)

    command_pane_class(pane, db_pane)

    if add_filler_pane:
        pane.add(ttk.Panedwindow(pane, orient=VERTICAL))

    return pane


# direction is either: "horizontal" or "vertical"
def add_buttons(panel, data, row=1, column=1, direction="horizontal"):
    if direction.lower() == "horizontal":
        row_inc = 0
        column_inc = 1
    elif direction.lower() == "vertical":
        row_inc = 1
        column_inc = 0
    else:
        raise ValueError("Direction should be one of horizontal or vertical only.")

    for datum in data:
        datum_len = len(datum)

        if datum_len == 2:
            button = tk.Button(panel, text=datum[0], command=datum[1])
        elif datum_len >= 3:
            button = tk.Button(panel, text=datum[0], command=datum[1], fg=datum[2])

            if datum_len > 3:
                tk.Label(panel, text=datum[3]).grid(column=column+1, row=row, sticky="W")
        else:
            raise ValueError("Button initialization data should at least include button text and command: %s" % datum)

        button.grid(row=row, column=column, sticky=(W, E, S))

        row += row_inc
        column += column_inc

    return row


def add_separator(panel, row):
    ttk.Separator(panel).grid(row=row, columnspan=2, sticky="ew")

    return (row + 1)


def grid_panel(panel, spacing = 5):
    for child in panel.winfo_children():
        child.grid_configure(padx=spacing, pady=spacing)


def popup_message(title, msg):
    messagebox.showinfo(title, msg)