import time
import sys
import boto3
import botocore
import traceback
import multiprocessing
from functools import partial

import sqlprocessor as sp
from db import DBConnectionPane

import tkinter as tk
from tkinter import ttk
from tkinter import *
from tkinter import messagebox

class RdsCommandPane(tk.Frame):
    def __init__(self, parent, dbPane):
        tk.Frame.__init__(self, parent)
        self.dbPane = dbPane
        self.dbConn = None
        self.client = boto3.client('rds')

        self.instance = StringVar()
        self.size = StringVar()
        self.identifier = StringVar()   

        self.serverFrame = ttk.Labelframe(parent, text="Relational Database Service", width=400, height=50)
        self.serverFrame.grid(column=0, row=0, sticky=(N, W, E, S))
        self.serverFrame.columnconfigure(0, weight=1)
        self.serverFrame.rowconfigure(0, weight=1)

        self.entry_row = 0
        dbOpts = self.dbPane.getDbOptions()
        server = dbOpts["server"]
        if server:
            self.instance.set(self.get_db_instance_class(server.split(".")[0]))
        else:
            self.instance.set("Server not specified. Test connection before refreshing.")
        self.add_data_entry(self.serverFrame, self.instance, "Current Instance Size", 60, "Refresh", self.refreshInstance, readonly=True)

        lsb = Spinbox(self.serverFrame, textvariable=self.size, width=59, values=("db.t2.large", "db.m4.large", "db.m4.2xlarge", "db.m4.4xlarge"), state=NORMAL) 
        self.add_data_entry(self.serverFrame, lsb, "Scale to", 40, "Update", self.modify_instance, readonly=True)

        for child in self.serverFrame.winfo_children(): child.grid_configure(padx=5, pady=5)

        parent.add(self.serverFrame)

    def add_data_entry(self, panel, entry_var, entry_text, entry_len, cmd_text, cmd, readonly=False):
        self.entry_row += 1
        tk.Label(panel, text=entry_text).grid(column=0, row=self.entry_row, sticky=W)
        
        # if entry_var is a widget already, we just need to grid it. otherwise, we create an input Entry to wrap it.
        if isinstance(entry_var, Widget):
            data_entry = entry_var
        else:
            data_entry = tk.Entry(panel, width=entry_len, textvariable=entry_var)

        if readonly == True:
            data_entry.bind("<Key>", lambda e: "break")

        data_entry.grid(column=1, row=self.entry_row, sticky=W)

        tk.Button(panel, text=cmd_text, command= cmd).grid(column=2, row=self.entry_row, sticky=W)

    def refreshInstance(self):
        dbOpts = self.dbPane.getDbOptions()
        server = dbOpts["server"]
        if server:
            getInstance = self.get_db_instance_class(server.split(".")[0])
            if getInstance is not None:
                self.instance.set(getInstance)
                self.identifier.set(server.split(".")[0])
            else:
                self.instance.set("Server is still being scaled...")
        else:
            self.instance.set("Server not specified. Test connection before refreshing.")

    def get_db_instance_class(self, instance_name):
        response = self.client.describe_db_instances()

        if response:
            instances = response.get('DBInstances')

            for instance in instances:
                identifier = instance.get('DBInstanceIdentifier')
                if identifier == instance_name:
                    instance_class = instance.get('DBInstanceClass')
                    status = instance.get('DBInstanceStatus')

                    if status == 'available':
                        print('Server is available and ready to be modified')
                        return instance_class
                    else:
                        print("Instance %s not found or not currently available" % instance_name)
                        break

        return None

    def modify_instance(self):
        try:
            result = messagebox.askokcancel("Server Scaling", "Scale this server?")
            if result == True:
                print("Checking server...")
            else:
                print("Cancelled")
                return
            instance_class = self.get_db_instance_class(self.identifier.get())
            if instance_class == None:
                return
            elif instance_class == self.size.get():
                print("Instance %s is already %s, no further modification needed." % (self.identifier.get(), self.size.get()))
            else:
                print("Server is being scaled... refresh instance for progress.")
                self.client.modify_db_instance(ApplyImmediately=True,
                                               DBInstanceIdentifier=self.identifier.get(),
                                               DBInstanceClass=self.size.get())
                                     
        except botocore.exceptions.ClientError as e:
            print(sys.exc_info())
            raise

class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)

        mainPane = ttk.Panedwindow(master, orient=VERTICAL)
        dbPane = DBConnectionPane(parent=mainPane, title="DB Connection", include_threads=False, include_sqlfile=False)

        print("\nImportant Note: The instance MUST be scaled back to its default setting.\n\n +\
            db.t2.large - QA default setting\n +\
            db.m4.large - Prod default setting\n +\
            db.m4.2xlarge - 8 concurrent threads\n +\
            db.m4.4xlarge - 16 concurrent threads \n")

        RdsCommandPane(mainPane, dbPane)
        mainPane.pack(expand=1, fill='both')

def main():
    root = tk.Tk()
    root.title("RDS")
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