import boto3
import botocore

from db import DBConnectionPane

from tkinter_util import *


class RdsCommandPane(tk.Frame):
    def __init__(self, parent, dbPane):
        tk.Frame.__init__(self, parent)
        self.dbPane = dbPane
        self.dbConn = None
        self.client = boto3.client('rds')

        self.instance = StringVar()
        self.size = StringVar()
        self.identifier = StringVar()   

        self.serverFrame = add_label_frame(parent, "Relational Database Service", 400, 50)

        dbOpts = self.dbPane.getDbOptions()
        server = dbOpts["server"]
        if server:
            self.instance.set(self.get_db_instance_class(server.split(".")[0]))
        else:
            self.instance.set("Server not specified. Test connection before refreshing.")
        cmd_row = add_command(self.serverFrame, 0, "Current Instance Size", self.instance, "Refresh", self.refreshInstance, readonly=True)

        lsb = Spinbox(self.serverFrame, textvariable=self.size, width=59, values=("db.t2.large", "db.m4.large", "db.m4.2xlarge", "db.m4.4xlarge"), state=NORMAL) 
        cmd_row = add_command(self.serverFrame, cmd_row, "Scale to", lsb, "Update", self.modify_instance, readonly=True)

        grid_panel(self.serverFrame)

        parent.add(self.serverFrame)

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


if __name__ == "__main__":
    tkinter_client_main(Application, "RDS")