import optparse
import sqlprocessor as sp

from tkinter_util import *


class SqlProcessorGuiCommandPane(tk.Frame):
    def __init__(self, parent, dbPane, promptForSqlFile=False):
        tk.Frame.__init__(self, parent)
        self.dbPane = dbPane
        self.dbConn = None

        self.promptForSqlFile = promptForSqlFile

        if promptForSqlFile:
            sqlFrame = add_label_frame(parent, "Sql", 400, 50)

            self.db_sqlfile = StringVar()
            self.db_sqlcmd = StringVar()
            self.db_threads = IntVar()

            cmd_row = add_data_entry(sqlFrame, 0, self.db_sqlfile, "db_sqlfile", 80)
            cmd_row = add_data_entry(sqlFrame, cmd_row, self.db_sqlcmd, "db_sqlcmd", 80)
            self.db_threads.set(4)
            cmd_row = add_data_entry(sqlFrame, cmd_row, self.db_threads, "db_threads", 3)

            grid_panel(sqlFrame)

            parent.add(sqlFrame)
        
        processFrame = add_label_frame(parent, "Process", 400, 50)
         
        pb = tk.Button(processFrame, text="  Process  ", fg="red", command=self.process)
        pb.place(relx=0.5, rely=0.5, anchor=CENTER)

        parent.add(processFrame)

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

# ===============================================================================================
# ----- MAIN
if __name__ == "__main__":  
    Application("SqlProcessor GUI", SqlProcessorGuiCommandPane, include_threads=True, include_sqlfile=True).run()