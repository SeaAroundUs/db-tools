import optparse
import traceback
import multiprocessing

import sqlprocessor as sp

from db import DBConnectionPane

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