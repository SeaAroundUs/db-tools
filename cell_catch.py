import optparse
import traceback
import multiprocessing
import tkinter as tk
from tkinter import ttk
from tkinter import *
import sqlprocessor as sp
from functools import partial
from tkinter import messagebox
from db import getDbConnection
from db import DBConnectionPane


class CellCatchCommandPane(tk.Frame):
    INSERT_CELL_CATCH_PARTITION_CMD = "select allocation.generate_insert_cell_catch_partition_statements(%s)"

    def __init__(self, parent, dbPane):
        tk.Frame.__init__(self, parent)
        self.dbPane = dbPane
        self.buttonsPerRow = 10

        # Placeholders, these will really be initialized lazily latter, once all db parameters are available.
        self.yearList = None

        self.cmdFrame = ttk.Labelframe(parent, text='Aggregate Cell Catch', width=100, height=250)
        self.cmdFrame.grid(column=0, row=0, sticky=(N, W))
        self.cmdFrame.columnconfigure(0, weight=1)
        self.cmdFrame.rowconfigure(0, weight=1)

        parent.add(tk.Button(parent, text="Get list of cell catch year partitions to aggregate", fg="red", command=self.setupCommandPane))

        parent.add(self.cmdFrame)

    def setupCommandPane(self):
        if not self.dbPane.isConnectionTestedSuccessfully():
            messagebox.showinfo("Connection not yet tested",
                                "The Main DB Connection has not been tested successfully.\n" + \
                                "Once the Main DB Connection has been tested successfully, you can click that button again.")
            return

        i = 0
        row = 0
        column = 0

        try:
            dbSession = getDbConnection(optparse.Values(self.dbPane.getDbOptions())).getSession()
            self.yearList = dbSession.execute(
                "SELECT replace(table_name, 'allocation_data_', '')::INT AS year " +
                "  FROM schema_v('allocation_data_partition') " +
                " WHERE table_name NOT LIKE 'TOTALS%'" +
                " ORDER BY 1").fetchall()

            color = "blue"
            for par in self.yearList:
                self.createCommandButton(self.cmdFrame, par.year, row, column, color)
                column += 1

                if column >= self.buttonsPerRow:
                    column = 0
                    row += 1
        finally:
            dbSession.close()

        tk.Button(self.cmdFrame, text="All", fg="red", command=self.aggregateAllPartition).grid(
            column=column, row=row, sticky=E)

        for child in self.cmdFrame.winfo_children(): child.grid_configure(padx=5, pady=5)

    def createCommandButton(self, parent, year, gRow, gColumn, color):
        tk.Button(parent, text=year, fg=color, command=partial(self.processYearPartition, year)).grid(
            column=gColumn, row=gRow, sticky=E)

    def processYearPartition(self, year):
        opts = self.dbPane.getDbOptions()

        try:
            dbSession = getDbConnection(optparse.Values(opts)).getSession()
            dbSession.execute("TRUNCATE TABLE allocation.allocation_data_partition_udi")
            dbSession.execute("SELECT allocation.populate_allocation_data_partition_udi(%s)" % year)
            dbSession.execute("VACUUM ANALYZE allocation.allocation_data_partition_udi")

            opts['sqlfile'] = None
            opts['sqlcmd'] = "SELECT allocation.generate_insert_cell_catch_partition_statements(%s)" % year
            if 'threads' not in opts or opts['threads'] == 0:
                opts['threads'] = 8
            sp.process(optparse.Values(opts))

            # Post insertions operation to finalize the target cell catch partition for immediate use
            cellCatchPartition = "cell_catch_p%s" % year
            dbSession.execute("VACUUM ANALYZE web_partition.%s" % cellCatchPartition)
            for indexSql in dbSession.execute(
                "SELECT web_partition.maintain_cell_catch_indexes('%s') AS cmd" % cellCatchPartition).fetchall():
                dbSession.execute(indexSql.cmd)
        finally:
            dbSession.close()

    def aggregateAllPartition(self):
        for par in self.yearList:
            self.processYearPartition(par.year, False)


class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)

        mainPane = ttk.Panedwindow(master, orient=VERTICAL)
        dbPane = DBConnectionPane(mainPane, "DB Connection", True)
        CellCatchCommandPane(mainPane, dbPane)
        mainPane.pack(expand=1, fill='both')


# ===============================================================================================
# ----- MAIN
def main():
    root = tk.Tk()
    root.title("Cell Catch Aggregation")
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
