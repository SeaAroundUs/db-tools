import os
import optparse
import traceback
import multiprocessing
import tkinter as tk
import sqlprocessor as sp

from tkinter import ttk
from tkinter import *
from tkinter import messagebox
from functools import partial

from db import getDbConnection
from db import DBConnectionPane
from models import DataTransfer

NUMBER_OF_ALLOCATION_RESULT_PARTITIONS = 150


class PullIntegrationDataCommandPane(tk.Frame):
    def __init__(self, parent, mainDbPane, sourceDbPane, buttonsPerRow = 6):
        tk.Frame.__init__(self, parent)

        self.parent = parent
        self.mainDbPane = mainDbPane
        self.sourceDbPane = sourceDbPane
        self.buttonsPerRow = buttonsPerRow

        # Placeholders, these will really be initialized lazily latter, once all db parameters are available.
        self.mainDbSession = None
        self.sourceDbSession = None
        self.dataTransfer = None

        scb = tk.Button(parent, text="Get list of integration db tables to pull data down", fg="red", command=self.setupCommandPane)
        parent.add(scb)
        rmv = tk.Button(parent, text="Refresh a materialized views (not necessary if pull all tables is selected)", fg="red", command=self.refreshAllMaterializedViews)
        parent.add(rmv)

        self.cmdFrame = ttk.Labelframe(parent, text='Integration DB Tables To Pull', width=100, height=300)
        self.cmdFrame.grid(column=0, row=0, sticky=(N, W, E, S))
        self.cmdFrame.columnconfigure(0, weight=1)
        self.cmdFrame.rowconfigure(0, weight=1)

        self.parent.add(self.cmdFrame)

    def setupCommandPane(self):
        if not self.mainDbPane.isConnectionTestedSuccessfully():
            messagebox.showinfo("Connection not yet tested",
                                "The Main DB Connection has not been tested successfully.\n" +
                                "Once the Main DB Connection has been tested successfully, you can click that button again.")
            return

        #if self.sourceDbPane is not DBSqlServer:
        #    raise Exception("Source database must be a connection to a Sql Server instance!")

        for child in self.cmdFrame.winfo_children(): child.destroy()

        i = 0
        row = 0
        column = 0

        self.mainDbSession = getDbConnection(optparse.Values(self.mainDbPane.getDbOptions())).getSession()
        self.sourceDbSession = getDbConnection(optparse.Values(self.sourceDbPane.getDbOptions())).getSession()

        self.dataTransfer = self.mainDbSession.query(DataTransfer).filter_by(target_schema_name='web') \
            .order_by(DataTransfer.id).all()

        color = "blue"
        for tab in self.dataTransfer:
            self.createCommandButton(self.cmdFrame, tab.target_table_name, tab, row, column, color)
            column += 1

            if column > self.buttonsPerRow:
                column = 0
                row += 1

        tk.Button(self.cmdFrame, text="Pull all integration db tables", fg="red", command=self.pullAllIntegrationDbData) \
            .grid(column=int(column/2), row=row+1, sticky=(E, W, N, S))

        for child in self.cmdFrame.winfo_children(): child.grid_configure(padx=5, pady=5)

    def createCommandButton(self, parent, buttonText, tabDescriptor, gRow, gColumn, color, commandDescription=None):
        tk.Button(parent, text=buttonText, fg=color, command=partial(self.processTable, tabDescriptor)).grid(
                column=gColumn, row=gRow, sticky=E)

    def processTable(self, tabDescriptor):
        opts = self.sourceDbPane.getDbOptions()
        self.downloadAndCopyTable(tabDescriptor, opts)

    def downloadAndCopyTable(self, tabDescriptor, sourceDbOpts):
        targetTable = "{0}.{1}".format(tabDescriptor.target_schema_name, tabDescriptor.target_table_name)

        if tabDescriptor.target_table_name == "uncertainty_time_period":
            tabDescriptor.source_select_clause = "period_id, numrange(begin_year, end_year, '[]')"
        elif tabDescriptor.target_table_name == "uncertainty_eez":
            tabDescriptor.source_select_clause = "u.eez_id,s.sector_type_id,u.period_id,u.score"

        tabQuery = "(select {0} from {1} {2})".format(tabDescriptor.source_select_clause,
                                                      tabDescriptor.source_table_name,
                                                      tabDescriptor.source_where_clause)

        outputDataFile = "data/" + targetTable

        if self.downloadSourceTable(sourceDbOpts, tabQuery, outputDataFile):
            self.copyToTargetTable(tabDescriptor, targetTable, outputDataFile)

    def downloadSourceTable(self, dbOpts, tabQuery, outputDataFile):
        rawConn = self.sourceDbSession.connection().connection
        cursor = rawConn.cursor()
        cursor.execute("SET CLIENT_ENCODING TO 'UTF-8'")
        cursor.copy_expert(sql="copy {0} to STDOUT with(encoding 'UTF-8')".format(tabQuery),
                           file=open(outputDataFile, 'w')
                          )
        rawConn.commit()
        cursor.close()

        if not (os.path.isfile(outputDataFile) and os.access(outputDataFile, os.R_OK)):
            messagebox.showinfo("Data file download unsuccessful",
                                "Attempt to download data for the table/query '{0}' failed.".format(tabQuery) +
                                "Please check that all parameters for Source DB Connection has been tested entered correctly and try again!")
            return False

        return True

    def copyToTargetTable(self, tabDescriptor, targetTable, inputDataFile):
        if tabDescriptor.target_excluded_columns:
            columnList = self.mainDbSession.execute(
                "SELECT g.col FROM get_table_column('{0}', ARRAY{1}) AS g(col)".format(targetTable,
                                                                                       tabDescriptor.target_excluded_columns))
            columnList = "(" + columnList.first().col + ")"
        else:
            columnList = ""

        add_fk_cmd = None
        tt = targetTable.split(".")
        fk_cmds = self.mainDbSession.execute("SELECT drop_fk_cmd, add_fk_cmd FROM get_table_foreign_key('{0}', '{1}')".format(tt[0], tt[1]))
        for fk_cmd in fk_cmds:
            add_fk_cmd = fk_cmd.add_fk_cmd
            if fk_cmd.drop_fk_cmd:
                self.mainDbSession.execute(fk_cmd.drop_fk_cmd)

        try:
            self.mainDbSession.execute("TRUNCATE TABLE %s" % targetTable)

            rawConn = self.mainDbSession.connection().connection
            cursor = rawConn.cursor()
            cursor.execute("SET CLIENT_ENCODING TO 'UTF-8'")
            cursor.copy_expert(sql="copy {0}{1} from STDIN with(encoding 'UTF-8')".format(targetTable, columnList),
                               file=open(inputDataFile, 'r')
                              )
            rawConn.commit()
            cursor.close()
        finally:
            if add_fk_cmd:
                self.mainDbSession.execute(add_fk_cmd)

        self.mainDbSession.execute("VACUUM ANALYZE %s" % targetTable)

    def pullAllIntegrationDbData(self):
        for tab in self.dataTransfer:
            self.processTable(tab)

        self.refreshAllMaterializedViews()

        print('All integration db tables successfully pulled down.')

    def refreshAllMaterializedViews(self):
        mainDbOpts = self.mainDbPane.getDbOptions()
        mainDbOpts['sqlfile'] = "sql/refresh_matviews.sql"
        mainDbOpts['threads'] = 4
        sp.process(optparse.Values(mainDbOpts))

        print('All materialized views in main db refreshed.')


class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)

        mainPane = ttk.Panedwindow(master, orient=VERTICAL)
        mainDbPane = DBConnectionPane(mainPane, "Main DB Connection", True)
        sourceDbPane = DBConnectionPane(mainPane, "Source DB Connection", True)
        PullIntegrationDataCommandPane(mainPane, mainDbPane, sourceDbPane)
        mainPane.pack(expand=1, fill='both')


# ===============================================================================================
# ----- MAIN
def main():
    root = tk.Tk()
    root.title("Pull Integration DB Data")
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