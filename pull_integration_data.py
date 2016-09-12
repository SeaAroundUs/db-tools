import os
import optparse
import traceback
import multiprocessing
import sqlprocessor as sp

from tkinter_util import *

from functools import partial
from sqlalchemy import func
from db import getDbConnection
from db import DBConnectionPane
from models import DataTransfer

NUMBER_OF_ALLOCATION_RESULT_PARTITIONS = 150


class PullIntegrationDataCommandPane(tk.Frame):
    def __init__(self, parent, mainDbPane, sourceDbPane, buttonsPerRow = 6, suppressMaterializedViewRefreshButton = FALSE):
        tk.Frame.__init__(self, parent)

        self.parent = parent
        self.mainDbPane = mainDbPane
        self.sourceDbPane = sourceDbPane
        self.buttonsPerRow = buttonsPerRow

        # Placeholders, these will really be initialized lazily latter, once all db parameters are available.
        self.mainDbSession = None
        self.sourceDbSession = None
        self.dataTransfer = None

        scb = tk.Button(parent, text="Get list of integration db tables to pull data down (Postgres)", fg="red", height=1, command=self.setupCommandPane)
        parent.add(scb)

        self.cmdFrame = add_label_frame(parent, "Integration DB Tables To Pull", 100, 350)
        parent.add(self.cmdFrame)

        if not suppressMaterializedViewRefreshButton:
            rmv = tk.Button(parent, text="Refresh all Main DB materialized views",
                            fg="red", command=self.refreshAllMaterializedViews)
            parent.add(rmv)

    def setupCommandPane(self):
        if not self.mainDbPane.isConnectionTestedSuccessfully():
            messagebox.showinfo("Connection not yet tested",
                                "The Main DB Connection has not been tested successfully.\n" +
                                "Once the Main DB Connection has been tested successfully, you can click that button again.")
            return

        for child in self.cmdFrame.winfo_children(): child.destroy()

        i = 0
        row = 0
        column = 0

        sourceDbOpts = optparse.Values(self.sourceDbPane.getDbOptions())
        self.sourceDbSession = getDbConnection(sourceDbOpts).getSession()
        self.mainDbSession = getDbConnection(optparse.Values(self.mainDbPane.getDbOptions())).getSession()

        self.dataTransfer = self.mainDbSession.query(DataTransfer).filter(func.lower(DataTransfer.source_database_name)==func.lower(sourceDbOpts.dbname)) \
            .order_by(DataTransfer.id).all()

        color = "blue"
        for tab in self.dataTransfer:
            self.createCommandButton(self.cmdFrame, tab.target_table_name, tab, row, column, color)
            column += 1

            if column > self.buttonsPerRow:
                column = 0
                row += 1

        row += 1
        tk.Button(self.cmdFrame, text="Pull all integration db tables", fg="red", command=self.pullAllIntegrationDbData) \
            .grid(column=0, row=row, sticky=(E, W, N, S))

        tk.Button(self.cmdFrame, text="Drop foreign keys", fg="red", command=self.dropForeignKey) \
            .grid(column=1, row=row, sticky=(E, W, N, S))

        tk.Button(self.cmdFrame, text="Restore foreign keys", fg="red", command=self.restoreForeignKey) \
            .grid(column=2, row=row, sticky=(E, W, N, S))

        for child in self.cmdFrame.winfo_children(): child.grid_configure(padx=5, pady=5)

    def createCommandButton(self, parent, buttonText, tabDescriptor, gRow, gColumn, color, commandDescription=None):
        tk.Button(parent, text=buttonText, fg=color, command=partial(self.processTable, tabDescriptor)).grid(
                column=gColumn, row=gRow, sticky=E)

    def processTable(self, tabDescriptor):
        opts = self.sourceDbPane.getDbOptions()
        self.downloadAndCopyTable(tabDescriptor, opts)

    def downloadAndCopyTable(self, tabDescriptor, sourceDbOpts):
        targetTable = "{0}.{1}".format(tabDescriptor.target_schema_name, tabDescriptor.target_table_name)

        print("Pulling data for target table %s" % targetTable)

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

        self.mainDbSession.execute("TRUNCATE TABLE %s" % targetTable)

        rawConn = self.mainDbSession.connection().connection
        cursor = rawConn.cursor()
        cursor.execute("SET CLIENT_ENCODING TO 'UTF-8'")
        cursor.copy_expert(sql="copy {0}{1} from STDIN with(encoding 'UTF-8')".format(targetTable, columnList),
                               file=open(inputDataFile, 'r')
                           )
        rawConn.commit()
        cursor.close()

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

    def dropForeignKey(self):
        mainDbOpts = self.mainDbPane.getDbOptions()
        self.mainDbSession.execute("TRUNCATE TABLE admin.database_foreign_key")
        self.mainDbSession.execute(("INSERT INTO admin.database_foreign_key(drop_fk_cmd, add_fk_cmd) " +
                                   "SELECT * FROM get_foreign_key_cmd_by_db_owner(LOWER('%s')) " +
                                   " WHERE drop_fk_cmd IS NOT NULL AND drop_fk_cmd <> ''")
                                   % mainDbOpts['dbname'])
        self.mainDbSession.execute("SELECT exec(drop_fk_cmd) FROM admin.database_foreign_key")
        print("Foreign keys successfully dropped.")
        return

    def restoreForeignKey(self):
        self.mainDbSession.execute("SELECT exec(add_fk_cmd) FROM admin.database_foreign_key")
        print("Foreign keys successfully added.")
        return


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