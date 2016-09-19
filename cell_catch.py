import optparse
import sqlprocessor as sp
from functools import partial
from db import getDbConnection

from tkinter_util import *


class CellCatchCommandPane(tk.Frame):
    INSERT_CELL_CATCH_PARTITION_CMD = "select allocation.generate_insert_cell_catch_partition_statements(%s)"

    def __init__(self, parent, dbPane):
        tk.Frame.__init__(self, parent)
        self.dbPane = dbPane
        self.buttonsPerRow = 10

        # Placeholders, these will really be initialized lazily latter, once all db parameters are available.
        self.yearList = None
                                                  
        self.cmdFrame = add_label_frame(parent, "Aggregate Cell Catch", 100, 250)

        parent.add(tk.Button(parent, text="Get list of cell catch year partitions to aggregate", fg="red", command=self.setupCommandPane))

        parent.add(self.cmdFrame)

    def setupCommandPane(self):
        if not self.dbPane.isConnectionTestedSuccessfully():
            popup_message("Connection not yet tested",
                          "The Main DB Connection has not been tested successfully.\n" + \
                          "Once the Main DB Connection has been tested successfully, you can click that button again.")
            return

        for child in self.cmdFrame.winfo_children(): child.destroy()

        i = 0
        row = 0
        column = 0
                     
        try:
            opts = self.dbPane.getDbOptions()
            dbSession = getDbConnection(optparse.Values(opts)).getSession()

            # Rebuild the allocation_data partitions to make sure we are using the freshest allocation data
            partitions = dbSession.execute(
                "SELECT ('allocation_data_partition.' || table_name) AS partition_name" +
                "  FROM schema_v('allocation_data_partition') " +
                " WHERE table_name NOT LIKE 'TOTALS%'" +
                " ORDER BY 1").fetchall()

            for partition in partitions:
                dbSession.execute("DROP TABLE %s" % partition.partition_name)

            dbSession.execute("SELECT allocation.maintain_allocation_data_partition()")

            opts['sqlfile'] = "sql/insert_allocation_data_eez_hs.sql"
            if 'threads' not in opts or opts['threads'] == 0:
                opts['threads'] = 8
            sp.process(optparse.Values(opts))

            # Add buttons to command pane
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

        for child in self.cmdFrame.winfo_children():
            child.grid_configure(padx=5, pady=5)

    def createCommandButton(self, parent, year, gRow, gColumn, color):
        tk.Button(parent, name=str(year), text=str(year), fg=color, command=partial(self.processYearPartition, year)).grid(
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
            opts['threads'] = 16
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
            self.processYearPartition(par.year)

        print("Batch processing of all available years completed!")


# ===============================================================================================
# ----- MAIN                   
if __name__ == "__main__":
    Application("Cell Catch Aggregation", CellCatchCommandPane).run()