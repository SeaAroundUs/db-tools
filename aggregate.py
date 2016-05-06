import optparse
import traceback
import multiprocessing
import tkinter as tk
from tkinter import ttk
from tkinter import *
from tkinter import messagebox
import sqlprocessor as sp
from functools import partial
from db import getDbConnection
from db import DBConnectionPane


class AggregateCommandPane(tk.Frame):
    BUTTON_LABELS = ["All Area Types", "EEZ", "Highseas", "LME", "RFMO", "Global"]

    AREA_SQL_FILES = [None,
                      "sql/aggregate_eez.sql",
                      "sql/aggregate_high_seas.sql",
                      "sql/aggregate_lme.sql",
                      "sql/aggregate_rfmo.sql",
                      "sql/aggregate_global.sql"]
    
    def __init__(self, parent, dbPane, isVerticallyAligned=False, descriptions=None):
        tk.Frame.__init__(self, parent)
        self.dbPane = dbPane
        self.isVerticallyAligned = isVerticallyAligned
        self.descriptions = descriptions
        self.index_create_cmds = []

        self.createUiFrame(parent, "V_Fact_Data", self.addVFactDataUiComponents)
        self.createUiFrame(parent, "Aggregate", self.addAggregateUiComponents)

    def createUiFrame(self, parent, name, addComponents):
        uiFrame = ttk.Labelframe(parent, text=name, width=100, height=100)
        uiFrame.grid(column=0, row=0, sticky=(N, W))
        uiFrame.columnconfigure(0, weight=1)
        uiFrame.rowconfigure(0, weight=1)

        addComponents(uiFrame)

        for child in uiFrame.winfo_children(): child.grid_configure(padx=5, pady=5)

        parent.add(uiFrame)

    def addVFactDataUiComponents(self, parent):
        tk.Button(parent, text="Drop indexes before aggregation", fg="blue", command=self.dropIndexes) \
            .grid(column=0, row=1, sticky=E)
        tk.Button(parent, text="Recreate indexes post aggregation", fg="blue", command=self.recreateIndexes) \
            .grid(column=1, row=1, sticky=E)

    def addAggregateUiComponents(self, parent):
        row = 0
        column = 0

        for i in range(len(AggregateCommandPane.BUTTON_LABELS)):
            if i == 0:
                color = "red"
            else:
                color = "blue"

            if self.isVerticallyAligned:
                row += 1
            else:
                column += 1

            if self.descriptions:
                self.createCommandButton(parent, AggregateCommandPane.BUTTON_LABELS[i],
                                         AggregateCommandPane.AREA_SQL_FILES[i], row, column, color, self.descriptions[i])
            else:
                self.createCommandButton(parent, AggregateCommandPane.BUTTON_LABELS[i],
                                         AggregateCommandPane.AREA_SQL_FILES[i], row, column, color)

    def createCommandButton(self, parent, buttonText, sqlFile, gRow, gColumn, color, commandDescription=None):
        if sqlFile:
            tk.Button(parent, text=buttonText, fg=color, command=partial(self.kickoffSqlProcessor, sqlFile)) \
                .grid(column=gColumn, row=gRow, sticky=E)
        else:
            tk.Button(parent, text=buttonText, fg=color, command=self.aggregateAll) \
                .grid(column=gColumn, row=gRow, sticky=E)

        if commandDescription:
            tk.Label(parent, text=commandDescription).grid(column=gColumn+1, row=gRow, sticky=W)

    def postAggregationOperations(self):
        opts = self.dbPane.getDbOptions()
        dbConn = getDbConnection(optparse.Values(opts))
        if 'threads' not in opts or opts['threads'] == 0:
            opts['threads'] = 8

        print("Merging Unknown fishing entity in catch data...")
        dbConn.execute("UPDATE web.v_fact_data SET fishing_entity_id = 213 WHERE fishing_entity_id = 223")

        print("Vacuuming v_fact_data afterward...")
        dbConn.execute("vacuum analyze web.v_fact_data")

        # And now refresh all materialized views as most are dependent on data in the v_fact_data table
        opts['sqlcmd'] = "SELECT 'refresh materialized view web.' || table_name FROM matview_v('web') WHERE table_name NOT LIKE 'TOTALS%'"
        sp.process(optparse.Values(opts))
        opts['sqlcmd'] = "SELECT 'vacuum analyze web.' || table_name FROM matview_v('web') WHERE table_name NOT LIKE 'TOTALS%'"
        sp.process(optparse.Values(opts))

        print("Aggregation process completed...")

        dbConn.close()

    def kickoffSqlProcessor(self, sqlFileName, isPostOpsRequired=True):
        opts = self.dbPane.getDbOptions()
        opts['sqlfile'] = sqlFileName
        if 'threads' not in opts or opts['threads'] == 0:
            opts['threads'] = 8
        sp.process(optparse.Values(opts))

        if isPostOpsRequired:
            self.postAggregationOperations()

    def aggregateAll(self):
        for sqlFile in AggregateCommandPane.AREA_SQL_FILES:
            if sqlFile:
                self.kickoffSqlProcessor(sqlFile, False)

        self.postAggregationOperations()

    def dropIndexes(self):
        if len(self.index_create_cmds) > 0:
            messagebox.showinfo("Index drop previously executed",
                                "A prior execution of index drop has been detected. This invocation is aborted.")
            return

        opts = self.dbPane.getDbOptions()
        dbConn = getDbConnection(optparse.Values(opts))
        indexes = dbConn.execute("SELECT (schemaname || '.' || indexname) index_name, indexdef index_create_cmd \
                                    FROM pg_indexes \
                                   WHERE tablename = 'v_fact_data' AND indexname NOT LIKE 'v_fact_data_pkey'")
        for index in indexes:
            self.index_create_cmds.append(index.index_create_cmd)
            dbConn.execute("DROP INDEX %s" % index.index_name)

        dbConn.close()

    def recreateIndexes(self):
        if len(self.index_create_cmds) == 0:
            messagebox.showinfo("Prior index drop",
                                "A prior execution of index drop has not been detected. This invocation is aborted.")
            return

        opts = self.dbPane.getDbOptions()
        dbConn = getDbConnection(optparse.Values(opts))

        for index_create_cmd in self.index_create_cmds:
            dbConn.execute(index_create_cmd)

        dbConn.close()


class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)

        mainPane = ttk.Panedwindow(master, orient=VERTICAL)
        dbPane = DBConnectionPane(mainPane, "DB Connection", True)
        AggregateCommandPane(mainPane, dbPane, False, None)
        mainPane.pack(expand=1, fill='both')


# ===============================================================================================
# ----- MAIN
def main():
    root = tk.Tk()
    root.title("Aggregation")
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

# Sample construction with description list
# CommandPane(parent, True, ['Aggregrate data for all marine layers',
#                            'Aggregrate data for marine layer 1',
#                            'Aggregrate data for marine layer 2',
#                            'Aggregrate data for marine layer 3',
#                            'Aggregrate data for marine layer 4',
#                            'Aggregrate data for marine layer 6'])
