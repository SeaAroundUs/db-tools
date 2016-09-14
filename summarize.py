import optparse
import sqlprocessor as sp
from functools import partial
from db import getDbConnection
from db import DBConnectionPane

from tkinter_util import *


class SummarizeCommandPane(tk.Frame):
    GLOBAL_AVERAGE_UNIT_PRICE = 1466.0

    BUTTON_LABELS  = ["All Area Types", "EEZ", "Highseas", "LME", "RFMO", "Global"]

    SUMMARY_TABLES = [None,
                      "allocation_result_eez",
                      "allocation_result_high_seas",
                      "allocation_result_lme",
                      "allocation_result_rfmo",
                      "allocation_result_global"]

    def __init__(self, parent, dbPane, isVerticallyAligned=False, descriptions=None):
        tk.Frame.__init__(self, parent)
        self.dbPane = dbPane

        cmdFrame = add_label_frame(parent, "Summarize", 100, 100)

        row = 0
        column = 0

        for i in range(len(SummarizeCommandPane.BUTTON_LABELS)):
            if i == 0:
                color = "red"
            else:
                color = "blue"

            if isVerticallyAligned:
                row += 1
            else:
                column += 1

            if descriptions:
                self.createCommandButton(cmdFrame, SummarizeCommandPane.BUTTON_LABELS[i], SummarizeCommandPane.SUMMARY_TABLES[i], row, column, color, descriptions[i])
            else:
                self.createCommandButton(cmdFrame, SummarizeCommandPane.BUTTON_LABELS[i], SummarizeCommandPane.SUMMARY_TABLES[i], row, column, color)

        grid_panel(cmdFrame)

        parent.add(cmdFrame)

    def createCommandButton(self, parent, buttonText, summaryTable, gRow, gColumn, color, commandDescription=None):
        if summaryTable:
            tk.Button(parent, text=buttonText, fg=color, command=partial(self.kickoffSqlProcessor, summaryTable)).grid(
                column=gColumn, row=gRow, sticky=E)
        else:
            tk.Button(parent, text=buttonText, fg=color, command=self.summarizeAll).grid(column=gColumn, row=gRow, sticky=E)

        if commandDescription:
            tk.Label(parent, text=commandDescription).grid(column=gColumn+1, row=gRow, sticky=W)

    def postAggregationOperations(self, summaryTable):
        opts = self.dbPane.getDbOptions()
        dbConn = getDbConnection(optparse.Values(opts))

        print("Updating allocation data unit price...")
        if 'threads' not in opts or opts['threads'] == 0:
            opts['threads'] = 8
        opts['sqlfile'] = "sql/update_allocation_data_unit_price.sql"
        sp.process(optparse.Values(opts))
        dbConn.execute("UPDATE allocation.allocation_data SET unit_price = %s WHERE unit_price IS NULL" % SummarizeCommandPane.GLOBAL_AVERAGE_UNIT_PRICE)
        dbConn.execute("VACUUM ANALYZE allocation.allocation_data")

        print("Vacuum and analyze target summary table(s)...")
        if summaryTable:
            dbConn.execute("VACUUM ANALYZE allocation.%s" % summaryTable)
        else:
            # if input summaryTable = None, it's really the signal to vacuum analyze all summary tables
            for tab in SummarizeCommandPane.SUMMARY_TABLES:
                if tab:
                    dbConn.execute("VACUUM ANALYZE allocation.%s" % tab)

        print("Summarization process completed...")

        dbConn.close()

    def kickoffSqlProcessor(self, summaryTable, isPostOpsRequired=True):
        opts = self.dbPane.getDbOptions()
        dbConn = getDbConnection(optparse.Values(opts))
        dbConn.execute("TRUNCATE allocation.%s" % summaryTable)
        opts['sqlfile'] = "sql/summarize_%s.sql" % summaryTable
        if 'threads' not in opts or opts['threads'] == 0:
            opts['threads'] = 8
        sp.process(optparse.Values(opts))

        if isPostOpsRequired:
            self.postAggregationOperations(summaryTable)

    def summarizeAll(self):
        for summaryTable in SummarizeCommandPane.SUMMARY_TABLES:
            if summaryTable:
                self.kickoffSqlProcessor(summaryTable, False)
        self.postAggregationOperations(None)


class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)

        mainPane = ttk.Panedwindow(master, orient=VERTICAL)
        dbPane = DBConnectionPane(mainPane, "DB Connection", True)
        SummarizeCommandPane(mainPane, dbPane, False, None)
        mainPane.pack(expand=1, fill='both')


# ===============================================================================================
# ----- MAIN
if __name__ == "__main__":
    tkinter_client_main(Application, "Summarization")

        # CommandPane(parent, True, ['Summarize data for all marine layers',
        #                            'Summarize data for marine layer 1',
        #                            'Summarize data for marine layer 2',
        #                            'Summarize data for marine layer 3',
        #                            'Summarize data for marine layer 4',
        #                            'Summarize data for marine layer 6'])
