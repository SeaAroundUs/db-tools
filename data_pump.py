import traceback
import multiprocessing
import tkinter as tk
from tkinter import ttk
from tkinter import *
from db import DBConnectionPane
from rds import RdsCommandPane
from pull_integration_data import PullIntegrationDataCommandPane
from pull_allocation_data import PullAllocationDataCommandPane
from summarize import SummarizeCommandPane
from aggregate import AggregateCommandPane
from cell_catch import CellCatchCommandPane
from taxon_extent import TaxonExtentCommandPane
from cache_data import CacheDataCommandPane
from sqlprocessor_gui import SqlProcessorGuiCommandPane

root = tk.Tk()
root.title("SAU Data Pump")


class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)

        mainNB = ttk.Notebook(root, width=680, height=520)

        dbPane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        mainDB = DBConnectionPane(dbPane, 'Main DB')
        sourceDB = DBConnectionPane(dbPane, 'Source DB')

        # first tab
        rdsPane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        RdsCommandPane(
            rdsPane,
            mainDB
        )
        rdsPane.add(ttk.Panedwindow(rdsPane, orient=VERTICAL))        

        # second tab
        pullDataPane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        PullIntegrationDataCommandPane(pullDataPane, mainDB, sourceDB, 4, TRUE)
        PullAllocationDataCommandPane(pullDataPane, mainDB, sourceDB)

        # third tab
        summarizePane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        SummarizeCommandPane(
            summarizePane,
            mainDB,
            True,
            ['Summarize data for marine layers 1, 2, 3, 4 and 6',
             'Summarize data for marine layer 1',
             'Summarize data for marine layer 2',
             'Summarize data for marine layer 3',
             'Summarize data for marine layer 4',
             'Summarize data for marine layer 6']
        )
        # Also added a filler pane to purely improve look and feel only
        summarizePane.add(ttk.Panedwindow(summarizePane, orient=VERTICAL))

        # fourth tab
        aggregatePane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        AggregateCommandPane(
            aggregatePane,
            mainDB,
            True,
            ['Aggregrate data for marine layers 1, 2, 3, 4 and 6',
             'Aggregrate data for marine layer 1',
             'Aggregrate data for marine layer 2',
             'Aggregrate data for marine layer 3',
             'Aggregrate data for marine layer 4',
             'Aggregrate data for marine layer 6']
        )
        aggregatePane.add(ttk.Panedwindow(aggregatePane, orient=VERTICAL))

        # fifth tab
        cellCatchPane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        CellCatchCommandPane(
            cellCatchPane,
            mainDB
        )
        cellCatchPane.add(ttk.Panedwindow(cellCatchPane, orient=VERTICAL))

        # sixth tab
        cacheDataPane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        CacheDataCommandPane(
            cacheDataPane,
            mainDB
        )
        cacheDataPane.add(ttk.Panedwindow(cacheDataPane, orient=VERTICAL))

        # seventh tab
        taxonExtentPane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        TaxonExtentCommandPane(
            taxonExtentPane,
            mainDB
        )
        taxonExtentPane.add(ttk.Panedwindow(taxonExtentPane, orient=VERTICAL))

        # eighth tab
        sqlProcessorGuiPane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        SqlProcessorGuiCommandPane(
            sqlProcessorGuiPane,
            mainDB,
            True
        )
        sqlProcessorGuiPane.add(ttk.Panedwindow(sqlProcessorGuiPane, orient=VERTICAL))

        mainNB.add(dbPane, text='DB Connection')
        mainNB.add(rdsPane, text='RDS')
        mainNB.add(pullDataPane, text='Pull Data')
        mainNB.add(summarizePane, text='Summarize')
        mainNB.add(aggregatePane, text='Aggregate')
        mainNB.add(cellCatchPane, text='Cell Catch')
        mainNB.add(cacheDataPane, text='Cache Data')
        mainNB.add(taxonExtentPane, text='Taxon Extent')
        mainNB.add(sqlProcessorGuiPane, text='SQL Processor')

        mainNB.pack(expand=1, fill='both')

# ===============================================================================================
# ----- MAIN
def main():
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
