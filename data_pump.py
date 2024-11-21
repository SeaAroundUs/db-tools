from tkinter_util import *

from rds import RdsCommandPane
from pull_integration_data import PullIntegrationDataCommandPane
from pull_allocation_data import PullAllocationDataCommandPane
from summarize import SummarizeCommandPane
from aggregate import AggregateCommandPane
from cell_catch import CellCatchCommandPane
from cache_data import CacheDataCommandPane
from taxon_extent import TaxonExtentCommandPane
from distribution import DistributionCommandPane
from allocation import AllocationCommandPane
from sqlprocessor_gui import SqlProcessorGuiCommandPane
from stock_status import StockStatusCommandPane


class DataPumpCommandPane(tk.Frame):
    def __init__(self, master):
        tk.Frame.__init__(self, master)

        mainNB = ttk.Notebook(master, width=820, height=520)

        dbPane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        mainDB = DBConnectionPane(dbPane, 'Main DB')
        sourceDB = DBConnectionPane(dbPane, 'Source DB')
        dbPane.add(ttk.Panedwindow(dbPane, orient=VERTICAL))    # Also added a filler pane to improve look and feel only

        # rds tab
        rdsPane = add_pane(mainNB, mainDB, RdsCommandPane, add_filler_pane=True)

        # taxon_extent tab
        taxonExtentPane = add_pane(mainNB, mainDB, TaxonExtentCommandPane, add_filler_pane=True)

        # distribution tab
        distributionPane = add_pane(mainNB, mainDB, DistributionCommandPane, add_filler_pane=True)

        # allocation tab
        allocationPane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        AllocationCommandPane(allocationPane, mainDB, sourceDB)
        allocationPane.add(ttk.Panedwindow(allocationPane, orient=VERTICAL))

        # pull tab
        pullDataPane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        PullIntegrationDataCommandPane(pullDataPane, mainDB, sourceDB, suppressMaterializedViewRefreshButton=TRUE)
        PullAllocationDataCommandPane(pullDataPane, mainDB, sourceDB)
        pullDataPane.add(ttk.Panedwindow(pullDataPane, orient=VERTICAL))

        # summarize tab
        summarizePane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        SummarizeCommandPane(
            summarizePane,
            mainDB,
            True,
            ['Summarize data for marine layers 1, 2, 3, 19, 4. 14 and 6',
             'Summarize data for marine layer 1',
             'Summarize data for marine layer 2',
             'Summarize data for marine layer 3',
             'Summarize data for marine layer 19',
             'Summarize data for marine layer 4',
             'Summarize data for marine layer 14',
             'Summarize data for marine layer 6']
        )
        # Also added a filler pane to purely improve look and feel only
        summarizePane.add(ttk.Panedwindow(summarizePane, orient=VERTICAL))

        # aggregate tab
        aggregatePane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        AggregateCommandPane(
            aggregatePane,
            mainDB,
            True,
            ['Aggregrate data for marine layers 1, 2, 3, 19, 4, 14 and 6',
             'Aggregrate data for marine layer 1',
             'Aggregrate data for marine layer 2',
             'Aggregrate data for marine layer 3',
             'Aggregrate data for marine layer 19',
             'Aggregrate data for marine layer 4',
             'Aggregrate data for marine layer 14',
             'Aggregrate data for marine layer 6']
        )
        aggregatePane.add(ttk.Panedwindow(aggregatePane, orient=VERTICAL))

        # cell_catch tab
        cellCatchPane = add_pane(mainNB, mainDB, CellCatchCommandPane, add_filler_pane=True)

        # cache_data tab
        cacheDataPane = add_pane(mainNB, mainDB, CacheDataCommandPane, add_filler_pane=True)

        # sqlprocessor tab
        sqlProcessorGuiPane = ttk.Panedwindow(mainNB, orient=VERTICAL)
        SqlProcessorGuiCommandPane(
            sqlProcessorGuiPane,
            mainDB,
            True
        )
        sqlProcessorGuiPane.add(ttk.Panedwindow(sqlProcessorGuiPane, orient=VERTICAL))

        # stock_status tab
        stockStatusPane = add_pane(mainNB, mainDB, StockStatusCommandPane, add_filler_pane=True)

        mainNB.add(dbPane, text='DB Connection')
        mainNB.add(rdsPane, text='RDS')
        mainNB.add(taxonExtentPane, text='Taxon Extent')
        mainNB.add(distributionPane, text='Distribution')
        mainNB.add(allocationPane, text='Allocation')
        mainNB.add(pullDataPane, text='Pull Data')
        mainNB.add(summarizePane, text='Summarize')
        mainNB.add(aggregatePane, text='Aggregate')
        mainNB.add(cellCatchPane, text='Cell Catch')
        mainNB.add(cacheDataPane, text='Cache Data')
        mainNB.add(sqlProcessorGuiPane, text='SQL Processor')
        mainNB.add(stockStatusPane, text='Stock Status')

        mainNB.pack(expand=1, fill='both')

# ===============================================================================================
# ----- MAIN
if __name__ == "__main__":
    app = Application("SAU Data Pump")
    app.run(DataPumpCommandPane(app.root))
