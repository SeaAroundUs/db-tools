from functools import partial

from tkinter_util import *
from db_util import *
from pull_integration_data import PullIntegrationDataCommandPane

class AllocationCommandPane(tk.Frame):
    def __init__(self, parent, mainDbPane, sourceDbPane, **kargs):
        tk.Frame.__init__(self, parent)

        self.parent = parent
        self.mainDbPane = mainDbPane
        self.sourceDbPane = sourceDbPane
        self.pullDataPane = PullIntegrationDataCommandPane(self.parent, self.mainDbPane, self.sourceDbPane, silentMode = True)

# ===============================================================================================
# ----- MAIN
if __name__ == "__main__":
    Application("Allocation (a.k.a. merlin)", AllocationCommandPane, include_source_db=True).run()
