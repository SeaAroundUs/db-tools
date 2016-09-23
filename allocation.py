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

        processFrame = add_label_frame(parent, "Process", 400, 50)

        row = add_buttons(processFrame,
                          [["Pull Data from Integration DB & Initialize Merlin Output Tables", self.initialize_merlin, "red"],
                           ["Start Allocation", self.start_allocation, "red"]],
                          0, 0, "vertical")

        grid_panel(processFrame)

        parent.add(processFrame)

    def initialize_merlin(self):
        mainDbConn = getDbConnection(optparse.Values(self.mainDbPane.getDbOptions()))

        print("Truncating ao.AllocationResult, ao.Log_Import_Raw, ao.Data...")
        mainDbConn.execute("TRUNCATE ao.AllocationResult, ao.Log_Import_Raw, ao.Data")

        print("Pulling data from Integration DB")
        drop_foreign_key(self.mainDbPane)
        self.pullDataPane.pullAllIntegrationDbData()
        restore_foreign_key(self.mainDbPane)
        refresh_all_materialized_views(self.mainDbPane)

        print("Transfering layer3 data...")
        mainDbConn.execute("SELECT ai.layer3_transfer_to_dataraw()")
        mainDbConn.execute("SELECT ai.layer3_update_taxon_substitutions()")

        print("Truncating and initializing ao tables...")
        mainDbConn.execute("TRUNCATE TABLE ao.AllocationSimpleArea")
        mainDbConn.execute("TRUNCATE TABLE ao.AllocationHybridArea")
        mainDbConn.execute("TRUNCATE TABLE ao.AutoGen_HybridToSimpleAreaMapper")
        mainDbConn.execute("TRUNCATE TABLE ao.SimpleAreaCellAssignment")
        mainDbConn.execute("SELECT SETVAL('ao.allocationsimplearea_allocationsimpleareaid_seq', 1)");
        mainDbConn.execute("Insert into ao.AllocationSimpleArea " +
                           "SELECT nextval('ao.allocationsimplearea_allocationsimpleareaid_seq'), * FROM a_internal.generate_allocation_simple_area_table()")
        mainDbConn.execute("SELECT SETVAL('ao.simpleareacellassignment_rowid_seq', 1)");
        mainDbConn.execute("SELECT a_internal.SimpleAreaCellAssignment_Populate()")

        print("Initialize Merlin process completed successfully.")

    def start_allocation(self):
        print("Allocation in progress...")

        # Hook in the new merlin.exe at this point in the process

        self.pullAllocationLogPane = PullIntegrationDataCommandPane(self.parent, self.sourceDbPane, self.mainDbPane, silentMode = True, suppressTableListButton = False)

        print("Allocation completed sucessfully.")


# ===============================================================================================
# ----- MAIN
if __name__ == "__main__":
    Application("Allocation (a.k.a. merlin)", AllocationCommandPane, include_source_db=True).run()
