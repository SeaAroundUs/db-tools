import os
import copy

from tkinter_util import *
from db_util import *

from functools import partial
import subprocess

import sqlprocessor as sp
from db import getDbConnection

from models import DataTransfer, AllocationResultPartitionMap

NUM_OF_ALLOCATION_RESULT_PARTITIONS = 150


class PullAllocationDataCommandPane(tk.Frame):
    def __init__(self, parent, mainDbPane, sourceDbPane, **kargs):
        tk.Frame.__init__(self, parent)

        self.parent = parent
        self.mainDbPane = mainDbPane
        self.sourceDbPane = sourceDbPane

        # Placeholders, these will really be initialized lazily latter, once all db parameters are available.
        self.dbSession = None
        self.dataTransfer = None

        scb = tk.Button(parent, text="Get list of allocation tables to pull data down (SQLServer)", fg="red", height=1,
                        command=self.setupCommandPane)
        self.parent.add(scb)

        self.cmdFrame = add_label_frame(parent, "Allocation Tables To Pull", 100, 120)
        parent.add(self.cmdFrame)

        rmv = tk.Button(parent, text="Refresh all Main DB materialized views", fg="red",
                        command=partial(refresh_all_materialized_views, self.mainDbPane))
        parent.add(rmv)

        self.set_defaults()

    def set_defaults(self):
        self.mainDbPane.db_type.set("postgres")
        self.mainDbPane.db_server.set("sau-db-1.ck24jacu2hmg.us-west-2.rds.amazonaws.com")
        self.mainDbPane.db_name.set("sau_int")
        self.mainDbPane.db_username.set("sau_int")
        self.sourceDbPane.db_type.set("sqlserver")
        self.sourceDbPane.db_server.set("merlin.seaaroundus.org")
        self.sourceDbPane.db_name.set("merlin")
        self.sourceDbPane.db_username.set("sau_merlin")

    def setupCommandPane(self):
        if not self.mainDbPane.isConnectionTestedSuccessfully():
            popup_message("Connection not yet tested",
                          "The Main DB Connection has not been tested successfully.\n" +
                          "Once the Main DB Connection has been tested successfully, you can click that button again.")
            return

        if self.sourceDbPane.db_type.get() != 'sqlserver':
            popup_message("DB Connection not SQL Server",
                          "The Source DB Connection should be a SQL Server instance.\n" +
                          "Once the Source DB Connection has been re-configured, you can click that button again.")
            return

        for child in self.cmdFrame.winfo_children(): child.destroy()

        dbConn = getDbConnection(optparse.Values(self.mainDbPane.getDbOptions()))
        self.dbSession = dbConn.getSession()
        self.dataTransfer = self.dbSession.query(DataTransfer).filter_by(target_schema_name='allocation').order_by(DataTransfer.id).all()

        add_buttons(self.cmdFrame,
                    [[tab.target_table_name, partial(self.processTable, tab), "blue"] for tab in self.dataTransfer],
                    0, 0, "horizontal")

        add_buttons(self.cmdFrame,
                    [["Pull all allocation db tables", self.pullAllAllocationData, "red"],
                    ["Drop foreign keys", partial(drop_foreign_key, self.mainDbPane), "red"],
                    ["Restore foreign keys", partial(restore_foreign_key, self.mainDbPane), "red"]],
                    1, 0, "horizontal")

        grid_panel(self.cmdFrame)

    def processTable(self, tabDescriptor):
        opts = self.sourceDbPane.getDbOptions()

        if tabDescriptor.target_table_name != "allocation_result":
            self.downloadAndCopyTable(tabDescriptor, opts)
        else:
            #
            # Special processing for allocation_result due to its excessive size.
            #

            # Clone the table descriptor for the input table to create one for the allocation_result_distribution table
            tableDesc = copy.deepcopy(tabDescriptor)
            tableDesc.target_table_name = "allocation_result_distribution"
            tableDesc.source_select_clause = "UniversalDataID, count(*)"
            tableDesc.source_where_clause = "GROUP BY UniversalDataID"
            self.dbSession.execute("TRUNCATE TABLE allocation.%s" % tableDesc.target_table_name)
            self.downloadAndCopyTable(tableDesc, opts)
            self.dbSession.execute("VACUUM ANALYZE allocation.%s" % tableDesc.target_table_name)

            # Now managed the allocation result partition map table to receive download data
            partitionMapCmds = [
                "TRUNCATE TABLE allocation.allocation_result_partition_map",
                "select allocation.maintain_allocation_result_partition()",
                "select allocation.calculate_allocation_result_partition_map(%s)" % NUM_OF_ALLOCATION_RESULT_PARTITIONS,
                "VACUUM ANALYZE allocation.allocation_result_partition_map",
                "select allocation.maintain_allocation_result_partition()"
            ]
            for cmd in partitionMapCmds:
                self.dbSession.execute(cmd)

            # Loop over each partition to load data from source
            arPartitionMaps = self.dbSession.query(AllocationResultPartitionMap).order_by(AllocationResultPartitionMap.partition_id).all()
            tableDesc.target_schema_name = "allocation_partition"
            tableDesc.source_select_clause = "*"
            for partitionMap in arPartitionMaps:
                tableDesc.target_table_name = "allocation_result_%s" % partitionMap.partition_id
                tableDesc.source_where_clause = "WHERE AllocatedCatch > 0 AND UniversalDataID BETWEEN %s AND %s" \
                                                % (partitionMap.begin_universal_data_id, partitionMap.end_universal_data_id)
                self.downloadAndCopyTable(tableDesc, opts)

            mainDbOpts = self.mainDbPane.getDbOptions()
            mainDbOpts['sqlfile'] = None
            mainDbOpts['threads'] = 8

            # Let's now vacuum and analyze all the partitions we just populated above
            mainDbOpts['sqlcmd'] = "SELECT format('VACUUM ANALYZE allocation_partition.%s', table_name) " +\
                                   "  FROM schema_v('allocation_partition') " + \
                                   " WHERE table_name NOT LIKE 'TOTALS%'"
            sp.process(optparse.Values(mainDbOpts))

            # And now add necessary indexes to all the partitions we just created above
            mainDbOpts['sqlcmd'] = "SELECT allocation.maintain_allocation_result_indexes(table_name)" + \
                                   "  FROM schema_v('allocation_partition')" + \
                                   " WHERE table_name NOT LIKE 'TOTALS%'"
            sp.process(optparse.Values(mainDbOpts))

    def downloadAndCopyTable(self, tabDescriptor, sourceDbOpts):
        targetTable = "{0}.{1}".format(tabDescriptor.target_schema_name, tabDescriptor.target_table_name)

        if tabDescriptor.source_where_clause:
            tabQuery = ("select {0} from dbo.{1} {2}").format(tabDescriptor.source_select_clause,
                                                              tabDescriptor.source_table_name,
                                                              tabDescriptor.source_where_clause)
            dataMode = "queryout"
        else:
            tabQuery = "dbo.[%s]" % tabDescriptor.source_table_name
            dataMode = "out"

        outputDataFile = "data/" + targetTable

        if self.downloadSourceTable(sourceDbOpts, tabQuery, dataMode, outputDataFile):
            self.copyToTargetTable(tabDescriptor, targetTable, outputDataFile)

    def downloadSourceTable(self, dbOpts, tabQuery, dataMode, outputDataFile):
        subprocess.check_call(["bcp", tabQuery, dataMode, outputDataFile, "-c", "-k", "-a 65535",
                               "-S", dbOpts["server"],
                               "-d", dbOpts["dbname"],
                               "-U", dbOpts["username"],
                               "-P", dbOpts["password"]])

        if not (os.path.isfile(outputDataFile) and os.access(outputDataFile, os.R_OK)):
            popup_message("Data file download unsuccessful",
                          "Attempt to download data for the table/query '{0}' failed.".format(tabQuery) +
                          "Please check that all parameters for Source DB Connection has been tested entered correctly and try again!")
            return False

        return True

    def copyToTargetTable(self, tabDescriptor, targetTable, inputDataFile):
        if tabDescriptor.target_excluded_columns:
            columnList = self.dbSession.execute(
                "SELECT g.col FROM get_table_column('{0}', ARRAY{1}) AS g(col)".format(targetTable,
                                                                                       tabDescriptor.target_excluded_columns))
            columnList = "(" + columnList.first().col + ")"
        else:
            columnList = ""

        self.dbSession.execute("TRUNCATE TABLE %s" % targetTable)

        rawConn = self.dbSession.connection().connection
        cursor = rawConn.cursor()
        cursor.copy_expert(sql="copy {0}{1} from STDIN".format(targetTable, columnList),
                           file=open(inputDataFile))
        rawConn.commit()
        cursor.close()

        self.dbSession.execute("VACUUM ANALYZE %s" % targetTable)

    def pullAllAllocationData(self):
        for tab in self.dataTransfer:
            self.processTable(tab)

        refresh_all_materialized_views(self.mainDbPane)
        print('All allocation tables successfully pulled down.')


# ===============================================================================================
# ----- MAIN
if __name__ == "__main__":
    Application("Pull Allocation Data", PullAllocationDataCommandPane, include_source_db=True).run()