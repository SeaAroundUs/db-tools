import os

from tkinter_util import *
from db_util import *

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

        scb = tk.Button(parent, text="Get list of integration tables to pull data down", fg="red", height=1,
                        command=self.setupCommandPane)
        parent.add(scb)                                                      

        self.cmdFrame = add_label_frame(parent, "Integration DB Tables To Pull", 100, 280)
        parent.add(self.cmdFrame)

        if not suppressMaterializedViewRefreshButton:
            rmv = tk.Button(parent, text="Refresh all Main DB materialized views",
                            fg="red", command=partial(refresh_all_materialized_views, self.mainDbPane))  
            parent.add(rmv)     
                                                          
    def setupCommandPane(self):
        if not self.mainDbPane.isConnectionTestedSuccessfully():
            popup_message("Connection not yet tested",
                          "The Main DB Connection has not been tested successfully.\n" +
                          "Once the Main DB Connection has been tested successfully, you can click that button again.")
            return

        for child in self.cmdFrame.winfo_children(): child.destroy()

        sourceDbOpts = optparse.Values(self.sourceDbPane.getDbOptions())
        self.sourceDbSession = getDbConnection(sourceDbOpts).getSession()
        self.mainDbSession = getDbConnection(optparse.Values(self.mainDbPane.getDbOptions())).getSession()

        self.dataTransfer = self.mainDbSession.query(DataTransfer).filter(func.lower(DataTransfer.source_database_name)==func.lower(sourceDbOpts.dbname)) \
            .order_by(DataTransfer.id).all()

        button_data = []        
        column=0                       
        row=0
                  
        for tab in self.dataTransfer:
             button_data.append([tab.target_table_name, partial(self.processTable, tab), "blue"])
             column += 1 
        
             if column >= self.buttonsPerRow:
                 add_buttons(self.cmdFrame, button_data, row, 0, "horizontal")
                 button_data = []            
                 column = 0                    
                 row += 1
                                            
        if button_data != []:
            add_buttons(self.cmdFrame, button_data, row, 0, "horizontal")
            row += 1

        add_buttons(self.cmdFrame,
                    [["Pull all integration db tables", self.pullAllIntegrationDbData, "red"],
                    ["Drop foreign keys", partial(drop_foreign_key, self.mainDbPane), "red"],
                    ["Restore foreign keys", partial(restore_foreign_key, self.mainDbPane), "red"]],
                    row, 0, "horizontal")
                         
        grid_panel(self.cmdFrame)                                    
                                                          
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
            popup_message("Data file download unsuccessful",
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

        refresh_all_materialized_views(self.mainDbPane)

        print('All integration db tables successfully pulled down.')


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
if __name__ == "__main__":
    tkinter_client_main(Application, "Pull Integration DB Data")