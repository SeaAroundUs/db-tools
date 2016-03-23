import os
import os.path as path
import optparse
import traceback
import multiprocessing
from db import getDbConnection
from db import DBConnectionPane
import tkinter as tk
from tkinter import ttk
from tkinter import *
import osgeo.ogr as ogr

class TaxonExtentCommandPane(tk.Frame):
    def __init__(self, parent, dbPane):
        tk.Frame.__init__(self, parent)
        self.dbPane = dbPane
        self.dbConn = None

        cmdFrame = ttk.Labelframe(parent, text='Process Taxon Extent', width=100, height=100)
        cmdFrame.grid(column=0, row=0, sticky=(N, W))
        cmdFrame.columnconfigure(0, weight=1)
        cmdFrame.rowconfigure(0, weight=1)

        self.shapeFilePattern = re.compile('(\d+)\-?(\d+)?.shp')
        self.extentDir = StringVar()
        self.taxonLevelToRollupFor = IntVar()

        self.extentDir.set("D:/SeaAroundUs/integration-database.old/geo/Taxon_Extent_Edited")

        self.command_row = 0
        self.add_command(cmdFrame, "Taxon Extent Shape Directory", self.extentDir, "Load Into DB", self.processExtentDir)
        self.command_row += 1
        ttk.Separator(cmdFrame).grid(row=self.command_row, columnspan=2, sticky="ew")
        self.add_command(cmdFrame, "Target Taxon Level", self.taxonLevelToRollupFor, "Rollup Lower Level Extents", self.rollupExtent)

        for child in cmdFrame.winfo_children(): child.grid_configure(padx=5, pady=5)

        parent.add(cmdFrame)

    def add_command(self, panel, label_text, entry_var, cmd_text, cmd):
        self.command_row += 1
        tk.Label(panel, text=label_text).grid(column=0, row=self.command_row, sticky=W)
        self.command_row += 1
        tk.Entry(panel, width=60, textvariable=entry_var).grid(column=0, row=self.command_row, sticky=W)
        tk.Button(panel, text=cmd_text, command=cmd).grid(column=1, row=self.command_row, sticky=W)

    def processExtentDir(self):
        taxonExtentDir = self.extentDir.get()
        if os.path.isdir(taxonExtentDir):
            opts = self.dbPane.getDbOptions()
            rawConn = getDbConnection(optparse.Values(opts)).getSession().connection().connection
            cursor = rawConn.cursor()

            try:
                for subdir in [x[0] for x in os.walk(taxonExtentDir)]:
                    if subdir != taxonExtentDir:
                        for shpFile in [f for f in os.listdir(subdir) if f.endswith('.shp')]:
                            match = self.shapeFilePattern.match(shpFile)
                            if match:
                                self.processExtentShapeFile(cursor, path.join(subdir, shpFile), match.groups())
                                rawConn.commit()


                print("Vacuuming distribution.taxon_extent afterward...")
                cursor.execute("vacuum analyze distribution.taxon_extent")
                rawConn.commit()
                cursor.close()
            finally:
                if rawConn:
                    rawConn.close()


    def processExtentShapeFile(self, dbCursor, shpFilePath, taxonKeyGroup):
        taxonKey = int(taxonKeyGroup[0])

        if taxonKeyGroup[1] == None:
            isExtendedTaxon = False
        else:
            isExtendedTaxon = True

        print("Processing taxonKey: %s(%s)" % (taxonKey, isExtendedTaxon))

        dbCursor.execute("DELETE FROM distribution.taxon_extent" + \
                         " WHERE taxon_key = %s AND is_extended = %s" % (taxonKey, isExtendedTaxon))

        shapefile = ogr.Open(shpFilePath)
        layer = shapefile.GetLayer(0)
        for feature in layer:
            geom = feature.GetGeometryRef()
            if geom:
                dbCursor.execute("INSERT INTO distribution.taxon_extent(taxon_key, is_extended, geom)" +
                                 "VALUES (%(tkey)s, %(extended)s, ST_MULTI(ST_SetSRID(%(geom)s::geometry, 4326)))",
                                 {"tkey": taxonKey, "extended": isExtendedTaxon, "geom": geom.ExportToWkt()})
            else:
                print("Unable to get geom for feature %s. Was skipped." % feature)

    def rollupExtent(self):
        if not self.taxonLevelToRollupFor or self.taxonLevelToRollupFor == 0:
            print("Taxon level should be between 1 and 5. Please try again.")
            return

        print("Processing input taxon level: %s" % (self.taxonLevelToRollupFor.get()))

        dbConn = getDbConnection(optparse.Values(self.dbPane.getDbOptions()))

        try:
            # First off refresh the materialized views that we rely on to indicate which taxon has/has not an extent already
            dbConn.execute("REFRESH MATERIALIZED VIEW log.v_taxon_with_extent")
            dbConn.execute("REFRESH MATERIALIZED VIEW log.v_taxon_with_distribution")

            # Get the list of target taxon keys to rollup for the input taxon level
            rollups = dbConn.execute("SELECT * FROM distribution.get_rollup_taxon_list(%s::int) ORDER BY children_distributions_found" \
                                     % int(self.taxonLevelToRollupFor.get()))

            try:
                curTaxonKey = None
                for rollup in rollups:
                    curTaxonKey = rollup.taxon_key
                    childrenTaxa = rollup.children_taxon_keys
                    if len(childrenTaxa) == 1:
                        dbConn.execute(
                            ("INSERT INTO distribution.taxon_extent(taxon_key, is_extended, is_rolled_up, geom) " +
                             "SELECT %s, is_extended, TRUE, geom FROM distribution.taxon_extent WHERE taxon_key = %s") %
                            (curTaxonKey, childrenTaxa[0]))
                    else:
                        dbConn.execute(
                            ("INSERT INTO distribution.taxon_extent(taxon_key, is_extended, is_rolled_up, geom) " +
                             "SELECT %s, (array_agg(DISTINCT is_extended ORDER BY is_extended DESC))[1], TRUE, st_multi(st_union(geom)) " +
                             "  FROM distribution.taxon_extent WHERE taxon_key = any(ARRAY%s::int[]) " +
                             " GROUP BY TRUE") %
                            (curTaxonKey, childrenTaxa))

                    dbConn.execute(
                        ("INSERT INTO distribution.taxon_extent_rollup(taxon_key, children_distributions_found, children_taxon_keys) " +
                         "VALUES (%s, %s, ARRAY%s::int[])") %
                         (curTaxonKey, rollup.children_distributions_found, childrenTaxa))
            except Exception:
                print("Exception encountered during the processing of taxon: %s" % curTaxonKey)
                if curTaxonKey:
                    dbConn.execute("DELETE FROM distribution.taxon_extent WHERE taxon_key = %s" % curTaxonKey)

            # We only need to refresh the extent materialized view as in this module that's the only data we would write to
            dbConn.execute("REFRESH MATERIALIZED VIEW log.v_taxon_with_extent")
        finally:
            dbConn.close()

class Application(tk.Frame):
    def __init__(self, master=None):
        tk.Frame.__init__(self, master)

        mainPane = ttk.Panedwindow(master, orient=VERTICAL)
        dbPane = DBConnectionPane(parent=mainPane, title="DB Connection", include_threads=False, include_sqlfile=False)
        TaxonExtentCommandPane(mainPane, dbPane)
        mainPane.pack(expand=1, fill='both')


# ===============================================================================================
# ----- MAIN
def main():
    root = tk.Tk()
    root.title("Taxon Extent")
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
