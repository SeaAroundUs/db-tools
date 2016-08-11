import os
import os.path as path
import optparse
import traceback
import multiprocessing
from datetime import datetime
from db import getDbConnection
from db import DBConnectionPane
from psycopg2 import IntegrityError
import tkinter as tk
from tkinter import ttk
from tkinter import *
from osgeo import ogr


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
        self.taxonKeyToSimplify = IntVar()

        self.extentDir.set("taxon_extent")

        self.command_row = 0
        self.add_command(cmdFrame, "Taxon Extent Shape Directory", self.extentDir, "Check Taxon Name Against Key", self.checkExtentDir)

        self.addSeparator(cmdFrame)
        self.add_command(cmdFrame, "Taxon Extent Shape Directory", self.extentDir, "Load Into DB", self.processExtentDir)

        self.addSeparator(cmdFrame)
        lsb = Spinbox(cmdFrame, textvariable=self.taxonLevelToRollupFor, width=59, values=(5, 4, 3, 2, 1), state=NORMAL)
        self.add_command(cmdFrame, "Target Taxon Level", lsb, "Rollup Lower Level Extents", self.rollupExtent)

        self.addSeparator(cmdFrame)
        self.add_command(cmdFrame, "Taxon Key", self.taxonKeyToSimplify, "Simplify Taxon Extent", self.simplifyExtent)

        for child in cmdFrame.winfo_children(): child.grid_configure(padx=5, pady=5)

        parent.add(cmdFrame)

    def add_command(self, panel, label_text, entry_var, cmd_text, cmd):
        self.command_row += 1
        tk.Label(panel, text=label_text).grid(column=0, row=self.command_row, sticky=W)
        self.command_row += 1

        # if entry_var is a widget already, we just need to grid it. otherwise, we create an input Entry to wrap it.
        if entry_var.__class__.__base__.__name__ == "Widget":
            entry_var.grid(column=0, row=self.command_row, sticky=W)
        else:
            tk.Entry(panel, width=60, textvariable=entry_var).grid(column=0, row=self.command_row, sticky=W)

        tk.Button(panel, text=cmd_text, command=cmd).grid(column=1, row=self.command_row, sticky=W)

    def addSeparator(self, panel):
        self.command_row += 1
        ttk.Separator(panel).grid(row=self.command_row, columnspan=2, sticky="ew")

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

        #print("Processing taxonKey: %s(%s)" % (taxonKey, isExtendedTaxon))
        print("Processing taxonKey: %s" % taxonKey)

        dbCursor.execute("DELETE FROM distribution.taxon_extent" + \
                         " WHERE taxon_key = %s AND is_extended = %s" % (taxonKey, isExtendedTaxon))

        shapefile = ogr.Open(shpFilePath)
        layer = shapefile.GetLayer(0)
        feature_count = layer.GetFeatureCount()
        if feature_count > 1:
            print("Input extent for %s has %s features. Only one feature is expected." % (taxonKey, feature_count))
        else:
            for feature in layer:
                geom = feature.GetGeometryRef()
                if geom:
                    if geom.GetArea() > 0:
                        try:
                            dbCursor.execute("INSERT INTO distribution.taxon_extent(taxon_key, is_extended, geom)" +
                                            "VALUES (%(tkey)s, %(extended)s, ST_MULTI(ST_SetSRID(%(geom)s::geometry, 4326)))",
                                            {"tkey": taxonKey, "extended": isExtendedTaxon, "geom": geom.ExportToWkt()})
                        except IntegrityError:
                            print("Taxon key %s not found in master.taxon" % taxonKey)
                            print("Skipping this taxon for now")
                else:
                    print("Unable to get geom for feature %s. Was skipped." % feature)

    def checkExtentDir(self):
        taxonExtentDir = self.extentDir.get()
        if os.path.isdir(taxonExtentDir):
            dbConn = getDbConnection(optparse.Values(self.dbPane.getDbOptions()))

            try:
                for subdir in [x[0] for x in os.walk(taxonExtentDir)]:
                    if subdir != taxonExtentDir:
                        for shpFile in [f for f in os.listdir(subdir) if f.endswith('.shp')]:
                            match = self.shapeFilePattern.match(shpFile)
                            if match:
                                taxonKey = int(match.groups()[0])
                                taxons = dbConn.execute("SELECT scientific_name AS taxon_name " +
                                                         "  FROM master.taxon WHERE taxon_key = %s"
                                                         % taxonKey)
                                for taxon in taxons:
                                    foundInDirTaxonName = path.basename(subdir)
                                    if taxon.taxon_name != foundInDirTaxonName.replace("_", " "):
                                        print("Taxon %s's name %s does not match with subdir %s"
                                              % (taxonKey, taxon.taxon_name, foundInDirTaxonName))
            finally:
                if dbConn:
                    dbConn.close()

    def refreshMaterializedViews(self, dbConn):
        print("Refreshing materialized views this program depends on.")
        dbConn.execute("REFRESH MATERIALIZED VIEW distribution.v_taxon_with_extent")
        dbConn.execute("REFRESH MATERIALIZED VIEW distribution.v_taxon_with_distribution")

    def rollupExtent(self):
        if not self.taxonLevelToRollupFor or self.taxonLevelToRollupFor == 0:
            print("Taxon level should be between 1 and 5. Please try again.")
            return

        print("Processing input taxon level: %s" % (self.taxonLevelToRollupFor.get()))

        dbConn = getDbConnection(optparse.Values(self.dbPane.getDbOptions()))

        try:
            # First off refresh the materialized views that we rely on to indicate which taxon has/has not an extent already
            self.refreshMaterializedViews(dbConn)

            # Get the list of target taxon keys to rollup for the input taxon level
            rollups = dbConn.execute("SELECT * FROM distribution.get_rollup_taxon_list(%s::int) ORDER BY children_distributions_found" \
                                     % int(self.taxonLevelToRollupFor.get()))

            curTaxonKey = None
            for rollup in rollups:
                curTaxonKey = rollup.taxon_key
                childrenTaxa = rollup.children_taxon_keys
                print("Rollup for %s using lower-level taxons %s [%s]" % (curTaxonKey, childrenTaxa, datetime.now().strftime('%Y/%m/%d %H:%M:%S')))

                try:
                    if len(childrenTaxa) == 1:
                        dbConn.execute(
                            ("INSERT INTO distribution.taxon_extent(taxon_key, is_extended, is_rolled_up, geom) " +
                            "SELECT %s, is_extended, TRUE, geom FROM distribution.taxon_extent WHERE taxon_key = %s")
                            % (curTaxonKey, childrenTaxa[0]))
                    else:
                        last_seq = dbConn.execute("SELECT f.seq FROM distribution.extent_rollup_dumpout_polygons(%s, ARRAY%s::int[]) AS f(seq)"
                                                  % (curTaxonKey, childrenTaxa)).fetchone()[0]
                        while last_seq and last_seq > 1:
                            last_seq = dbConn.execute(
                                "SELECT f.seq FROM distribution.extent_rollup_purge_contained_polygons(%s, %s) AS f(seq)"
                                % (curTaxonKey, last_seq)).fetchone()[0]

                        dbConn.execute(
                            ("INSERT INTO distribution.taxon_extent(taxon_key, is_rolled_up, geom) " +
                            "SELECT taxon_key, TRUE, st_multi(st_collectionextract(st_union(st_buffer(st_simplifypreservetopology(geom, 0.01), 0.25)), 3)) " +
                            "  FROM distribution.taxon_extent_rollup_polygon WHERE taxon_key = %s " +
                            " GROUP BY taxon_key")
                            % curTaxonKey)

                        dbConn.execute("DELETE FROM distribution.taxon_extent_rollup_polygon WHERE taxon_key = %s" % curTaxonKey)

                    dbConn.execute(
                        ("INSERT INTO distribution.taxon_extent_rollup(taxon_key, children_distributions_found, children_taxon_keys) " +
                        "VALUES (%s, %s, ARRAY%s::int[])")
                        % (curTaxonKey, rollup.children_distributions_found, childrenTaxa))
                except Exception:
                    print("Exception encountered during the processing of taxon: %s" % curTaxonKey)
                    print(sys.exc_info())
                    if curTaxonKey:
                        dbConn.execute("DELETE FROM distribution.taxon_extent WHERE taxon_key = %s" % curTaxonKey)
                finally:
                    self.refreshMaterializedViews(dbConn)
        finally:
            dbConn.close()

        print("All taxon extent rollup operations for input taxon level completed.")


    def simplifyExtent(self):
        taxonKey = int(self.taxonKeyToSimplify)

        print("Simplifying taxonKey: %s(%s)" % taxonKey)

        dbConn = getDbConnection(optparse.Values(self.dbPane.getDbOptions()))

        dbConn.execute(
            "WITH ext(taxon_key, geom) AS (" +
            "  SELECT e.taxon_key, (st_dump(geom)).geom" +
            "    FROM distribution.taxon_extent e" +
            "   WHERE e.taxon_key = %(tk)s" +
            ")" +
            "UPDATE distribution.taxon_extent e" +
            "   SET geom = (SELECT ST_Union(ST_Buffer(ST_SimplifyPreserveTopology(geom, 0.01), 0.25))" +
            "                 FROM ext" +
            "                GROUP BY ext.taxon_key)" +
            " WHERE e.taxon_key = %(tk)s"
            % {"tk": taxonKey}
        )


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
