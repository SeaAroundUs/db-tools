import os
import os.path as path
import optparse
import subprocess
import glob

from datetime import datetime
from db import getDbConnection
from psycopg2 import IntegrityError
from osgeo import ogr

from tkinter_util import *


class TaxonExtentCommandPane(tk.Frame):
    def __init__(self, parent, dbPane):
        tk.Frame.__init__(self, parent)
        self.dbPane = dbPane
        self.dbConn = None

        self.shapeFilePattern = re.compile('(\d+)\-?(\d+)?.shp')
        self.extentDir = StringVar()
        self.taxonLevelToRollupFor = IntVar()
        self.taxonKeyToSimplify = IntVar()
        self.taxonKeyToExtract = IntVar()
        self.extentExtractDir = StringVar()

        self.extentDir.set("taxon_extent_upload")

        # Setting up UI widgets
        cmdFrame = add_label_frame(parent, 'Process Taxon Extent', 100, 100)

        cmd_row = add_command(cmdFrame, 0, "Taxon Extent Shape Input Directory", self.extentDir, "Check Taxon Name Against Key", self.checkExtentDir)

        cmd_row = add_separator(cmdFrame, cmd_row)
        cmd_row = add_command(cmdFrame, cmd_row, "Taxon Extent Shape Input Directory", self.extentDir, "Load Into DB", self.processExtentDir)

        cmd_row = add_separator(cmdFrame, cmd_row)
        lsb = Spinbox(cmdFrame, textvariable=self.taxonLevelToRollupFor, width=59, values=(5, 4, 3, 2, 1), state=NORMAL)
        cmd_row = add_command(cmdFrame, cmd_row, "Target Taxon Level", lsb, "Rollup Lower Level Extents", self.rollupExtent)

        cmd_row = add_separator(cmdFrame, cmd_row)
        cmd_row = add_command(cmdFrame, cmd_row, "Taxon Key", self.taxonKeyToSimplify, "Simplify Taxon Extent", self.simplifyExtent)

        cmd_row = add_separator(cmdFrame, cmd_row)
        cmd_row = add_command(cmdFrame, cmd_row, "Taxon Key", self.taxonKeyToExtract)
        cmd_row = add_command(cmdFrame, cmd_row, "Taxon Extent Shape Output Directory", self.extentExtractDir, "Extract Taxon Extent", self.extractExtent)

        grid_panel(cmdFrame)

        parent.add(cmdFrame)

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
        taxonKey = int(self.taxonKeyToSimplify.get())

        print("Simplifying taxon: %s" % taxonKey)

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


    def extractExtent(self):
        taxonKey = str(self.taxonKeyToExtract.get())

        print("Extracting taxon: %s" % taxonKey)

        if self.extentExtractDir.get().strip() == "":
            outputShapeDir = os.getcwd()
        else:
            outputShapeDir = self.extentExtractDir.get().strip()

        outputShapeFile = os.path.join(outputShapeDir, "%s.shp" % taxonKey)

        dbOpts = self.dbPane.getDbOptions()

        extentSql = ('pgsql2shp -h "%s" -p %s -u %s -P "%s" -f "%s" %s "SELECT * FROM distribution.taxon_extent WHERE taxon_key = %s"'
                     % (dbOpts["server"], dbOpts["port"], dbOpts["username"], dbOpts["password"], outputShapeFile, dbOpts["dbname"], taxonKey))

        try:
            subprocess.check_call(extentSql, shell=True)
        except subprocess.CalledProcessError:
            for f in glob.glob(os.path.join(outputShapeDir, "%s.*" % taxonKey)):
                os.remove(f)

        if not (os.path.isfile(outputShapeFile) and os.access(outputShapeFile, os.R_OK)):
            messagebox.showinfo("Taxon extent extraction unsuccessful",
                                "Attempt to extract extent for the taxon '{0}' failed.".format(taxonKey) +
                                "Please check that the taxon key was entered correctly and try again!")
            return False

        return True


# ===============================================================================================
# ----- MAIN
if __name__ == "__main__":
    app = Application("Taxon Extent", TaxonExtentCommandPane)
    app.mainDbPane.db_name.set('sau_int')
    app.mainDbPane.db_username.set('sau_int')
    app.run()