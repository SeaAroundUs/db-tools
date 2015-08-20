package com.vulcan.sau.backend.processor.util;

import org.apache.camel.Exchange;

import org.apache.camel.Processor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.postgis.PGgeometry;

import com.google.inject.Inject;
import com.google.inject.name.Named;

//import org.postgis.PGgeometry;

public class ComputeHighSeasGeo implements Processor {
	private static final Logger logger = Logger.getLogger(ComputeHighSeasGeo.class);

	private PreparedStatement faoAreaStmt = null, getEezIntersectionsStmt = null, subtractEezStmt = null, insertHighSeasStmt = null;

	int pagesToFetch;

	@Inject
	@Named("sau-ds")
	private DataSource _sauds;
	private Connection fConn = null;

	@Override
	public void process(Exchange exchange) throws Exception {
		ResultSet rs, eRs, sRs;
		try {

			prepareDBResources();

			rs = faoAreaStmt.executeQuery();

			while (rs.next()) {
				int faoAreaId = rs.getInt("fao_area_id");
				PGgeometry faoGeom = (PGgeometry) rs.getObject("geom");

				logger.info("Procesing fao area id: " + faoAreaId);

				getEezIntersectionsStmt.setInt(1, faoAreaId);
				eRs = getEezIntersectionsStmt.executeQuery();
				if (eRs.next()) {
					java.sql.Array eezFids = eRs.getArray("fids");

					for (Integer fid: (Integer []) eezFids.getArray()) {
						logger.info("Subtracting away eez fid " + fid.intValue() + "...");
						subtractEezStmt.setObject(1, faoGeom);
						subtractEezStmt.setInt(2, fid.intValue());
						sRs = subtractEezStmt.executeQuery();
						if (sRs.next()) {
							faoGeom = (PGgeometry) sRs.getObject("geomDiff");
						}
					}

					insertHighSeasStmt.setInt(1, faoAreaId);
					insertHighSeasStmt.setArray(2, eezFids);
					insertHighSeasStmt.setObject(3, faoGeom);
					insertHighSeasStmt.execute();
				}
			}
			
			logger.info("High Seas procesing completed successfully.");
		}
		catch (Exception exp) {
			logger.error(exp);
			throw exp;
		}
	}

	private void prepareDBResources() throws Exception {
		if (fConn == null) {
			try {
				fConn = _sauds.getConnection();

				/* 
				 * Add the geometry types to the connection. Note that you 
				 * must cast the connection to the pgsql-specific connection 
				 * implementation before calling the addDataType() method. 
				 */
				((org.postgresql.PGConnection) fConn).addDataType("geometry",Class.forName("org.postgis.PGgeometry"));
				((org.postgresql.PGConnection) fConn).addDataType("box3d",Class.forName("org.postgis.PGbox3d"));

				faoAreaStmt = fConn.prepareStatement("" +
						"SELECT f.f_code::INT AS fao_area_id, f.geom" +
						"  FROM geo.fao f" +
						"  LEFT JOIN geo.high_seas h ON (h.fao_area_id = f.f_code::INT)" +
						" WHERE h.fao_area_id IS NULL" +
						" ORDER BY 1");

				getEezIntersectionsStmt = fConn.prepareStatement("" +
						//"SELECT array_agg(e.ogc_fid ORDER BY e.ogc_fid) AS fids" + 
						"SELECT array_agg(e.ogc_fid) AS fids" + 
						"  FROM geo.fao f" +
						"  JOIN geo.eez e ON (st_intersects(e.wkb_geometry, f.geom))" + 
						" WHERE f.f_code::INT = ?" +
						"   AND e.ogc_fid <> 76" +
						" GROUP BY f.f_code"
						);

				subtractEezStmt = fConn.prepareStatement("SELECT st_makevalid(st_difference(?, e.wkb_geometry)) AS geomDiff FROM geo.eez e WHERE e.ogc_fid = ?");

				insertHighSeasStmt = fConn.prepareStatement("INSERT INTO geo.high_seas(fao_area_id, eez_ogc_fid_intersects, geom) VALUES(?::INT, ?::INT[], st_multi(st_collectionextract(?, 3)))");
			}
			catch (Exception e) {
				logger.error("Encountered unexpected exception trying to prepare DB resources", e);
				throw e;
			}
		}
	}
}
