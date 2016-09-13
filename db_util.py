import optparse
import sqlprocessor as sp

from db import getDbConnection

def drop_foreign_key(dbPane):
    dbConn = getDbConnection(dbPane.getDbOptions())

    dbConn.execute("TRUNCATE TABLE admin.database_foreign_key")

    dbConn.execute("WITH db(db_owner) AS (" +
                   "SELECT u.usename " +
                   "  FROM pg_database d" +
                   "  JOIN pg_user u ON (u.usesysid = d.datdba)" +
                   " WHERE d.datname = current_database()" +
                   ")" +
                   "INSERT INTO admin.database_foreign_key(drop_fk_cmd, add_fk_cmd) " +
                   "SELECT f.* " +
                   "  FROM get_foreign_key_cmd_by_db_owner((SELECT db_owner FROM db)) AS f" +
                   " WHERE COALESCE(f.drop_fk_cmd, '') <> ''")

    dbConn.execute("SELECT exec(drop_fk_cmd) FROM admin.database_foreign_key")

    print("Foreign keys successfully dropped.")

def restore_foreign_key(dbPane):
    getDbConnection(dbPane.getDbOptions()).execute("SELECT exec(add_fk_cmd) FROM admin.database_foreign_key")

    print("Foreign keys successfully added.")

def refresh_all_materialized_views(dbPane):
    dbOpts = dbPane.getDbOptions()
    dbOpts['sqlfile'] = "sql/refresh_matviews.sql"
    dbOpts['threads'] = 4
    sp.process(optparse.Values(dbOpts))

    print('All materialized views in db refreshed.')
