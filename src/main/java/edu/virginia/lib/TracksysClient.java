package edu.virginia.lib;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by md5wz on 4/7/15.
 */
public class TracksysClient {

    private Connection conn;

    private String connectionUrl;

    public TracksysClient(final String host, final String username, final String password) throws SQLException {
        connectionUrl = "jdbc:mysql://" + host + "/tracksys_production?user=" + username + "&password=" + password;
        conn = DriverManager.getConnection(connectionUrl);
    }

    public Summary getDescriptionOfPid(final String pid) throws SQLException {
        SQLException lastException = null;
        for (int attempts = 1; attempts <= 3; attempts ++) {
            try {
                return getDescriptionOfPidWithoutReconnect(pid);
            } catch (SQLException ex) {
                lastException = ex;
                conn.close();
                conn = DriverManager.getConnection(connectionUrl);
            }
        }
        throw lastException;
    }

    private Summary getDescriptionOfPidWithoutReconnect(final String pid) throws SQLException {
        // first try master files
        final String masterFileSql = "select master_files.title, metadata.title, master_files.pid, metadata.pid, master_files.indexing_scenario_id, master_files.component_id from master_files LEFT JOIN (units, metadata) ON (master_files.unit_id=units.id and units.metadata_id=metadata.id) where master_files.pid=?;";
        PreparedStatement p = conn.prepareStatement(masterFileSql);
        p.setString(1, pid);
        ResultSet rs = p.executeQuery();
        try {
            if (rs.first()) {
                if (rs.getInt(5) == 2) {
                    // image from a collection
                    return new Summary("\"" + rs.getString(1) + "\" from the collection \"" + rs.getString(2) + "\"", "http://search.lib.virginia.edu/catalog/" + rs.getString(3));
                } else {
                    if (rs.getInt(6) != 0) {
                        // page of a component
                        PreparedStatement p2 = conn.prepareStatement("select master_files.title, components.title, components.pid from master_files LEFT JOIN (components) ON (master_files.component_id=components.id) where master_files.pid=?;");
                        p2.setString(1, pid);
                        ResultSet rs2 = p2.executeQuery();
                        try {
                            rs2.first();
                            return new Summary("Page \"" + rs2.getString(1) + "\" from \"" + rs2.getString(2) + "\"", "http://search.lib.virginia.edu/catalog/" + rs2.getString(3) + "/view#openLayer/" + pid + "/0/0/0/1/0");
                        } finally {
                            rs2.close();
                        }
                    } else {
                        // page from a book
                        return new Summary("Page \"" + rs.getString(1) + "\" from \"" + rs.getString(2) + "\"", "http://search.lib.virginia.edu/catalog/" + rs.getString(4) + "#openLayer/" + rs.getString(3) + "/0/0/0/1/0");
                    }
                }
            }
        } finally {
            rs.close();
        }

        // not a master file, try a metadata item
        final String metadataSql = "select metadata.title, metadata.pid from metadata where metadata.pid=?;";
        p = conn.prepareStatement(metadataSql);
        p.setString(1, pid);
        rs = p.executeQuery();
        try {
            if (rs.first()) {
                return new Summary(rs.getString(1), "http://search.lib.virginia.edu/catalog/" + rs.getString(2));
            }
        } finally {
            rs.close();
        }

        return null;
    }

    public List<String> getPagePids(final String pid, boolean allowComponentLookup) throws SQLException {
        SQLException lastException = null;
        for (int attempts = 1; attempts <= 3; attempts ++) {
            try {
                return getPagePidsWithoutReconnect(pid, allowComponentLookup);
            } catch (SQLException ex) {
                lastException = ex;
                conn.close();
                conn = DriverManager.getConnection(connectionUrl);
            }
        }
        throw lastException;
    }

    private List<String> getPagePidsWithoutReconnect(final String pid, boolean allowComponentLookup) throws SQLException {
        List<String> result = new ArrayList<String>();
        // return early if it's a master file
        PreparedStatement isMasterFile = conn.prepareStatement("select master_files.id from master_files where master_files.pid=?;");
        isMasterFile.setString(1, pid);
        ResultSet r = isMasterFile.executeQuery();
        try {
            if (r.next()) {
                return result;
            }
        } finally {
            r.close();
        }
        final String masterFileSql = "select master_files.pid from master_files LEFT JOIN (units, metadata) ON (master_files.unit_id=units.id and units.metadata_id=metadata.id) where metadata.pid=?;";
        PreparedStatement p = conn.prepareStatement(masterFileSql);
        p.setString(1, pid);
        ResultSet rs = p.executeQuery();
        try {
            while (rs.next()) {
                result.add(rs.getString(1));
            }
        } finally {
            rs.close();
        }
        if (result.isEmpty()) {
            if (!allowComponentLookup) {
                throw new RuntimeException("The item may be a component, but 'allowComponentLookup' is false!");
            }
            // try to treat it as a component... a very expensive call
            final String componentMasterFileSql = "select master_files.pid from components JOIN (master_files) ON (components.id=master_files.component_id) where components.pid=?;";
            p = conn.prepareStatement(componentMasterFileSql);
            p.setString(1, pid);
            rs = p.executeQuery();
            try {
                while (rs.next()) {
                    result.add(rs.getString(1));
                }
            } finally {
                rs.close();
            }
        }
        return result;
    }

    public int lookupAgencyId(final String agencyName) throws SQLException {
        SQLException lastException = null;
        for (int attempts = 1; attempts <= 3; attempts ++) {
            try {
                return lookupAgencyIdWithoutReconnect(agencyName);
            } catch (SQLException ex) {
                lastException = ex;
                conn.close();
                conn = DriverManager.getConnection(connectionUrl);
            }
        }
        throw lastException;
    }

    public int lookupAgencyIdWithoutReconnect(final String agencyName) throws SQLException {
        String sql = "select id, name from agencies where name = ?";
        PreparedStatement s = conn.prepareStatement(sql);
        s.setString(1, agencyName);
        ResultSet rs = s.executeQuery();
        try {
            if (rs.next()) {
                final int id = rs.getInt(1);
                if (rs.next()) {
                    throw new IllegalStateException("More than one matching agency!");
                } else {
                    return id;
                }
            } else {
                throw new IllegalStateException("No matching agencies found!");
            }
        } finally {
            rs.close();
        }
    }

    public Connection getDBConnection() {
    	return this.conn;
    }
    
    public void closeConnection() throws SQLException {
        conn.close();
    }

    public static class Summary {
        private String title;
        private String url;

        private Summary(final String title, final String url) {
            this.title = title;
            this.url = url;
        }

        public String getTitle() {
            return this.title;
        }

        public String getUrl() {
            return this.url;
        }

        public String toString() {
            return title + " (" + url + ")";
        }
    }

}
