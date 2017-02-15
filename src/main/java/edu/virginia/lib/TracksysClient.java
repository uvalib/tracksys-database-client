package edu.virginia.lib;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Created by md5wz on 4/7/15.
 */
public class TracksysClient {

    private Connection conn;

    private String connectionUrl;

    public TracksysClient(final String host, final String username, final String password) throws SQLException {
        DriverManager.registerDriver(new com.mysql.jdbc.Driver());
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

    private static final Collection<String> PUBLISHED_COMPONENTS_METADATA = Arrays.asList(new String[]{ "uva-lib:2065830", "uva-lib:2147933", "uva-lib:2221376", "uva-lib:2448611", "uva-lib:2246086" });

    private Summary getDescriptionOfPidWithoutReconnect(final String pid) throws SQLException {
        // first try master files
        final String masterFileSql = "select metadata.pid, metadata.indexing_scenario_id, master_files.component_id, metadata.id, units.metadata_id, metadata.title, um.title, um.pid, master_files.title from master_files LEFT JOIN (metadata) ON (master_files.metadata_id=metadata.id) LEFT JOIN (units) ON (master_files.unit_id=units.id) LEFT JOIN (metadata as um) ON (units.metadata_id=um.id) where master_files.pid=?;";
        PreparedStatement p = conn.prepareStatement(masterFileSql);
        p.setString(1, pid);
        ResultSet rs = p.executeQuery();
        try {
            if (rs.first()) {
                final String metadataPid = rs.getString(1);
                if (PUBLISHED_COMPONENTS_METADATA.contains(metadataPid)) {
                    // special handling for these few published hierarchicalcollections
                    PreparedStatement p2 = conn.prepareStatement("select pid, title from components where id=?");
                    p2.setInt(1, rs.getInt(3));
                    ResultSet componentInfo = p2.executeQuery();
                    if (componentInfo.next()) {
                        return new Summary("Page " + rs.getString(9) + " in " + componentInfo.getString(2), "http://search.lib.virginia.edu/catalog/" + componentInfo.getString(1) + "/view#openLayer/" + pid + "/0/0/0/1/0");
                    }
                } else {
                    final int indexingScenario = rs.getInt(2);
                    final String url = "http://search.lib.virginia.edu/catalog/" + metadataPid + ((indexingScenario == 2) ? "" : "/view#openLayer/" + pid + "/0/0/0/1/0");
                    if (rs.getString(1).equals(rs.getString(8))) {
                        return new Summary("Page " + rs.getString(9) + " in " + rs.getString(6), url);
                    } else {
                        return new Summary(rs.getString(6) + " from " + rs.getString(7), url);
                    }
                }
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

    public int getDigitizedImageCountForCatalogKey(final String catalogKey) throws SQLException {
        SQLException lastException = null;
        for (int attempts = 1; attempts <= 3; attempts ++) {
            try {
                return getDigitizedImageCountForCatalogKeyWithoutReconnect(catalogKey);
            } catch (SQLException ex) {
                lastException = ex;
                conn.close();
                conn = DriverManager.getConnection(connectionUrl);
            }
        }
        throw lastException;
    }

    public int getDigitizedImageCountForCatalogKeyWithoutReconnect(final String catalogKey) throws SQLException {
        final String sql = "select count(*) from master_files left join (metadata) ON (master_files.metadata_id=metadata.id) where metadata.catalog_key=?";
        final PreparedStatement s = conn.prepareStatement(sql);
        s.setString(1, catalogKey);
        final ResultSet rs= s.executeQuery();
        try {
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                throw new RuntimeException("Query returned 0 rows! (" + sql + " (" + catalogKey + ")");
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

    public void forEachPublishedCatalogKeyAndPid(Consumer<String> catKey, Consumer<String> pid) throws SQLException {
        String sql = "select catalog_key, pid from metadata where date_dl_ingest is not null and catalog_key is not null";
        PreparedStatement s = conn.prepareStatement(sql);
        ResultSet rs = s.executeQuery();
        while (rs.next()) {
            catKey.accept(rs.getString(1));
            pid.accept(rs.getString(2));
        }
    }

}
