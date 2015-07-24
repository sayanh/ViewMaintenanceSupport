package de.tum.viewmaintenance.client;

/**
 * Created by shazra on 6/21/15.
 */

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.sun.org.apache.xpath.internal.SourceTree;
import de.tum.viewmaintenance.viewsTableStructure.Column;
import de.tum.viewmaintenance.viewsTableStructure.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CassandraClientUtilities {
    protected static final Logger logger = LoggerFactory.getLogger(CassandraClientUtilities.class);

    /*
    * This method creates a keyspace if it is not present in a Cassandra instance
    */
    public static boolean createKeySpace(Cluster cluster, String keyspace) {
        boolean isSucc = false;
        try {
            logger.debug("Creating keyspace {}", keyspace);
            String query = "CREATE SCHEMA IF NOT EXISTS " +
                    keyspace + " WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
            isSucc = CassandraClientUtilities.commandExecution(cluster, query);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error !!" + e.getMessage());
            return false;
        }
        return isSucc;
    }

    /*
    * This method creates a connection to a Cassandra instance and returns the cluster
    */
    public static Cluster getConnection(String ip) {
        Session session = null;
        Cluster cluster = null;
        try {
            ResultSet results;
            Row rows;
            cluster = Cluster.builder()
                    .addContactPoint(ip)
                    .build();
        } catch (Exception e) {
            logger.error("Error occurred CassandraClientUtilities| getConnection | " + e.getMessage());
            e.printStackTrace();
        }
        return cluster;
    }

    public static boolean closeConnection(Cluster cluster) {
        try {
            if (!cluster.isClosed()) {
                logger.debug("Closing cluster connection!!");
                cluster.close();
            }
            logger.info("Connection is successfully closed!!");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error !!" + e.getMessage());
            return false;
        }
        return true;
    }





    /*
    * This method executes any CQL3 query on a Cassandra instance
    *
    */
    public static boolean commandExecution(Cluster cluster, String query) {
        ResultSet results = null;
        Session session = null;
        try {
            session = cluster.connect();
            logger.debug("Final query = " + query);
            results = session.execute(query);
            String resultString = results.all().toString();

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            session.close();
        }

        return true;
    }

    public static boolean commandExecution(String ip, String query) {
        boolean isResultSuccessful = false;
        Cluster cluster = null;
        try {
            cluster = CassandraClientUtilities.getConnection("localhost");
            isResultSuccessful = CassandraClientUtilities.commandExecution(cluster, query.toString());
        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("Error !!!" + e.getMessage());
            return false;
        } finally {
            CassandraClientUtilities.closeConnection(cluster);
        }
        return true;
    }

    /*
    * This method creates a table in a Cassandra instance
    */
    public static boolean createTable(Cluster cluster, Table table) {
        ResultSet results = null;
        Session session = null;
        try {
            session = cluster.connect();
            StringBuilder query = new StringBuilder();
            query.append("create table if not exists " + table.getKeySpace() + "." + table.getName() + " (");
            List<Column> columns = table.getColumns();
            for (Column col : columns) {
                String primaryKey = col.isPrimaryKey() ? " PRIMARY KEY" : "";
                query.append(col.getName() + " " + col.getDataType() + primaryKey + ",");
            }
            String finalQuery = query.substring(0, query.length() - 1) + ");";
            System.out.println("Final query = " + finalQuery);
            results = session.execute(finalQuery);

            logger.debug("Successfully created table {}.{}", table.getKeySpace(), table.getName());

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error !!" + e.getMessage());
            return false;
        } finally {
            logger.debug("Closing the session!!! isSessionClosed = " + session.isClosed());
            if (!session.isClosed()) {
                session.close();
            }
        }

        return true;
    }

    /*
    * This method deletes a table from a Cassandra instance
    */
    public static boolean deleteTable(Cluster cluster, Table table) {
        ResultSet results = null;
        Session session = null;
        try {
            session = cluster.connect();
            StringBuilder query = new StringBuilder();
            query.append("delete table " + table.getKeySpace() + "." + table.getName() + ";");

            System.out.println("Final query = " + query);
            results = session.execute(query.toString());

            logger.debug("Successfully delete table {}.{}", table.getKeySpace(), table.getName());

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error !!" + e.getMessage());
            return false;
        } finally {
            session.close();
        }

        return true;
    }

    /*
    * This method deletes a table from a Cassandra instance
    */
    public static boolean deleteTable(Cluster cluster, String keySpaceName, String tableName) {
        ResultSet results = null;
        Session session = null;
        try {
            session = cluster.connect();
            StringBuilder query = new StringBuilder();
            query.append("drop table if exists " + keySpaceName + "." + tableName + ";");

            System.out.println("Final query = " + query);
            results = session.execute(query.toString());

            logger.debug("Successfully delete table {}.{}", keySpaceName, tableName);

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error !!" + e.getMessage());
            return false;
        } finally {
            session.close();
        }

        return true;
    }

    /*
    * This method checks the presence of a table in a Cassandra instance
    *
    */
    public static boolean searchTable(Cluster cluster, Table table) {
        ResultSet results = null;
        Session session = null;
        try {
            session = cluster.connect();
            StringBuilder query = new StringBuilder();
            query.append("Select columnfamily_name from system.schema_columnfamilies where columnfamily_name = '" + table.getName() + "' ALLOW FILTERING ;");

            System.out.println("Final query = " + query);
            results = session.execute(query.toString());
            String resultString = results.all().toString();
            logger.debug("Resultset {}", resultString);
            if (resultString.contains(table.getName())) {
                return true;
            }

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            if (session.isClosed()) {
                session.close();
            }
        }

        return false;
    }

    public static ResultSet getResultSet(String ip, String query) {
        boolean isResultSuccessful = false;
        Session session = null;
        ResultSet resultSet = null;
        Cluster cluster = null;
        try {
            cluster = CassandraClientUtilities.getConnection("localhost");
            session = cluster.connect();
            logger.debug("Final query = " + query);
            resultSet = session.execute(query);
            String resultString = resultSet.all().toString();
            logger.debug("Result set= {} ", resultSet.all());
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            session.close();
            cluster.close();
        }
        return resultSet;
    }

    static Table createDeltaViewTable(Table baseTable) {
        Table viewTable = new Table();
        viewTable.setName(baseTable.getName() + "_deltaView");
        viewTable.setKeySpace(baseTable.getKeySpace());
        List<Column> columns = baseTable.getColumns();
        List<Column> viewTableCols = new ArrayList<>();
        System.out.println("columns size = " + columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Column col = columns.get(i);
            System.out.println("working on col = " + col.getName());
            Column viewCol = new Column();
            if (col.isPrimaryKey()) {
                viewCol.setName(col.getName());
                viewCol.setIsPrimaryKey(col.isPrimaryKey());

            } else {
                Column viewCol_cur = new Column();
                viewCol_cur.setName(col.getName() + "_cur");
                viewCol.setName(col.getName() + "_last");
                viewCol_cur.setDataType(col.getDataType());
                viewTableCols.add(viewCol_cur);

            }

            viewCol.setDataType(col.getDataType());
            viewTableCols.add(viewCol);
        }
        viewTable.setColumns(viewTableCols);
        return viewTable;
    }

}
