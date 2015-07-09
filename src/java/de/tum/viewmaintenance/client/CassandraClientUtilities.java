package de.tum.viewmaintenance.client;

/**
 * Created by shazra on 6/21/15.
 */

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.trigger.DeltaViewTrigger;
import de.tum.viewmaintenance.trigger.TriggerProcess;
import de.tum.viewmaintenance.trigger.TriggerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.tum.viewmaintenance.view_table_structure.Column;
import de.tum.viewmaintenance.view_table_structure.Table;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

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
                    .addContactPoint("localhost")
                    .build();
        } catch (Exception e) {
            logger.error("Error occurred CassandraClientUtilities| getConnection | " + e.getMessage());
            e.printStackTrace();
        }
        return cluster;
    }

    public static boolean closeConnection(Cluster cluster) {
        try {
            if (cluster.isClosed()) {
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
            if (session.isClosed()) {
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
            cluster = CassandraClientUtilities.getConnection(ip);
            isResultSuccessful = CassandraClientUtilities.commandExecution(cluster, query.toString());
        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("Error !!!" + e.getMessage());
            return false;
        } finally {
            if (!cluster.isClosed()) {
                CassandraClientUtilities.closeConnection(cluster);
            }
        }
        return true;
    }

    public static List<Row> getAllRows(String keyspace, String table , Clause equal) {
        Cluster cluster = null;
        Session session = null;
        List<Row> result = null;
        try {
            cluster = CassandraClientUtilities.getConnection("localhost");
            session = cluster.connect();
            Statement statement = null;
            if (equal == null) {
                statement = QueryBuilder
                        .select()
                        .all()
                        .from(keyspace, table);
            } else {
                statement = QueryBuilder
                        .select()
                        .all()
                        .from(keyspace, table).
                                where(equal);
            }

            logger.debug("Final statement got executed : ", statement);
            result = session
                    .execute(statement)
                    .all();
            session.close();
            cluster.close();


        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("Error !!!" + CassandraClientUtilities.getStackTrace(e));
        } finally {
            if (session.isClosed()) {
                session.close();
            }

            if (cluster.isClosed()) {
                cluster.close();
            }
        }
        return  result;
    }

    public static String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        String exceptionAsString = sw.toString();
        return exceptionAsString;
    }

}
