package de.tum.viewmaintenance.config;

import com.datastax.driver.core.Cluster;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.view_table_structure.Column;
import de.tum.viewmaintenance.view_table_structure.Table;
import de.tum.viewmaintenance.view_table_structure.Views;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by shazra on 6/20/15.
 * This singleton class reads the view maintenance config and sets up the view tables infrastructure.
 */
public class ViewMaintenanceConfig {
    protected static final Logger logger = LoggerFactory.getLogger(ViewMaintenanceConfig.class);
    private final static String CONFIG_FILE = "viewConfig.xml";


    /**
     * This methods read the files from the viewconfig.xml and initializes all the tables.
     */
    public static void readViewConfigFromFile() {
        logger.debug("************************ Reading View config files ******************");
        XMLConfiguration config = new XMLConfiguration();
        config.setDelimiterParsingDisabled(true);
        try {
            config.load(CONFIG_FILE);

            logger.debug("testing=" + config.getList("tableDefinition.name"));
            Views viewsObj = Views.getInstance();

            List<String> views = config.getList("tableDefinition.name");
            String keyspaceName = config.getString("keyspace");
            viewsObj.setKeyspace(keyspaceName);

            logger.debug("views = " + views);
            List<Table> tempTableList = new ArrayList<>();
            for (int i = 0; i < views.size(); i++) {
                Table table = new Table();
                List<Column> columns = new ArrayList<>();
                String viewTableName = config.getString("tableDefinition(" + i + ").name");
                String tableActionType = config.getString("tableDefinition(" + i + ").actionType");
                String tableBasedOn = config.getString("tableDefinition(" + i + ").basedOn");
                String refBaseTable = config.getString("tableDefinition(" + i + ").refBaseTable");
                String primaryKeyName = config.getString("tableDefinition(" + i + ").primaryKey.name");
                String primaryKeyDataType = config.getString("tableDefinition(" + i + ").primaryKey.dataType");

                table.setName(viewTableName);
                table.setRefBaseTable(refBaseTable);
                Column primaryKey = new Column();
                primaryKey.setName(primaryKeyName);
                primaryKey.setIsPrimaryKey(true);
                primaryKey.setDataType(primaryKeyDataType);
                columns.add(primaryKey);
                logger.debug("primary Key name: {} | and datatype: {}", primaryKeyName, primaryKeyDataType);
                List<String> coldefs = config.getList("tableDefinition(" + i + ").column.name");
                logger.debug("no. of columns present = {}", coldefs.size());
                for (int x = 0; x < coldefs.size(); x++) {
                    Column col = new Column();
                    String colName = config.getString("tableDefinition(" + i + ").column(" + x + ").name");
                    String colDataType = config.getString("tableDefinition(" + i + ").column(" + x + ").dataType");

                    String colConstraint = config.getString("tableDefinition(" + i + ").column(" + x + ").constraint");
                    String correspondingColumn = config.getString("tableDefinition(" + i + ").column(" + x + ").correspondingColumn");

                    col.setName(colName);
                    col.setDataType(colDataType);
                    col.setConstraint(colConstraint);
                    col.setCorrespondingColumn(correspondingColumn);
                    columns.add(col);
                    logger.debug("Column definition = {}, {}, {}, {}, {}", colName, colDataType, colConstraint, correspondingColumn);
                }

                table.setColumns(columns);
                table.setActionType(tableActionType);
                table.setBasedOn(tableBasedOn);
                table.setKeySpace(keyspaceName);
                logger.debug("Adding the table = {}", table);
                tempTableList.add(table);
                viewsObj.setTables(tempTableList);
            }
        } catch (Exception cex) {
            cex.printStackTrace();
            logger.error("Error !!!" + CassandraClientUtilities.getStackTrace(cex));
        }
    }

    /**
     * This methods creates the view tables in a cassandra instance.
     */

    public static void setupViewMaintenanceInfrastructure() {
        logger.debug("************************ Creating view maintenance tables ******************");
        Views viewsObj = Views.getInstance();
//        System.out.println("hash = " + viewsObj.hashCode());
        List<Table> tempTables = viewsObj.getTables();
        logger.debug("Tables present are = " + tempTables);
        Cluster cluster = CassandraClientUtilities.getConnection("localhost");
        boolean resultKeyspace = CassandraClientUtilities.createKeySpace(cluster, viewsObj.getKeyspace());
//        CassandraClientUtilities.closeConnection(cluster);
        logger.debug("Process to create keyspace is = " + resultKeyspace);
        if (resultKeyspace) {
            for (Table t : tempTables) {
                if (t.getActionType().equalsIgnoreCase("preAggregation")) {
                    setupPreAggregationViews(cluster, t);
                } else if (t.getActionType().equalsIgnoreCase("reverseJoin")) {
                    setupReverseJoinViews(cluster, t);
                } else {
                   CassandraClientUtilities.createTable(cluster, t);
               }
            }
        }

        CassandraClientUtilities.closeConnection(cluster);
    }

    /*
    * This method creates the view tables for reverse join.
    *
    */
    public static void setupReverseJoinViews(Cluster cluster, Table table) {
        logger.debug("********************** Creating the reverse join view table **********************");
        String baseTablesInvolved = table.getRefBaseTable();
        String baseTablesInvolvedArr [] = baseTablesInvolved.split(",");
        List<Column> columnList = table.getColumns();
        for (String baseTableName: baseTablesInvolvedArr) {
            Column colForBaseTable = new Column();
            colForBaseTable.setDataType("map <int, text>");
            colForBaseTable.setName(baseTableName.replaceAll("\\.", "_"));
            columnList.add(colForBaseTable);
        }

        table.setColumns(columnList);
        logger.debug(" Before create | Reverse join view " + table);
        CassandraClientUtilities.createTable(cluster, table);
    }

    /*
    * This method creates the pre-aggregation view table.
    */
    public static void setupPreAggregationViews(Cluster cluster, Table table) {
        /*
        * Assumption: The non primary key column is a map <int, String> i.e. primaryKeyBaseTable ->
        * String (collection of all other columns into a String delimited by a comma.)
        */

        logger.debug("********************** Creating the pre aggregation join view table **********************");
        List<Column> columns = table.getColumns();
        Column dataCol = new Column();
        dataCol.setName(table.getRefBaseTable().replaceAll("\\.", "_"));
        dataCol.setDataType("map <int, text>");
        columns.add(dataCol);
        table.setColumns(columns);
        logger.debug(" Before create | Pre Aggregation View " + table);
        CassandraClientUtilities.createTable(cluster, table);

    }
}
