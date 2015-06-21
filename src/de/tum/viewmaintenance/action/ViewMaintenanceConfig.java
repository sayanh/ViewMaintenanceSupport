package de.tum.viewmaintenance.action;

import com.datastax.driver.core.Cluster;
import de.tum.viewmaintenance.client.CassandraClient;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.viewsTableStructure.Column;
import de.tum.viewmaintenance.viewsTableStructure.Table;
import de.tum.viewmaintenance.viewsTableStructure.Views;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by shazra on 6/20/15.
 */
public class ViewMaintenanceConfig {
    protected static final Logger logger = LoggerFactory.getLogger(ViewMaintenanceConfig.class);
    private final static String CONFIG_FILE = "viewConfig.xml";

    public static void readViewConfigFromFile() {
        {
            XMLConfiguration config = new XMLConfiguration();
            config.setDelimiterParsingDisabled(true);
            try {
                config.load(CONFIG_FILE);
                System.out.println("testing=" + config.getList("tableDefinition.name"));
                Views viewsObj = Views.getInstance();

                List<String> views = config.getList("tableDefinition.name");
                String keyspaceName = config.getString("keyspace");
                viewsObj.setKeyspace(keyspaceName);

                System.out.println("views = " + views);
                List<Table> tempTableList = new ArrayList<>();
                for (int i = 0; i < views.size(); i++) {
                    Table table = new Table();
                    List<Column> columns = new ArrayList<>();
                    String viewTableName = config.getString("tableDefinition(" + i + ").name");
                    String primaryKeyName = config.getString("tableDefinition(" + i + ").primaryKey.name");
                    String primaryKeyDataType = config.getString("tableDefinition(" + i + ").primaryKey.dataType");
                    table.setName(viewTableName);
                    Column primaryKey = new Column();
                    primaryKey.setName(primaryKeyName);
                    primaryKey.setIsPrimaryKey(true);
                    primaryKey.setDataType(primaryKeyDataType);
                    columns.add(primaryKey);
                    logger.debug("primary Key ={} {}", primaryKeyName, primaryKeyDataType);
                    List<String> coldefs = config.getList("tableDefinition(" + i + ").column.name");
                    logger.debug("no. of columns present = {}", coldefs.size());
                    for (int x = 0; x < coldefs.size(); x++) {
                        Column col = new Column();
                        String colName = config.getString("tableDefinition(" + i + ").column(" + x + ").name");
                        String colDataType = config.getString("tableDefinition(" + i + ").column(" + x + ").dataType");
                        String colActionType = config.getString("tableDefinition(" + i + ").column(" + x + ").actionType");
                        String colConstraint = config.getString("tableDefinition(" + i + ").column(" + x + ").constraint");

                        col.setName(colName);
                        col.setDataType(colDataType);
                        col.setActionType(colActionType);
                        col.setConstraint(colConstraint);
                        columns.add(col);
                        logger.debug("Column definition = {} {} {} {}", colName, colDataType, colActionType, colConstraint);
                    }

                    table.setColumns(columns);
                    table.setKeySpace(keyspaceName);
                    logger.debug("Adding the table = {}", table);
                    tempTableList.add(table);
                    viewsObj.setTables(tempTableList);
                }
            } catch (ConfigurationException cex) {
                cex.printStackTrace();
            }
        }
    }

    public static void setupViewMaintenanceInfrastructure() {
        Views viewsObj = Views.getInstance();
        System.out.println("hash = " + viewsObj.hashCode());
        List<Table> tempTables = viewsObj.getTables();
        System.out.println("Tables present are = " + tempTables);
        Cluster cluster = CassandraClientUtilities.getConnection("localhost");
        boolean resultKeyspace = CassandraClientUtilities.createKeySpace(cluster, viewsObj.getKeyspace());
        System.out.println("Process to create keyspace is = " + resultKeyspace);
        if (resultKeyspace) {
            for (Table t : tempTables) {
                System.out.println("table details = " + t);
                System.out.println("is the table " + t.getName() + " present in the DB = " + CassandraClientUtilities.searchTable(cluster, t));
                if (!CassandraClientUtilities.searchTable(cluster, t)) {
                    CassandraClientUtilities.createTable(cluster, t);
                }


            }
        }

        CassandraClientUtilities.closeConnection(cluster);
    }

    public static void main(String[] args) {
        ViewMaintenanceConfig.readViewConfigFromFile();
        ViewMaintenanceConfig.setupViewMaintenanceInfrastructure();
    }
}
