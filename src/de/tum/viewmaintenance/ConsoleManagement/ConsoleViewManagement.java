package de.tum.viewmaintenance.ConsoleManagement;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.client.Load;
import de.tum.viewmaintenance.client.LoadGenerationProcess;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by shazra on 8/18/15.
 */
public class ConsoleViewManagement {
    final static String NOT_AVAILABLE = "Functionality is not available now!!!!";
    final static String DELTATABLE_SUFFIX = "_deltaview";
    private static final String STATUS_FILE = "viewmaintenance_status.txt"; // Stores the operation_id last processed
    private static final String LOG_FILE = "viewMaintenceCommitLogsv2.log";
    final static String HELP = "########## Commands available ###########" +
            "\n show views - To see all the view tables in localhost" +
            "\n show basetables - To see all the base tables in localhost" +
            "\n show rviews - To see all the remote view tables" +
            "\n show rbasetables - To see all the remote base tables" +
            "\n delete views - To delete all the view tables in the localhost" +
            "\n reset views - To reset all the view tables in the localhost" +
            "\n reset basetables - To reset all the base tables in the localhost" +
            "\n reset rviews - To reset all the views in the remote host" +
            "\n delete logs - To reset all the logs in the local host" +
            "\n reset rbasetables - To reset all the base tables in the remote host" +
            "\n describe <keyspace.tablename> - To get the description of the table";
    @Argument(metaVar = "[load]", usage = "targets")
    private List<String> targets = new ArrayList<String>();


    public void run() {
        System.out.println("Hello user!!");
        LoadGenerationProcess loadGenerationProcess = new LoadGenerationProcess();
        Load load = loadGenerationProcess.configFileReader();
        while ( true ) {
            System.out.println("Please type in a command below.");
            Scanner scanner = new Scanner(System.in);
            String command = scanner.nextLine();
            System.out.println("Your message: " + command);
            CmdLineParser parser = new CmdLineParser(this);
            String args[] = command.split(" ");
            try {
                parser.parseArgument(args);
            } catch ( CmdLineException e ) {
//                e.printStackTrace();
                System.out.println("Error!! " + e.getMessage());
            }

            if ( !targets.isEmpty() ) {
//                System.out.println("targets are " + targets);
                if ( targets.get(0).equalsIgnoreCase("show") && targets.size() == 2 && targets.get(1)
                        .equalsIgnoreCase("views") ) {
                    List<String> viewList = getAllViews();
                    if ( viewList.isEmpty() ) {
                        System.out.println("###########Output###########");
                        System.out.println("No view tables are present!!");
                    } else {
                        System.out.println("###########Output###########");
                        for ( String row : getAllViews() ) {
                            System.out.println(row);
                        }
                    }
                } else if ( targets.get(0).equalsIgnoreCase("god") && targets.size() == 2 && targets.get(1)
                        .equalsIgnoreCase("reset") ) {
                    // Delete all views
                    deleteAllViews();


                    // Reset all base tables
                    resetBaseAndDeltaTables(load);


                } else if ( targets.get(0).equalsIgnoreCase("show") && targets.size() == 2 && targets.get(1)
                        .equalsIgnoreCase("remoteviews") ) {
                    System.out.println("Getting all the tables from the remote machine...");
                    System.out.println(NOT_AVAILABLE);
                } else if ( targets.get(0).equalsIgnoreCase("delete") && targets.size() == 2 && targets.get(1)
                        .equalsIgnoreCase("logs") ) {
                    // Delete Commit logs and status
                    deleteLocalCommitLogsAndStatus();
                } else if ( targets.get(0).equalsIgnoreCase("delete") && targets.size() == 2
                        && targets.get(1).equalsIgnoreCase("views") ) {
                    System.out.println("Deleting all the views from the localhost.....");
                    for ( String viewTableName : getAllViews() ) {
                        Cluster cluster = CassandraClientUtilities.getConnection("localhost");
                        CassandraClientUtilities.deleteTable(cluster,
                                ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(viewTableName)[0],
                                ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(viewTableName)[1]);
                        CassandraClientUtilities.closeConnection(cluster);
                    }

                    System.out.println("###########Output###########");
                    System.out.println("Deletion Successful!!!");
                } else if ( targets.get(0).equalsIgnoreCase("reset") && targets.size() == 2
                        && targets.get(1).equalsIgnoreCase("views") ) {
                    System.out.println("Resetting all the views.....");
                    ViewMaintenanceUtilities.resetAllViews();
                } else if ( targets.get(0).equalsIgnoreCase("describe") && targets.size() == 2 ) {
                    String enteredTableName = targets.get(1);
                    List<String> viewList = getAllViews();
                    if ( viewList.contains(enteredTableName) ) {
                        System.out.println("###########Output###########");
                        System.out.println(NOT_AVAILABLE);
//                        Statement statement = QueryBuilder.desc()
                    } else {
                        System.out.println("###########Output###########");
                        System.out.println("Table " + enteredTableName + " not found!!!");
                    }

                } else if ( targets.get(0).equalsIgnoreCase("reset") && targets.size() == 2 ) {

                    if ( targets.get(1).equalsIgnoreCase("basetables") ) {

                        resetBaseAndDeltaTables(load);
                        System.out.println("###########Output###########");
                        System.out.println("Resetting of local base tables are successfully acheived!!!");
                    } else {
                        System.out.println("###########Output###########");
                        System.out.println("Sorry, invalid command!!");
                        System.out.println(HELP);
                    }
                } else if ( targets.get(0).equalsIgnoreCase("exit")
                        && targets.size() == 1 ) {
                    System.out.println("###########Output###########");
                    System.out.println("Bye!!!!");
                    System.exit(0);

                } else if ( targets.get(0).equalsIgnoreCase("show")
                        && targets.size() == 2 && targets.get(1).equalsIgnoreCase("basetables") ) {
                    List<String> baseTables = getAllBaseTables();
                    for ( String baseTable : baseTables ) {
                        System.out.println(baseTable);
                    }

                } else if ( targets.get(0).equalsIgnoreCase("reset")
                        && targets.size() == 2 && targets.get(1).equalsIgnoreCase("rbasetables") ) {
                    System.out.println("Resetting remote base tables....");
                } else if ( targets.get(0).equalsIgnoreCase("load") && targets.size() == 1 ) {
                    System.out.println("Loading basetable for the localhost");
                } else if ( targets.get(0).equalsIgnoreCase("load") && targets.get(1).equalsIgnoreCase("2nodes") ) {
                    System.out.println("Loading basetables in remote machines(2 nodes)");
                } else if ( targets.get(0).equalsIgnoreCase("help") && targets.size() == 1 ) {
                    System.out.println(HELP);
                } else {
                    System.out.println("Sorry, invalid command!!");
                    System.out.println(HELP);

                }
                targets.clear();
            }

        }
    }

    private List<String> getAllViews() {
        System.out.println("Getting all the tables from localhost...");
        Statement statement = QueryBuilder.select("keyspace_name", "columnfamily_name")
                .from("system", "schema_columnfamilies");
        List<Row> rows = CassandraClientUtilities.commandExecution("localhost", statement);
        List<String> finalListViews = new ArrayList<>();
        for ( Row row : rows ) {
            if ( row.getString("keyspace_name").equalsIgnoreCase("schema2") ) {
                finalListViews.add(row.getString("keyspace_name") + "." +
                        row.getString("columnfamily_name"));
//            } else if ( row.getString("keyspace_name").equalsIgnoreCase("schematest") &&
//                    row.getString("columnfamily_name").contains("deltaview") ) {
//                finalListViews.add(row.getString("keyspace_name") + "." +
//                        row.getString("columnfamily_name"));
            }


        }
        return finalListViews;
    }

    private List<String> getAllBaseTables() {
        System.out.println("Getting all the base tables from localhost...");
        Statement statement = QueryBuilder.select("keyspace_name", "columnfamily_name")
                .from("system", "schema_columnfamilies");
        List<Row> rows = CassandraClientUtilities.commandExecution("localhost", statement);
        List<String> finalListViews = new ArrayList<>();
        for ( Row row : rows ) {
            if ( row.getString("keyspace_name").equalsIgnoreCase("schematest")
//                     && !row.getString("columnfamily_name").contains("deltaview")
                    ) {
                finalListViews.add(row.getString("keyspace_name") + "." +
                        row.getString("columnfamily_name"));
            }
        }
        return finalListViews;
    }

    private void resetBaseAndDeltaTables(Load load) {
        for ( Table table : load.getTables() ) {
            System.out.println("Table Name = " + table.getName());
            System.out.println("schema name = " + table.getKeySpace());
            System.out.println("Deleting the table....");
            Cluster cluster = CassandraClientUtilities.getConnection("localhost");
            CassandraClientUtilities.deleteTable(cluster, table.getKeySpace(), table.getName());
            CassandraClientUtilities.deleteTable(cluster, table.getKeySpace(), table.getName() + DELTATABLE_SUFFIX);
            System.out.println("Rebuilding the tables from the config...." + table);
            CassandraClientUtilities.createTable(cluster, table);
            Table deltaTable = CassandraClientUtilities.createDeltaViewTable(table);
            CassandraClientUtilities.createTable(cluster, deltaTable);
            CassandraClientUtilities.closeConnection(cluster);
            System.out.println("Table: " + table.getKeySpace() + "." + table.getName() + " creation complete!!!");
        }
    }

    private void deleteAllViews() {
        for ( String viewTableName : getAllViews() ) {
            Cluster cluster = CassandraClientUtilities.getConnection("localhost");
            CassandraClientUtilities.deleteTable(cluster,
                    ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(viewTableName)[0],
                    ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(viewTableName)[1]);
            CassandraClientUtilities.closeConnection(cluster);
        }
    }

    private void deleteLocalCommitLogsAndStatus() {
        File commitLogsFile = new File("/home/anarchy/work/sources/cassandra/logs/viewMaintenceCommitLogsv2.log");

        if ( commitLogsFile.exists() ) {

            commitLogsFile.delete();
        }

        File viewMaintenanceStatusFile = new File("/home/anarchy/work/sources/cassandra/data/viewmaintenance_status.txt");

        if ( viewMaintenanceStatusFile.exists() ) {

            viewMaintenanceStatusFile.delete();
        }

        System.out.println("logs files are deleted!!!");
    }

    public static void main(String[] args) {
        new ConsoleViewManagement().run();
    }
}
