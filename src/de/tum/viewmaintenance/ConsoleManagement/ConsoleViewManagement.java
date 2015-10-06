package de.tum.viewmaintenance.ConsoleManagement;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import de.tum.viewmaintenance.Evaluation.MemoryAnalysis;
import de.tum.viewmaintenance.Evaluation.MemoryLogsReader;
import de.tum.viewmaintenance.Evaluation.ViewManagerOperationsVsTimeConsumedPlots;
import de.tum.viewmaintenance.OperationsManagement.OperationsGenerator;
import de.tum.viewmaintenance.OperationsManagement.OperationsUtils;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.client.Load;
import de.tum.viewmaintenance.client.LoadGenerationProcess;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.SocketException;
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
    private static final String IP_IN_USE = "192.168.56.20";
    public static final String MEMORY_DATA_FILE = "/home/anarchy/work/sources/cassandra/memoryLogs.out";

    final static String HELP = "########## Commands available ###########" +
            "\n show views - To see all the view tables in localhost" +
            "\n show basetables - To see all the base tables in localhost" +
            "\n show rviews - To see all the remote view tables" +
            "\n show rbasetables - To see all the remote base tables" +
            "\n delete views - To delete all the view tables in the localhost" +
            "\n delete rviews - To delete all the view tables in the remote machine" +
            "\n reset views - To reset all the view tables in the localhost" +
            "\n reset basetables - To reset all the base tables in the localhost" +
            "\n reset rviews - To reset all the views in the remote host" +
            "\n delete logs - To reset all the logs in the local host" +
            "\n reset rbasetables - To reset all the base tables in the remote host" +
            "\n memoryanalysis - To run memory analysis on memory logs" +
            "\n clearremotelogs - To clear remote logs from all the machines" +
            "\n describe <keyspace.tablename> - To get the description of the table";
    @Argument(metaVar = "[load]", usage = "targets")
    private List<String> targets = new ArrayList<String>();


    public void run() {
        System.out.println("Hello user!!");
        LoadGenerationProcess loadGenerationProcess = new LoadGenerationProcess();
        Load load = loadGenerationProcess.configFileReader();
        OperationsGenerator operationsGenerator = OperationsGenerator.getInstance();
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
                    List<String> viewList = getAllViews("localhost");
                    if ( viewList.isEmpty() ) {
                        System.out.println("###########Output###########");
                        System.out.println("No view tables are present!!");
                    } else {
                        System.out.println("###########Output###########");
                        for ( String row : getAllViews("localhost") ) {
                            System.out.println(row);
                        }
                    }
                } else if ( targets.get(0).equalsIgnoreCase("god") && targets.size() == 2 && targets.get(1)
                        .equalsIgnoreCase("reset") ) {
                    // Delete all views
                    try {
                        deleteAllViews();
                    } catch ( SocketException e ) {
                        e.printStackTrace();
                    }


                    // Reset all base tables
                    try {
                        resetBaseAndDeltaTables(load, CassandraClientUtilities.getEth0Ip());
                    } catch ( SocketException e ) {
                        e.printStackTrace();
                    }

                } else if ( targets.get(0).equalsIgnoreCase("god") && targets.size() == 2 && targets.get(1)
                        .equalsIgnoreCase("remotereset") ) {
                    // Delete all views
                    deleteAllViewsRemote(IP_IN_USE);


                    // Reset all base tables
                    try {
                        resetBaseAndDeltaTables(load, IP_IN_USE);
                    } catch ( SocketException e ) {
                        e.printStackTrace();
                    }


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
                    for ( String viewTableName : getAllViews("localhost") ) {
                        Cluster cluster = null;
                        try {
                            cluster = CassandraClientUtilities
                                    .getConnection(CassandraClientUtilities.getEth0Ip());
                        } catch ( SocketException e ) {
                            e.printStackTrace();
                        }
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
                } else if ( targets.get(0).equalsIgnoreCase("reset") && targets.size() == 2
                        && targets.get(1).equalsIgnoreCase("rviews") ) {
                    System.out.println("Resetting all the remote views.....");
                    ViewMaintenanceUtilities.resetAllViewRemote(IP_IN_USE);

                } else if ( targets.get(0).equalsIgnoreCase("describe") && targets.size() == 2 ) {
                    String enteredTableName = targets.get(1);
                    List<String> viewList = getAllViews("localhost");
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

                        try {
                            resetBaseAndDeltaTables(load, CassandraClientUtilities.getEth0Ip());
                        } catch ( SocketException e ) {
                            e.printStackTrace();
                        }
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
                    List<String> baseTables = null;
                    try {
                        baseTables = getAllBaseTables();
                    } catch ( SocketException e ) {
                        e.printStackTrace();
                    }
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
                } else if ( targets.get(0).equalsIgnoreCase("clearremotelogs") && targets.size() == 1 ) {

                } else if ( targets.get(0).equalsIgnoreCase("applyload") && targets.size() == 1 ) {
//                    LoadGenerationProcess loadGenerationProcess = new LoadGenerationProcess();
//                    OperationsGenerator operationsGenerator = OperationsGenerator.getInstance();
//                    Load load = loadGenerationProcess.configFileReader();
                    System.out.println("Applying load of " + operationsGenerator.getNumOfOperations() + " operations");
                    System.out.println("Length of the list of tables=" + load.getTables().size());

                    List<String> operationList = operationsGenerator.cqlGenerator();
                    OperationsUtils.displayOperationsList(operationList);

                    OperationsUtils.pumpInOperationsIntoCassandra(operationsGenerator.getIpsInvolved().get(0), operationList,
                            operationsGenerator.getIntervalOfFiringOperations());

                    System.out.println("#########################################################################");

                    System.out.println("#############################Load is applied successfully!!############################");
                } else if ( targets.get(0).equalsIgnoreCase("memorytimeanalysis") && targets.size() == 1 ) {
                    try {

                        // Move logs from remote server to localhost
                        List<String> lines = MemoryLogsReader.getMemoryLogs(operationsGenerator.getIpsInvolved().get(0),
                                operationsGenerator.getUsername(), operationsGenerator.getPassword());

                        // Running memory analysis engine to create plots.

                        System.out.println(" num of lines processed " + lines.size());
                        MemoryAnalysis memoryAnalysis = new MemoryAnalysis();
                        memoryAnalysis.drawMemoryAnalysisHistogram(lines);
                        System.out.println("##### Memory usage plots are successfully generated!! #####");

                        ViewManagerOperationsVsTimeConsumedPlots operationsTimePlots =
                                new ViewManagerOperationsVsTimeConsumedPlots();

                        operationsTimePlots.drawDualAxisHistogramOperationsVsTime
                                (operationsGenerator.getUsername(),
                                        operationsGenerator.getPassword(),
                                        operationsGenerator.getIpsInvolved().get(0));

                        System.out.println("####### Time vs operation plots are " +
                                "created successfully!! ");
                    } catch ( IOException e ) {
                        e.printStackTrace();
                    }

                } else if ( targets.get(0).equalsIgnoreCase("timeplots") && targets.size() == 1 ) {

                    //
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

    private List<String> getAllViews(String ip) {
        System.out.println("Getting all the tables from ..." + ip);
        Statement statement = QueryBuilder.select("keyspace_name", "columnfamily_name")
                .from("system", "schema_columnfamilies");
        List<Row> rows = CassandraClientUtilities.commandExecution(ip, statement);
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

    private List<String> getAllBaseTables() throws SocketException {
        System.out.println("Getting all the base tables from localhost...");
        Statement statement = QueryBuilder.select("keyspace_name", "columnfamily_name")
                .from("system", "schema_columnfamilies");
        List<Row> rows = CassandraClientUtilities.commandExecution(CassandraClientUtilities.getEth0Ip(), statement);
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

    private void resetBaseAndDeltaTables(Load load, String ip) throws SocketException {
        for ( Table table : load.getTables() ) {
            System.out.println("Table Name = " + table.getName());
            System.out.println("schema name = " + table.getKeySpace());
            System.out.println("Deleting the table....");
            Cluster cluster = CassandraClientUtilities.getConnection(ip);
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

    private void deleteAllViews() throws SocketException {
        for ( String viewTableName : getAllViews(CassandraClientUtilities.getEth0Ip()) ) {
            Cluster cluster = CassandraClientUtilities.getConnection(CassandraClientUtilities.getEth0Ip());
            CassandraClientUtilities.deleteTable(cluster,
                    ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(viewTableName)[0],
                    ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(viewTableName)[1]);
            CassandraClientUtilities.closeConnection(cluster);
        }
    }

    private void deleteAllViewsRemote(String ip) {
        for ( String viewTableName : getAllViews(ip) ) {
            Cluster cluster = null;
            try {
                cluster = CassandraClientUtilities.getConnection(ip);
            } catch ( SocketException e ) {
                e.printStackTrace();
            }
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
