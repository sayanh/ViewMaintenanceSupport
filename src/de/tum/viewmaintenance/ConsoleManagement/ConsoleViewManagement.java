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

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by shazra on 8/18/15.
 */
public class ConsoleViewManagement {
    final static String NOT_AVAILABLE = "Functionality is not available now!!!!";
    final static String HELP = "########## Commands available ###########" +
            "\n show views - To see all the view tables in localhost" +
            "\n show rviews - To see all the remote view tables" +
            "\n delete views - To delete all the view tables in the localhost" +
            "\n reset basetables - To reset all the base tables in the localhost" +
            "\n reset rviews - To reset all the views in the remote host" +
            "\n reset rbasetables - To reset all the base tables in the remote host" +
            "\n describe <keyspace.tablename> - To get the description of the table";
    @Argument(metaVar = "[load]", usage = "targets")
    private List<String> targets = new ArrayList<String>();


    public void run() {
        System.out.println("Hello user!!");
        LoadGenerationProcess loadGenerationProcess = new LoadGenerationProcess();
        Load load = loadGenerationProcess.configFileReader();
        while (true) {
            System.out.println("Please type in a command below.");
            Scanner scanner = new Scanner(System.in);
            String command = scanner.nextLine();
            System.out.println("Your message: " + command);
            CmdLineParser parser = new CmdLineParser(this);
            String args[] = command.split(" ");
            try {
                parser.parseArgument(args);
            } catch (CmdLineException e) {
                e.printStackTrace();
            }

            if (!targets.isEmpty()) {
//                System.out.println("targets are " + targets);
                if (targets.get(0).equalsIgnoreCase("show") && targets.size() == 2 && targets.get(1)
                        .equalsIgnoreCase("views")) {
                    List<String> viewList = getAllViews();
                    if (viewList.isEmpty()) {
                        System.out.println("No view tables are present!!");
                    } else {
                        for (String row : getAllViews()) {
                            System.out.println(row);
                        }
                    }

                } else if (targets.get(0).equalsIgnoreCase("show") && targets.size() == 2 && targets.get(1)
                        .equalsIgnoreCase("remoteviews")) {
                    System.out.println("Getting all the tables from the remote machine...");
                    System.out.println(NOT_AVAILABLE);
                } else if (targets.get(0).equalsIgnoreCase("delete") && targets.size() == 2
                        && targets.get(1).equalsIgnoreCase("views")) {
                    System.out.println("Deleting all the views from the localhost.....");
                    for (String viewTableName : getAllViews()) {
                        Cluster cluster = CassandraClientUtilities.getConnection("localhost");
                        CassandraClientUtilities.deleteTable(cluster,
                                ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(viewTableName)[0],
                                ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(viewTableName)[1]);
                        CassandraClientUtilities.closeConnection(cluster);
                    }
                } else if (targets.get(0).equalsIgnoreCase("describe") && targets.size() == 2 ) {
                    String enteredTableName = targets.get(1);
                    List <String> viewList = getAllViews();
                    if (viewList.contains(enteredTableName)) {

                    } else {
                        System.out.println("Table " + enteredTableName + " not found!!!");
                    }

                } else if (targets.get(0).equalsIgnoreCase("reset") && targets.size() == 2) {

                    if (targets.get(1).equalsIgnoreCase("basetables")) {

                        for (Table table : load.getTables()) {
                            System.out.println("Table Name = " + table.getName());
                            System.out.println("schema name = " + table.getKeySpace());
//                            loadGenerationProcess.resetTestInfrastructure(table, "localhost");
                        }
                    } else {
                        System.out.println("Sorry, invalid command!!");
                        System.out.println(HELP);
                    }
                } else if (targets.get(0).equalsIgnoreCase("reset")
                        && targets.size() == 2 && targets.get(1).equalsIgnoreCase("rbasetables")) {
                    System.out.println("Resetting remote base tables....");
                } else if (targets.get(0).equalsIgnoreCase("load") && targets.size() == 1) {
                    System.out.println("Loading basetable for the localhost");
                } else if (targets.get(0).equalsIgnoreCase("load") && targets.get(1).equalsIgnoreCase("2nodes")) {
                    System.out.println("Loading basetables in remote machines(2 nodes)");
                } else if (targets.get(0).equalsIgnoreCase("help") && targets.size() == 1) {
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
        for (Row row : rows) {
            if (row.getString("keyspace_name").equalsIgnoreCase("schema2")) {
                finalListViews.add(row.getString("keyspace_name") + "." +
                        row.getString("columnfamily_name"));
            }
        }
        return finalListViews;
    }

    public static void main(String[] args) {
        new ConsoleViewManagement().run();
    }
}
