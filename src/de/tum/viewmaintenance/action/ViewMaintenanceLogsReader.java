package de.tum.viewmaintenance.action;

/**
 * Created by shazra on 6/20/15.
 */

import com.datastax.driver.core.Cluster;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.viewsTableStructure.Table;
import de.tum.viewmaintenance.viewsTableStructure.Views;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


/**
 * Created by shazra on 6/19/15.
 *
 * This class is meant to read the commitlogsv2 and take actions to maintain the views based on the events.
 */
public class ViewMaintenanceLogsReader extends Thread {

    private static volatile ViewMaintenanceLogsReader instance;
    private static final String STATUS_FILE = "viewmaintenance_status.txt"; // Stores the operation_id last processed
    private static final String LOG_FILE = "viewMaintenceCommitLogsv2.log";
    private static final String LOG_FILE_LOCATION = "/home/anarchy/work/sources/cassandra/logs/";

    public ViewMaintenanceLogsReader() {
        this.start();

    }

    @Override
    public void run() {
        Double lastOpertationIdProcessed;
        BufferedReader bufferedReader = null;
        while (true) {
            try {
                lastOpertationIdProcessed = Double.parseDouble(new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/data/" + STATUS_FILE))));
                bufferedReader = new BufferedReader(new FileReader(LOG_FILE_LOCATION + LOG_FILE));
                String tempLine = "";
                while ((tempLine = bufferedReader.readLine()) != null) {
                    JSONObject json;
                    try {
                        Map<String, Object> retMap = new Gson().fromJson(tempLine, new TypeToken<HashMap<String, Object>>() {
                        }.getType());
                        LinkedTreeMap dataJson = null;
                        String type = "";
                        Double operation_id = 0.0;
                        String tableName = "";

                        for (Map.Entry<String, Object> entry : retMap.entrySet()) {
                            String key = entry.getKey();
                            String value = "";
                            System.out.println(key + "/" + entry.getValue());
                            if (key.equals("type")) {
                                type = (String) entry.getValue();
                            } else if (key.equals("operation_id")) {
                                operation_id = (Double) entry.getValue();
                            } else if (key.equals("data")) {
                                dataJson = (LinkedTreeMap) entry.getValue();
                            } else if (key.equals("table")) {
                                tableName = (String) entry.getValue();
                            }
                        }
                        if (lastOpertationIdProcessed < operation_id) {
                            // Perform the action present in the logs file

                            if ("insert".equalsIgnoreCase(type)) {

                                Views views = Views.getInstance();
                                List<Table> tables = views.getTables();
                                for (Table table: tables) {
                                    if (table.getName().equals("vt1")) { // Hardcoded value for view table name
                                        // TODO : Relationship between base table and view table should be configurable
                                        String query = "Insert into " + views.getKeyspace() + "." + table.getName()
                                                + "values (k, select_view1_age) values ( ";
                                        Set keySet = dataJson.keySet();
                                        Iterator dataIter = keySet.iterator();

                                        //TODO: Check for the table structure and primary key
                                        while (dataIter.hasNext()) {
                                            String tempDataKey = (String) dataIter.next();
                                            System.out.println("Key: " + tempDataKey);
                                            System.out.println("Value: " + dataJson.get(tempDataKey));
                                            if (tempDataKey.equals("user_id"))
                                            {
                                                query = query + dataJson.get(tempDataKey) + ", ";
                                            } else if (tempDataKey.equals("age")) {
                                                query = query + dataJson.get(tempDataKey) + ") ";
                                            }
                                        }
                                        System.out.println("Query : " + query);
                                        Cluster cluster = CassandraClientUtilities.getConnection("localhost");
                                        CassandraClientUtilities.commandExecution(cluster, query);
                                        CassandraClientUtilities.closeConnection(cluster);
                                        break;
                                    }
                                }
                            } else if ("update".equalsIgnoreCase(type)) {

                            } else {

                            }


                            // Update the lastOperationProcessed variable
                            lastOpertationIdProcessed = operation_id;

                            // Update the status file
                            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/data/" + STATUS_FILE));
                            bufferedWriter.write(lastOpertationIdProcessed + "");
                            bufferedWriter.flush();
                            bufferedWriter.close();

                        } else {
                            // The action is already executed. Continue.
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                System.out.println("Going to sleep now");
                this.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    public static ViewMaintenanceLogsReader getInstance() {
        if (instance == null) {
            synchronized (ViewMaintenanceLogsReader.class) {
                if (instance == null) {
                    instance = new ViewMaintenanceLogsReader();
                }
            }
        }

        return instance;
    }

    public static void main(String[] args) {
        ViewMaintenanceConfig.readViewConfigFromFile();
        ViewMaintenanceConfig.setupViewMaintenanceInfrastructure();
        ViewMaintenanceLogsReader obj = ViewMaintenanceLogsReader.getInstance();

    }
}
