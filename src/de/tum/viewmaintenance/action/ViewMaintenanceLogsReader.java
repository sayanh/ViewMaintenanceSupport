package de.tum.viewmaintenance.action;

/**
 * Created by shazra on 6/20/15.
 */

import com.datastax.driver.core.Cluster;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.view_table_structure.Table;
import de.tum.viewmaintenance.view_table_structure.Views;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


/**
 * Created by shazra on 6/19/15.
 * <p>
 * This class is meant to read the commitlogsv2 and take actions to maintain the views based on the events.
 */
public class ViewMaintenanceLogsReader extends Thread {

    private static volatile ViewMaintenanceLogsReader instance;
    private static final String STATUS_FILE = "viewmaintenance_status.txt"; // Stores the operation_id last processed
    private static final String LOG_FILE = "viewMaintenceCommitLogsv2.log";
    private static final String LOG_FILE_LOCATION = "/home/anarchy/work/sources/cassandra/logs/";

    public ViewMaintenanceLogsReader() {
        ViewMaintenanceConfig.readViewConfigFromFile();
        ViewMaintenanceConfig.setupViewMaintenanceInfrastructure();
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
                List<String> linesCommitLog = new ArrayList<>();
                while ((tempLine = bufferedReader.readLine()) != null) {
                    linesCommitLog.add(tempLine);
                }
                System.out.println("printing the list of tasks " + linesCommitLog);

                for (String lineActivity : linesCommitLog) {
                    JSONObject json;
                    boolean isResultSuccessful = false;
                    try {
                        Map<String, Object> retMap = new Gson().fromJson(lineActivity, new TypeToken<HashMap<String, Object>>() {
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
                            System.out.println(" The action should be processed for opertion_id" + operation_id);
                            if ("insert".equalsIgnoreCase(type)) {

                                Views views = Views.getInstance();
                                List<Table> tables = views.getTables();
                                for (Table table : tables) {
                                    if (table.getName().equals("vt1")) { // Hardcoded value for view table name
                                        // TODO : Relationship between base table and view table should be configurable
                                        String query = "Insert into " + views.getKeyspace() + "." + table.getName()
                                                + " (k, select_view1_age) values ( ";
                                        Set keySet = dataJson.keySet();
                                        Iterator dataIter = keySet.iterator();
                                        String tempUserId = "";
                                        int age = 0;

                                        //TODO: Check for the table structure and primary key
                                        while (dataIter.hasNext()) {
                                            String tempDataKey = (String) dataIter.next();
                                            System.out.println("Key: " + tempDataKey);
                                            System.out.println("Value: " + dataJson.get(tempDataKey));

                                            if (tempDataKey.equals("user_id")) {
                                                tempUserId = (String) dataJson.get(tempDataKey);
                                            } else if (tempDataKey.equals("age")) {
                                                age = Integer.parseInt((String) dataJson.get(tempDataKey));
                                            }
                                        }
                                        query = query + tempUserId + " , " + age + ");";
                                        System.out.println("Query : " + query);
                                        Cluster cluster = CassandraClientUtilities.getConnection("localhost");
                                        isResultSuccessful = CassandraClientUtilities.commandExecution(cluster, query);
                                        CassandraClientUtilities.closeConnection(cluster);
                                        break;
                                    }
                                }
                            } else if ("update".equalsIgnoreCase(type)) {

                            } else {

                            }

                            if (isResultSuccessful) {
                                // If result is successful
                                // Update the lastOperationProcessed variable
                                lastOpertationIdProcessed = operation_id;

                                // Update the status file
                                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(System.getProperty("user.dir") + "/data/" + STATUS_FILE));
                                bufferedWriter.write(lastOpertationIdProcessed + "");
                                bufferedWriter.flush();
                                bufferedWriter.close();
                            }

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
        ViewMaintenanceLogsReader obj = ViewMaintenanceLogsReader.getInstance();
        System.out.println("Insert into schema2.vt1 (k, select_view1_age) values ( 102 , 42);".contains("view"));
    }
}
