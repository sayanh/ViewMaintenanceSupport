package org.apache.cassandra.viewmaintenance;

import com.datastax.driver.core.Cluster;
import de.tum.viewmaintenance.config.ViewMaintenanceConfig;
import org.json.simple.JSONObject;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.view_table_structure.Table;
import de.tum.viewmaintenance.view_table_structure.Views;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by shazra on 6/19/15.
 * <p/>
 * This class is meant to read the commitlogsv2 and take actions to maintain the views based on the events.
 */
public class ViewMaintenanceLogsReader extends Thread {

    private static volatile ViewMaintenanceLogsReader instance;
    private static final String STATUS_FILE = "viewmaintenance_status.txt"; // Stores the operation_id last processed
    private static final String LOG_FILE = "viewMaintenceCommitLogsv2.log";
    private static final String LOG_FILE_LOCATION = System.getProperty("user.dir") + "/logs/";
    private static final Logger logger = LoggerFactory.getLogger(ViewMaintenanceLogsReader.class);
    private static final int SLEEP_INTERVAL = 10000;

    public ViewMaintenanceLogsReader() {
        ViewMaintenanceConfig.readViewConfigFromFile();
        ViewMaintenanceConfig.setupViewMaintenanceInfrastructure();
        this.start();

    }

    @Override
    public void run() {
        int lastOpertationIdProcessed;
        BufferedReader bufferedReader = null;
        boolean isResultSuccessful= false;
        while (true) {
            try {
                File statusFile = new File(System.getProperty("user.dir") + "/data/" + STATUS_FILE);
                if (statusFile.exists()) {
                    lastOpertationIdProcessed = Integer.parseInt(new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/data/" + STATUS_FILE))));
                } else {
                    lastOpertationIdProcessed = 0;
                }

                bufferedReader = new BufferedReader(new FileReader(LOG_FILE_LOCATION + LOG_FILE));
                String tempLine = "";
                List<String> linesCommitLog = new ArrayList<>();
                while ((tempLine = bufferedReader.readLine()) != null) {
                    linesCommitLog.add(tempLine);
                }
                logger.debug("printing the list of tasks " + linesCommitLog);

                for (String lineActivity : linesCommitLog) {
                    JSONObject json;
                    try {
                        Map<String, Object> retMap = new Gson().fromJson(lineActivity, new TypeToken<HashMap<String, Object>>() {
                        }.getType());
                        LinkedTreeMap dataJson = null;
                        String type = "";
                        int operation_id = 0;
                        String tableName = "";

                        for (Map.Entry<String, Object> entry : retMap.entrySet()) {
                            String key = entry.getKey();
                            String value = "";
                            logger.debug(key + "/" + entry.getValue());
                            if (key.equals("type")) {
                                type = (String) entry.getValue();
                            } else if (key.equals("operation_id")) {
                                operation_id = ((Double) entry.getValue()).intValue();
                            } else if (key.equals("data")) {
                                dataJson = (LinkedTreeMap) entry.getValue();
                            } else if (key.equals("table")) {
                                tableName = (String) entry.getValue();
                            }
                        }
                        //TODO : Read from the configuration which table should be used to fill the views
                        if (lastOpertationIdProcessed < operation_id && tableName.equals("schematest.emp")) {
                            // Perform the action present in the logs file
                            logger.debug(" The action should be processed for operation_id" + operation_id);
                            if ("insert".equalsIgnoreCase(type)) {
                                isResultSuccessful = insertTrigger(dataJson);
                            } else if ("update".equalsIgnoreCase(type)) {
                                isResultSuccessful = updateTrigger(dataJson);
                            } else {
                                isResultSuccessful = deleteTrigger(dataJson);
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
                logger.debug("Going to sleep now");
                this.sleep(SLEEP_INTERVAL);
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

    private static boolean insertTrigger(LinkedTreeMap dataJson) {

        boolean isResultSuccessful = false;
        Views views = Views.getInstance();
        List<Table> tables = views.getTables();
        for (Table table : tables) {
            if (table.getName().equals("vt1")) { // Hardcoded value for view table name
                // TODO : Relationship between base table and view table should be configurable

                //TODO: The query should be generated from the table.getColumns() dynamically
                // and the contraints should be applied as required.
                String query = "Insert into " + views.getKeyspace() + "." + table.getName()
                        + " (k, select_view1_age) values ( ";
                Set keySet = dataJson.keySet();
                Iterator dataIter = keySet.iterator();
                String tempUserId = "";
                int age = 0;

                //TODO: Check for the table structure and primary key
                while (dataIter.hasNext()) {
                    String tempDataKey = (String) dataIter.next();
                    logger.debug("Key: " + tempDataKey);
                    logger.debug("Value: " + dataJson.get(tempDataKey));

                    if (tempDataKey.equals("user_id")) {
                        tempUserId = (String) dataJson.get(tempDataKey);
                    } else if (tempDataKey.equals("age")) {
                        age = Integer.parseInt((String) dataJson.get(tempDataKey));
                    }
                }
                query = query + tempUserId + " , " + age + ");";
                System.out.println("Query : " + query);
                //TODO: Have a queue and store all the jobs. Asynchronously run these jobs
                // background.
                Cluster cluster = CassandraClientUtilities.getConnection("localhost");
                isResultSuccessful = CassandraClientUtilities.commandExecution(cluster, query);
                CassandraClientUtilities.closeConnection(cluster);
                break;
            }
        }

        return isResultSuccessful;
    }

    private static boolean updateTrigger(LinkedTreeMap dataJson) {
        boolean isResultSuccessful = false;

        return isResultSuccessful;
    }
    private static boolean deleteTrigger(LinkedTreeMap dataJson) {
        boolean isResultSuccessful = false;

        return isResultSuccessful;
    }

}