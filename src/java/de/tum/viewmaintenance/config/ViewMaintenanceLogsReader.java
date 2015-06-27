package de.tum.viewmaintenance.config;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import de.tum.viewmaintenance.config.ViewMaintenanceLogsReader;
import de.tum.viewmaintenance.trigger.TriggerRequest;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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
        logger.debug("************************ Starting view maintenance infrastructure set up ******************");
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
                        String whereString = null;
                        String type = "";
                        int operation_id = 0;
                        String tableName = "";
                        TriggerRequest request = new TriggerRequest();

                        for (Map.Entry<String, Object> entry : retMap.entrySet()) {
                            String key = entry.getKey();
                            String value = "";
                            logger.debug(key + "/" + entry.getValue());
                            if (key.equalsIgnoreCase("type")) {
                                type = (String) entry.getValue();
                                request.setType(type);
                            } else if (key.equalsIgnoreCase("operation_id")) {
                                operation_id = ((Double) entry.getValue()).intValue();
                            } else if (key.equalsIgnoreCase("data")) {
                                dataJson = (LinkedTreeMap) entry.getValue();
                                request.setDataJson(dataJson);
                            } else if (key.equalsIgnoreCase("table")) {
                                tableName = (String) entry.getValue();
                                request.setBaseTableName(tableName);
                            } else if (key.equalsIgnoreCase("where")) {
                                logger.debug("************** getting class ***************" + entry.getValue().getClass());
                                whereString = (String) entry.getValue();
                                request.setWhereString(whereString);
                            }
                        }
                        //TODO : Read from the configuration which table should be used to fill the views
                        if (lastOpertationIdProcessed < operation_id && tableName.equals("schematest.emp")) {
                            // Perform the action present in the logs file
                            logger.debug(" The action should be processed for operation_id" + operation_id);
                            if ("insert".equalsIgnoreCase(type)) {
                                isResultSuccessful = ViewMaintenanceUtilities.insertTrigger(request);
                            } else if ("update".equalsIgnoreCase(type)) {
                                isResultSuccessful = ViewMaintenanceUtilities.updateTrigger(request);
                            } else  if ("delete".equalsIgnoreCase(type)) {
                                isResultSuccessful = ViewMaintenanceUtilities.deleteTrigger(request);
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
}