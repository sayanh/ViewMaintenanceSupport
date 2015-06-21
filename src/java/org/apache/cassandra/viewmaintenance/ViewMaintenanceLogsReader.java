package org.apache.cassandra.viewmaintenance;

import org.apache.cassandra.db.DataTracker;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by shazra on 6/19/15.
 */
public class ViewMaintenanceLogsReader extends Thread{

    private static volatile ViewMaintenanceLogsReader instance;
    private static final String STATUS_FILE = "viewmaintenance_status.txt"; // Stores the operation_id last processed
    private static final String LOG_FILE = "";
    
    public ViewMaintenanceLogsReader() {
        this.start();

    }
    @Override
    public void run() {
        String lastOpertationIdProcessed = "";
        BufferedReader bufferedReader = null;
        while(true) {
            try {
                lastOpertationIdProcessed = new String(Files.readAllBytes(Paths.get(System.getProperty("user.dir") + "/view_data/" + STATUS_FILE)));
                bufferedReader = new BufferedReader(new FileReader(System.getProperty("user.dir") + "/view_data/" + LOG_FILE));
                String tempLine = "";
                while((tempLine = bufferedReader.readLine()) != null)
                {
                    JSONObject json;
                    try {
                        json = (JSONObject) new JSONParser().parse(tempLine);
                        String operation_id = (String)json.get("operation_id");
                        System.out.println("operation_id = " + operation_id);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                this.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    public static ViewMaintenanceLogsReader getInstance() {
        if (instance == null ) {
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

    }
}
