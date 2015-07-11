package de.tum.viewmaintenance.config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.trigger.TriggerRequest;
import de.tum.viewmaintenance.view_table_structure.Table;
import de.tum.viewmaintenance.view_table_structure.Views;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by shazra on 6/27/15.
 *
 * This class performs insert, update and delete in the view tables as a trigger to the insert, update and delete
 * made to the base table.
 */
public class ViewMaintenanceUtilities {
    private static final Logger logger = LoggerFactory.getLogger(ViewMaintenanceUtilities.class);
//    protected static boolean insertTrigger(TriggerRequest request) {
//        LinkedTreeMap dataJson = request.getDataJson();
//        boolean isResultSuccessful = false;
//        Views views = Views.getInstance();
//        List<Table> tables = views.getTables();
//        for (Table table : tables) {
//            // vt1 -> This view performs select
//            if (table.getName().equals("vt1")) { // Hardcoded value for view table name
//                // TODO : Relationship between base table and view table should be configurable
//
//                //TODO: The query should be generated from the table.getColumns() dynamically
//                // and the contraints should be applied as required.
//                String query = "Insert into " + views.getKeyspace() + "." + table.getName()
//                        + " (k, select_view1_age) values ( ";
//                Set keySet = dataJson.keySet();
//                Iterator dataIter = keySet.iterator();
//                String tempUserId = "";
//                int age = 0;
//
//                //TODO: Check for the table structure and primary key
//                while (dataIter.hasNext()) {
//                    String tempDataKey = (String) dataIter.next();
//                    logger.debug("Key: " + tempDataKey);
//                    logger.debug("Value: " + dataJson.get(tempDataKey));
//
//                    if (tempDataKey.equals("user_id")) {
//                        tempUserId = (String) dataJson.get(tempDataKey);
//                    } else if (tempDataKey.equals("age")) {
//                        age = Integer.parseInt((String) dataJson.get(tempDataKey));
//                    }
//                }
//                query = query + tempUserId + " , " + age + ");";
//                System.out.println("Query : " + query);
//                //TODO: Have a queue and store all the jobs. Asynchronously run these jobs
//                // background.
//                Cluster cluster = CassandraClientUtilities.getConnection("localhost");
//                isResultSuccessful = CassandraClientUtilities.commandExecution(cluster, query);
//                CassandraClientUtilities.closeConnection(cluster);
//                break;
//            }
//        }
//
//        return isResultSuccessful;
//    }
//
//    protected static boolean updateTrigger(TriggerRequest request) {
//        boolean isResultSuccessful = false;
//
//        return isResultSuccessful;
//    }
//    protected static boolean deleteTrigger(TriggerRequest request) {
//        boolean isResultSuccessful = false;
//        StringBuilder query = new StringBuilder("delete from ");
//        Cluster cluster = CassandraClientUtilities.getConnection("localhost");
//        CassandraClientUtilities.commandExecution(cluster, "");
//        return isResultSuccessful;
//    }

//    public static ResultSet getRecordIfPresent(String query) {
//        ResultSet resultSet = null;
//        Cluster cluster = CassandraClientUtilities.getConnection("localhost");
//        ResultSet results = null;
//        Session session = null;
//        try {
//            session = cluster.connect();
//            System.out.println("Final query = " + query);
//            results = session.execute(query);
//            String resultString = results.all().toString();
//            logger.debug("Resultset {}", resultString);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            logger.debug("Error !!" + e.getMessage());
//        } finally {
//            session.close();
//            CassandraClientUtilities.closeConnection(cluster);
//        }
//
//        return  resultSet;
//    }
}
