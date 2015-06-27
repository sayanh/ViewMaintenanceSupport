package de.tum.viewmaintenance.trigger;

import com.datastax.driver.core.Cluster;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.view_table_structure.Table;
import de.tum.viewmaintenance.view_table_structure.Views;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by anarchy on 6/27/15.
 */
public class SelectTrigger extends TriggerProcess {
    private static final Logger logger = LoggerFactory.getLogger(SelectTrigger.class);

    @Override
    public TriggerResponse insertTrigger(TriggerRequest request) {
        LinkedTreeMap dataJson = request.getDataJson();
        boolean isResultSuccessful = false;
        Views views = Views.getInstance();
        List<Table> tables = views.getTables();
        for (Table table : tables) {
            // vt1 -> This view performs select
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
        TriggerResponse response = new TriggerResponse();
        response.setIsSuccess(isResultSuccessful);
        return response;
    }

    @Override
    public TriggerResponse updateTrigger(TriggerRequest request) {
        return null;
    }

    @Override
    public TriggerResponse deleteTrigger(TriggerRequest request) {
        return null;
    }
}
