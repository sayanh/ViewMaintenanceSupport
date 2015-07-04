package de.tum.viewmaintenance.trigger;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClient;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by shazra on 7/4/15.
 */
public class DeltaViewTrigger extends TriggerProcess {
    private static final Logger logger = LoggerFactory.getLogger(DeltaViewTrigger.class);
    private static final String DELTAVIEW_SUFFIX = "_deltaview";
    @Override
    public TriggerResponse insertTrigger(TriggerRequest request) {
        logger.debug("---------- Inside Insert DeltaViewTrigger ----------");
        TriggerResponse response = new TriggerResponse();
        boolean isSuccess = false;
        try{

            // Assumption of the primary key to be user_id
            //TODO: Get the base table structure and dynamically determine the primary key and data type of the columns

            LinkedTreeMap dataMap = request.getDataJson();
            Set keySet = dataMap.keySet();
            Iterator dataIter = keySet.iterator();

            String tempUserId = "";
            int age = 0;
            String colAggKey = "";
            while (dataIter.hasNext()) {
                String tempDataKey = (String) dataIter.next();
                logger.debug("Key: " + tempDataKey);
                logger.debug("Value: " + dataMap.get(tempDataKey));

                if (tempDataKey.equals("user_id")) {
                    tempUserId = (String) dataMap.get(tempDataKey);
                } else if (tempDataKey.equals("age")) {
                    age = Integer.parseInt((String) dataMap.get(tempDataKey));
                } else if (tempDataKey.equals("colaggkey_x")) {
                    colAggKey = (String) dataMap.get(tempDataKey);
                }
            }

            String insertQueryToView = "insert into " + request.getBaseTableKeySpace() + "." + request.getBaseTableName()
                    + DELTAVIEW_SUFFIX + " ( user_id, age_cur, colaggkey_x_cur ) values ( " + tempUserId + "," + age + "," + colAggKey +" )";

            logger.debug(" InsertQuery to Delta View: " + insertQueryToView);
            isSuccess = CassandraClientUtilities.commandExecution("localhost", insertQueryToView);

        } catch (Exception e) {
            logger.debug("Error!!! " + CassandraClientUtilities.getStackTrace(e));
        }
        response.setIsSuccess(isSuccess);
        return response;
    }

    @Override
    public TriggerResponse updateTrigger(TriggerRequest request) {
        logger.debug("---------- Inside Update DeltaViewTrigger ----------");
        TriggerResponse response = new TriggerResponse();
        boolean isSuccess = false;
        try{

            // Assumption of the primary key to be user_id
            //TODO: Get the base table structure and dynamically determine the primary key and data type of the columns

            LinkedTreeMap dataMap = request.getDataJson();
            Set keySet = dataMap.keySet();
            Iterator dataIter = keySet.iterator();

            String tempUserId = "";
            int age = 0;
            String colAggKey = "";
            String whereString = request.getWhereString();
            List<String> changedFields = new ArrayList<>();

            StringTokenizer whereStringTokenizer = new StringTokenizer(whereString, " ");
            while (whereStringTokenizer.hasMoreTokens()) {
                if (whereStringTokenizer.nextToken().equalsIgnoreCase("=")) {
                    tempUserId = whereStringTokenizer.nextToken();
                    break;
                }
            }

            while (dataIter.hasNext()) {
                String tempDataKey = (String) dataIter.next();
                logger.debug("Key: " + tempDataKey);
                logger.debug("Value: " + dataMap.get(tempDataKey));

                changedFields.add(tempDataKey);

                if (tempDataKey.equals("user_id") && "".equalsIgnoreCase(tempUserId)) {
                    tempUserId = (String) dataMap.get(tempDataKey);
                } else if (tempDataKey.equals("age")) {
                    age = Integer.parseInt((String) dataMap.get(tempDataKey));
                } else if (tempDataKey.equals("colaggkey_x")) {
                    colAggKey = (String) dataMap.get(tempDataKey);
                }
            }

            logger.debug(" Base table information: {}.{} ", request.getBaseTableKeySpace(), request.getBaseTableName());

            List<Row> results= CassandraClientUtilities.getAllRows(request.getBaseTableKeySpace(), request.getBaseTableName() + DELTAVIEW_SUFFIX, QueryBuilder.eq("user_id", Integer.parseInt(tempUserId)));
            logger.debug("Existing record = " + results);

            Row existingRecord = results.get(0);
            String colaggkey_x_last = existingRecord.getString("colaggkey_x_cur");
            int age_last = existingRecord.getInt("age_cur");

            // TODO: Get the column name dynamically from the table description of Cassandra.
            StringBuilder updateQueryToDeltaView = new StringBuilder("update " + request.getBaseTableKeySpace() + "." + request.getBaseTableName() + DELTAVIEW_SUFFIX + " set ");
            if (changedFields.contains("age")) {
                updateQueryToDeltaView.append("age_cur=" + age + ", age_last=" + age_last + ",");
            }
            if (changedFields.contains("colaggkey_x")) {
                updateQueryToDeltaView.append("colaggkey_x_cur=" + colAggKey + ", colaggkey_x_last=" + colaggkey_x_last + ",");
            }

            if (updateQueryToDeltaView.lastIndexOf(",") == updateQueryToDeltaView.length() - 1) {
                updateQueryToDeltaView.delete(updateQueryToDeltaView.length() - 1, updateQueryToDeltaView.length());
            }
            updateQueryToDeltaView.append(" " + whereString);

            logger.debug(" UpdateQuery to Delta View: " + updateQueryToDeltaView);
            isSuccess = CassandraClientUtilities.commandExecution("localhost", updateQueryToDeltaView.toString());

        } catch (Exception e) {
            logger.debug("Error!!! " + CassandraClientUtilities.getStackTrace(e));
        }
        response.setIsSuccess(isSuccess);
        return response;
    }

    @Override
    public TriggerResponse deleteTrigger(TriggerRequest request) {
        return null;
    }
}
