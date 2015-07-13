package de.tum.viewmaintenance.trigger;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.view_table_structure.Column;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by anarchy on 7/12/15.
 */
public class ReverseJoinViewTrigger extends TriggerProcess {
    private static final Logger logger = LoggerFactory.getLogger(ReverseJoinViewTrigger.class);
    @Override
    public TriggerResponse insertTrigger(TriggerRequest request) {
        // vt5 -> This view performs "reverseJoinView"
        logger.debug("**********Insert Trigger for reverse join view maintenance**********");

        TriggerResponse triggerResponse = new TriggerResponse();
        LinkedTreeMap dataMap = request.getDataJson();
        boolean isResultSuccessful = false;
        Table table = request.getViewTable();
//        StringBuffer query = new StringBuffer("Insert into " + request.getViewKeyspace() + "." + table.getName() + " ( ");
//        StringBuffer valuesPartQuery = new StringBuffer("values ( ");
        List<Column> columns = table.getColumns();
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
            }  else if (tempDataKey.equals("colaggkey_x")) {
                colAggKey = (String) dataMap.get(tempDataKey);
            }
        }

        Statement existingRecordReverseJoinViewQuery = QueryBuilder.select(request.getBaseTableName().replaceAll("\\.", "_")).from(
                request.getViewKeyspace(), request.getViewTable().getName()).where(QueryBuilder.eq(
                "colaggkey_x", colAggKey));

        List<Row> existingRecordReverseJoinView  = CassandraClientUtilities.commandExecution("localhost", existingRecordReverseJoinViewQuery);

        logger.debug("Reverse join | insertTrigger | existing record: " + existingRecordReverseJoinView);

        if (existingRecordReverseJoinView.size() == 0) {
            // Insert into the reverse join view
            triggerResponse.setIsSuccess(insertIntoReverseJoinViewTable(request, tempUserId, colAggKey, age));
        } else {
            // Update the reverse join view
            triggerResponse.setIsSuccess(updateIntoReverseJoinViewTable(request, tempUserId, colAggKey, age, existingRecordReverseJoinView.get(0)));
        }

        return triggerResponse;
    }


    private boolean insertIntoReverseJoinViewTable(TriggerRequest request, String primaryKeyBaseTable, String colAggKey, int age){
        logger.debug("********** insertIntoReverseJoinViewTable **********");
        boolean isResultSucc = false;
        try {
            StringBuffer insertQuery = new StringBuffer("insert into " + request.getViewKeyspace() + "." + request.getViewTable().getName() + "( " + request.getViewTable().getBasedOn() + ", "  + request.getBaseTableKeySpace() + "_" +
                    request.getBaseTableName() + " ) values ( '" + colAggKey + "'" + "['") ;
            String cellValue = primaryKeyBaseTable + "," + age;
            insertQuery.append(cellValue + "'])");
            logger.debug("insertIntoReverseJoinViewTable Query {} ", insertQuery.toString());
            CassandraClientUtilities.commandExecution("localhost", insertQuery.toString());
            isResultSucc = true;
        } catch (Exception e) {
            logger.debug("Error !!" + CassandraClientUtilities.getStackTrace(e));
            isResultSucc = false;
        }
        return  isResultSucc;
    }

    private boolean updateIntoReverseJoinViewTable(TriggerRequest request, String primaryKeyBaseTable, String colAggKey, int age, Row existingRecord){
        logger.debug("********** updateIntoReverseJoinViewTable **********");
        boolean isResultSucc = false;
        try {
            String colNameInViewTable = request.getBaseTableKeySpace() + "_" + request.getBaseTableName();
            StringBuffer updateQuery = new StringBuffer("update " + request.getViewKeyspace()
                    + "." + request.getViewTable().getName() + "set " + colNameInViewTable + "=");
            List<String> existingRecordStr = existingRecord.getList(colNameInViewTable, String.class);

            List<String> finalRecordList = new ArrayList<>();
            for (String cellValue: existingRecordStr) {
                String cellValArr [] = cellValue.split(",");
                if (cellValArr[0].equalsIgnoreCase(primaryKeyBaseTable)) {
                    finalRecordList.add(cellValArr[0] + "," + age);
                } else {
                    finalRecordList.add(cellValue);
                }
            }
            updateQuery.append(finalRecordList + " where " + request.getViewTable().getBasedOn() + " = " + colAggKey);
            logger.debug("insertIntoReverseJoinViewTable Query {} ", updateQuery.toString());
            isResultSucc = CassandraClientUtilities.commandExecution("localhost", updateQuery.toString());

        } catch (Exception e) {
            logger.debug("Error !!" + CassandraClientUtilities.getStackTrace(e));
            isResultSucc = false;
        }
        return  isResultSucc;
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
