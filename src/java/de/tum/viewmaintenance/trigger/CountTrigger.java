package de.tum.viewmaintenance.trigger;


import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.view_table_structure.Column;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by shazra on 6/27/15.
 */
public class CountTrigger extends TriggerProcess {
    private static final Logger logger = LoggerFactory.getLogger(CountTrigger.class);

    /*
    *
    * This method is triggered when an insert query is made by a client.
    * It ensures that the count views are consistent.
    *
    */
    @Override
    public TriggerResponse insertTrigger(TriggerRequest request) {
        // vt2 -> This view is meant for "count"
        logger.debug("**********Inside Count Insert Trigger for view maintenance**********");
        TriggerResponse response = new TriggerResponse();
//        LinkedTreeMap dataJson = request.getDataJson();
        boolean isResultSuccessful = false;

        Table viewTable = request.getViewTable();
        int countAll = 0, countGreaterThan = 0, countLessThan = 0;


        List<Column> columns = viewTable.getColumns();
//        Set keySet = dataJson.keySet();
//        Iterator dataIter = keySet.iterator();
        String tempUserId = "";
        int age = 0;
        String insertQueryToView = "";
        try {
            // Check whether the record exists or not
            // Assumption: The primary key is 1 for table emp.
            // TODO: Make the primary value configurable in the view config file.
            List<Row> results = CassandraClientUtilities.getAllRows(request.getViewTable().getKeySpace(), request.getViewTable().getName(), QueryBuilder.eq("k", 1));
//            logger.debug("Response for getResultSet: {} ", results);
//            logger.debug("Response for getResultSet size: {} ", results.size());

            int constraintCountGreater = 0;
            int constraintCountLess = 0;

            for (int i = 0; i < columns.size(); i++) {
                Column tempCol = columns.get(i);
                if (tempCol.getName().equalsIgnoreCase("k")) {

                } else if (tempCol.getName().equalsIgnoreCase("count_view1_age")) {

                } else if (tempCol.getName().equalsIgnoreCase("count_view2_age")) {
                    String constraintArr[] = tempCol.getConstraint().split(" ");
                    constraintCountGreater = Integer.parseInt(constraintArr[1]);
                } else if (tempCol.getName().equalsIgnoreCase("count_view3_age")) {
                    String constraintArr[] = tempCol.getConstraint().split(" ");
                    constraintCountLess = Integer.parseInt(constraintArr[1]);
                }
            }

            logger.debug("Constraints: constraintCountGreater={} constraintCountLess={}", constraintCountGreater, constraintCountLess);

            // Getting all the rows from the base table
            logger.debug("keyspace: {}, basetable name: {} ", request.getBaseTableKeySpace(), request.getBaseTableName());
            List<Row> rowsBaseTable = CassandraClientUtilities.getAllRows(request.getBaseTableKeySpace(), request.getBaseTableName(), null);
            logger.debug("Basetable: Response for getResultSet: {} ", rowsBaseTable);
            logger.debug("Basetable: Response for getResultSet size: {} ", rowsBaseTable.size());
            countAll = rowsBaseTable.size();
            if (rowsBaseTable.size() > 0)   {
                for (Row baseTableRow: rowsBaseTable) {
                    int tempAge = baseTableRow.getInt("age");
                    if (tempAge > constraintCountGreater) {
                        countGreaterThan +=1;
                    }
                    if (tempAge < constraintCountLess) {
                        countLessThan +=1;
                    }
                }
                insertQueryToView = "insert into " + request.getViewTable().getKeySpace() +
                        "." + request.getViewTable().getName() + " ( k, count_view1_age, count_view2_age, " +
                        "count_view3_age) values ( 1, " + countAll + ", " + countGreaterThan + ", " +
                        countLessThan + " )";

            } else {
                insertQueryToView = "insert into " + request.getViewTable().getKeySpace() +
                        "." + request.getViewTable().getName() + " ( k, count_view1_age, count_view2_age, " +
                        "count_view3_age) values ( 1, 0, 0, 0 )";
            }

            logger.debug("******* View table insertion query : " + insertQueryToView);
            isResultSuccessful = CassandraClientUtilities.commandExecution("localhost", insertQueryToView);

        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("Error !!! Stacktrace:" + CassandraClientUtilities.getStackTrace(e));
        }
        response.setIsSuccess(isResultSuccessful);
        return response;
    }


    /*
    *
    * This method is triggered when an update query is made by a client.
    * It ensures that the count views are consistent.
    *
    */
    @Override
    public TriggerResponse updateTrigger(TriggerRequest request) {
        logger.debug("**********Inside Count Update Trigger for view maintenance**********");
        TriggerResponse response = insertTrigger(request);
        return response;
    }

    /*
    *
    * This method is triggered when an delete query is made by a client.
    * It ensures that the count views are consistent.
    *
    */
    @Override
    public TriggerResponse deleteTrigger(TriggerRequest request) {
        logger.debug("**********Inside Count Delete Trigger for view maintenance**********");
        TriggerResponse response = insertTrigger(request);
        return response;
    }
}
