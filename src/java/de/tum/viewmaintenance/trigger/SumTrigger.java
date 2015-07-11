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
 * Created by anarchy on 6/27/15.
 */
public class SumTrigger extends TriggerProcess{
    private static final Logger logger = LoggerFactory.getLogger(SumTrigger.class);
    @Override
    public TriggerResponse insertTrigger(TriggerRequest request) {
        // vt2 -> This view is meant for "count"
        logger.debug("**********Inside Sum Insert Trigger for view maintenance**********");
        TriggerResponse response = new TriggerResponse();
//        LinkedTreeMap dataJson = request.getDataJson();
        boolean isResultSuccessful = false;
        Table viewTable = request.getViewTable();
        int sumAll = 0, sumGreaterThan = 0, sumLessThan = 0;

        List<Column> columns = viewTable.getColumns();
//        Set keySet = dataJson.keySet();
//        Iterator dataIter = keySet.iterator();
        String tempUserId = "";
        String insertQueryToView = "";
        try {
            // Check whether the record exists or not
            // Assumption: The primary key is 1 for table emp.
            // TODO: Make the primary value configurable in the view config file.
            List<Row> results = CassandraClientUtilities.getAllRows(request.getViewTable().getKeySpace(), request.getViewTable().getName(), QueryBuilder.eq("k", 1));
            logger.debug("Response for getResultSet: {} ", results);
            logger.debug("Response for getResultSet size: {} ", results.size());

            int constraintSumGreater = 0;
            int constraintSumLess = 0;

            for (int i = 0; i < columns.size(); i++) {
                Column tempCol = columns.get(i);
                if (tempCol.getName().equalsIgnoreCase("k")) {

                } else if (tempCol.getName().equalsIgnoreCase("sum_view1_age")) {

                } else if (tempCol.getName().equalsIgnoreCase("sum_view2_age")) {
                    String constraintArr[] = tempCol.getConstraint().split(" ");
                    constraintSumGreater = Integer.parseInt(constraintArr[1]);
                } else if (tempCol.getName().equalsIgnoreCase("sum_view3_age")) {
                    String constraintArr[] = tempCol.getConstraint().split(" ");
                    constraintSumLess = Integer.parseInt(constraintArr[1]);
                }
            }

//            logger.debug("Constraints: constraintSumGreater={} constraintSumLess={}", constraintSumGreater, constraintSumLess);

            // Getting all the rows from the base table
            logger.debug("keyspace: {}, basetable name: {} ", request.getBaseTableKeySpace(), request.getBaseTableName());
            List<Row> rowsBaseTable = CassandraClientUtilities.getAllRows(request.getBaseTableKeySpace(), request.getBaseTableName(), null);
//            logger.debug("Basetable: Response for getResultSet: {} ", rowsBaseTable);
//            logger.debug("Basetable: Response for getResultSet size: {} ", rowsBaseTable.size());
            if (rowsBaseTable.size() > 0) {
                for (Row baseTableRow: rowsBaseTable) {
                    int tempAge = baseTableRow.getInt("age");
//                    logger.debug("Insert trigger | value of age " + tempAge);
                    sumAll = sumAll + tempAge;
//                    logger.debug("******* View table insertion query : " + sumAll);
                    if (tempAge > constraintSumGreater) {
                        sumGreaterThan = sumGreaterThan + tempAge;
//                        logger.debug("******* View table insertion query : " + sumGreaterThan);
                    }
                    if (tempAge < constraintSumLess) {
                        sumLessThan = sumLessThan + tempAge;
//                        logger.debug("******* View table insertion query : " + sumLessThan);
                    }
                }
                insertQueryToView = "insert into " + request.getViewTable().getKeySpace() +
                        "." + request.getViewTable().getName() + " ( k, sum_view1_age, sum_view2_age, " +
                        "sum_view3_age) values ( 1, " + sumAll + ", " + sumGreaterThan + ", " +
                        sumLessThan + " )";

            } else {
                insertQueryToView = "insert into " + request.getViewTable().getKeySpace() +
                        "." + request.getViewTable().getName() + " ( k, sum_view1_age, sum_view2_age, " +
                        "sum_view3_age) values ( 1, 0, 0, 0 )";
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

    @Override
    public TriggerResponse updateTrigger(TriggerRequest request) {
        logger.debug("**********Inside Sum Update Trigger for view maintenance**********");
        TriggerResponse response = insertTrigger(request);
        return response;
    }

    @Override
    public TriggerResponse deleteTrigger(TriggerRequest request) {
        logger.debug("**********Inside Sum Delete Trigger for view maintenance**********");
        TriggerResponse response = insertTrigger(request);
        return response;
    }
}
