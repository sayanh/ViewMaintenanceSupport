package de.tum.viewmaintenance.trigger;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.view_table_structure.Column;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
        LinkedTreeMap dataJson = request.getDataJson();
        boolean isResultSuccessful = false;
        Table viewTable = request.getViewTable();
        int sumAll = 0, sumGreaterThan = 0, sumLessThan = 0;

        List<Column> columns = viewTable.getColumns();
        Set keySet = dataJson.keySet();
        Iterator dataIter = keySet.iterator();
        String tempUserId = "";
        int age = 0;
        try {
            // Check whether the record exists or not
            // Assumption: The primary key is 1 for table emp.
            // TODO: Make the primary value configurable in the view config file.
            List<Row> results = CassandraClientUtilities.getAllRows(request.getViewTable().getKeySpace(), request.getViewTable().getName(), QueryBuilder.eq("k", 1));
            logger.debug("Response for getResultSet: {} ", results);
            logger.debug("Response for getResultSet size: {} ", results.size());

            int constraintCountGreater = 0;
            int constraintCountLess = 0;

            for (int i = 0; i < columns.size(); i++) {
                Column tempCol = columns.get(i);
                if (tempCol.getName().equalsIgnoreCase("k")) {
                    logger.debug("Value for {} is {}", tempCol.getName(), results.get(0).getInt(tempCol.getName()));

                } else if (tempCol.getName().equalsIgnoreCase("count_view1_age")) {
                    logger.debug("Value for {} is {}", tempCol.getName(), sumAll);

                } else if (tempCol.getName().equalsIgnoreCase("count_view2_age")) {
                    logger.debug("Value for {} is {}", tempCol.getName(), sumGreaterThan);
                    String constraintArr[] = tempCol.getConstraint().split(" ");
                    constraintCountGreater = Integer.parseInt(constraintArr[1]);
                } else if (tempCol.getName().equalsIgnoreCase("count_view3_age")) {
                    logger.debug("Value for {} is {}", tempCol.getName(), sumLessThan);
                    String constraintArr[] = tempCol.getConstraint().split(" ");
                    constraintCountLess = Integer.parseInt(constraintArr[1]);
                } // else if count_view3_age
            }

            // Getting all the rows from the base table
            List<Row> rowsBaseTable = CassandraClientUtilities.getAllRows(request.getBaseTable().getKeySpace(), request.getBaseTableName(), null);
            logger.debug("Basetable: Response for getResultSet: {} ", rowsBaseTable);
            logger.debug("Basetable: Response for getResultSet size: {} ", rowsBaseTable.size());
            if (rowsBaseTable.size() > 0) {
                for (Row baseTableRow: rowsBaseTable) {
                    int tempAge = baseTableRow.getInt("age");
                    sumAll = sumAll + age;
                    if (tempAge > constraintCountGreater) {
                        sumGreaterThan = sumGreaterThan + age;
                    }
                    if (tempAge < constraintCountLess) {
                        sumLessThan = sumLessThan + age;
                    }
                }
            }

            String insertQueryToView = "insert into" + request.getViewTable().getKeySpace() +
                    "." + request.getViewTable().getName() + " ( k, sum_view1_age, sum_view2_age, " +
                    "sum_view3_age) values ( 1, " + sumAll + ", " + sumGreaterThan + ", " +
                    sumLessThan + " )";
            logger.debug("******* Query : " + insertQueryToView);
            isResultSuccessful = CassandraClientUtilities.commandExecution("localhost", insertQueryToView);

        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("Error !!!" + e.getMessage());
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
