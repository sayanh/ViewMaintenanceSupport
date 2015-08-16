package de.tum.viewmaintenance.trigger;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.view_table_structure.Column;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by anarchy on 6/27/15.
 */
public class MaxTrigger extends TriggerProcess {
    private static final Logger logger = LoggerFactory.getLogger(MaxTrigger.class);

    @Override
    public TriggerResponse insertTrigger(TriggerRequest request) {
        // vt2 -> This view is meant for "count"
        logger.debug("**********Inside max Insert Trigger for view maintenance**********");
        TriggerResponse response = new TriggerResponse();
//        LinkedTreeMap dataJson = request.getDataJson();
        boolean isResultSuccessful = false;
        Table viewTable = request.getViewTable();
        int maxAll = 0, maxGreaterThan = 0, maxLessThan = 0; // TODO: use Apache commons to find out the min max : http://stackoverflow.com/questions/1484347/finding-the-max-min-value-in-an-array-of-primitives-using-java

        List<Column> columns = viewTable.getColumns();
//        Set keySet = dataJson.keySet();
//        Iterator dataIter = keySet.iterator();
        String tempUserId = "";
        String insertQueryToView = "";
        try {
            // Check whether the record exists or not
            // Assumption: The primary key is 1 for table emp.
            // TODO: Make the primary value configurable in the view config file.
//            List<Row> results = CassandraClientUtilities.getAllRows(request.getViewTable().getKeySpace(), request.getViewTable().getName(), QueryBuilder.eq("k", 1));
//            logger.debug("Response for getResultSet: {} ", results);
//            logger.debug("Response for getResultSet size: {} ", results.size());

            int constraintmaxGreater = 0;
            int constraintmaxLess = 0;

            for (int i = 0; i < columns.size(); i++) {
                Column tempCol = columns.get(i);
                if (tempCol.getName().equalsIgnoreCase("k")) {

                } else if (tempCol.getName().equalsIgnoreCase("max_view1_age")) {

                } else if (tempCol.getName().equalsIgnoreCase("max_view2_age")) {
                    String constraintArr[] = tempCol.getConstraint().split(" ");
                    constraintmaxGreater = Integer.parseInt(constraintArr[1]);
                } else if (tempCol.getName().equalsIgnoreCase("max_view3_age")) {
                    String constraintArr[] = tempCol.getConstraint().split(" ");
                    constraintmaxLess = Integer.parseInt(constraintArr[1]);
                }
            }

//            logger.debug("Constraints: constraintmaxGreater={} constraintmaxLess={}", constraintmaxGreater, constraintmaxLess);

            // Getting all the rows from the base table
            logger.debug("keyspace: {}, basetable name: {} ", request.getBaseTableKeySpace(), request.getBaseTableName());
            List<Row> rowsBaseTable = CassandraClientUtilities.getAllRows(request.getBaseTableKeySpace(), request.getBaseTableName(), null);
//            logger.debug("Basetable: Response for getResultSet: {} ", rowsBaseTable);
//            logger.debug("Basetable: Response for getResultSet size: {} ", rowsBaseTable.size());
            if (rowsBaseTable.size() > 0) {
                for (Row baseTableRow : rowsBaseTable) {
                    int tempAge = baseTableRow.getInt("age");
//                    logger.debug("Insert trigger | value of age " + tempAge);
                    if (maxAll < tempAge) {
                        maxAll = tempAge;
                    }
//                    logger.debug("******* View table insertion query : " + maxAll);
                    if (tempAge > constraintmaxGreater) {
                        if (maxGreaterThan < tempAge) {
                            maxGreaterThan = tempAge;
                        }
//                        logger.debug("******* View table insertion query : " + maxGreaterThan);
                    }
                    if (tempAge < constraintmaxLess) {
                        if (tempAge < constraintmaxGreater) {
                            if (maxLessThan < tempAge) {
                                maxLessThan = tempAge;
                            }
                        }
//                        logger.debug("******* View table insertion query : " + maxLessThan);
                    }
                }
                insertQueryToView = "insert into " + request.getViewTable().getKeySpace() +
                        "." + request.getViewTable().getName() + " ( k, max_view1_age, max_view2_age, " +
                        "max_view3_age ) values ( 1, " + maxAll + ", " + maxGreaterThan + ", " +
                        maxLessThan + " )";

            } else {
                insertQueryToView = "insert into " + request.getViewTable().getKeySpace() +
                        "." + request.getViewTable().getName() + " ( k, max_view1_age, max_view2_age, " +
                        "max_view3_age ) values ( 1, 0, 0, 0 )";
            }


            logger.debug("******* View table insertion query : " + insertQueryToView);
            isResultSuccessful = CassandraClientUtilities.commandExecution("localhost", insertQueryToView);

        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("Error !!! Stacktrace:" + ViewMaintenanceUtilities.getStackTrace(e));
        }
        response.setIsSuccess(isResultSuccessful);
        return response;
    }

    @Override
    public TriggerResponse updateTrigger(TriggerRequest request) {
        logger.debug("**********Inside Max Update Trigger for view maintenance**********");
        TriggerResponse response = insertTrigger(request);
        return response;
    }

    @Override
    public TriggerResponse deleteTrigger(TriggerRequest request) {
        logger.debug("**********Inside Max Update Trigger for view maintenance**********");
        TriggerResponse response = insertTrigger(request);
        return response;
    }
}
