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
 * Created by shazra on 6/27/15.
 */
public class SumTrigger extends TriggerProcess {
    // vt3 -> This view is meant for "sum"
    private static final Logger logger = LoggerFactory.getLogger(SumTrigger.class);
    private static final String DELTAVIEW_SUFFIX = "_deltaview";
    private static final String DELTAVIEW_SUFFIX_CURRENT = "_cur";
    private static final String DELTAVIEW_SUFFIX_LAST = "_last";

    /*
    *
    * This method is triggered when an insert query is made by a client.
    * It ensures that the sum views are consistent.
    *
    */
    @Override
    public TriggerResponse insertTrigger(TriggerRequest request) {

        logger.debug("**********Inside sum Insert Trigger for view maintenance**********");
        TriggerResponse response = new TriggerResponse();
        LinkedTreeMap dataMap = request.getDataJson();
        Table viewTable = request.getViewTable();
        List<Column> columns = viewTable.getColumns();
        Set keySet = dataMap.keySet();
        Iterator dataIter = keySet.iterator();
        String colAggKey = "";
        Row existingRecordDeltaView = null;
        String colAggKeyLastDeltaView = null;
        String tempUserId = "";
        int age = 0;
        boolean isIncrementQuerySucc = false;
        try {

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

            // Check whether the insert call is a new call or overwriting call
            // if the last field of the attribute in the delta view is NULL it is a new insert else it is a overwriting insert.

            List<Row> resultsDeltaTable = CassandraClientUtilities.getAllRows(request.getBaseTableKeySpace(), request.getBaseTableName() + DELTAVIEW_SUFFIX, QueryBuilder.eq("user_id", Integer.parseInt(tempUserId)));

            logger.debug("Existing record in delta view: " + resultsDeltaTable);
            existingRecordDeltaView = resultsDeltaTable.get(0);
            String colAggKeyCurDeltaView = existingRecordDeltaView.getString(request.getViewTable().getBasedOn() + DELTAVIEW_SUFFIX_CURRENT);
            colAggKeyLastDeltaView = existingRecordDeltaView.getString(request.getViewTable().getBasedOn() + DELTAVIEW_SUFFIX_LAST);
            if (colAggKeyLastDeltaView != null) {
                logger.debug(" Already present | overwriting insert ");


                if (colAggKeyCurDeltaView.equalsIgnoreCase(colAggKeyLastDeltaView)) {

                    if (existingRecordDeltaView.getInt("age_cur") == existingRecordDeltaView.getInt("age_last")) {
                        logger.debug(" Already sumed | nothing to do!!!");
                        response.setIsSuccess(true);
                        return response;
                    }

                    logger.debug(" Already sumed for sum all | need to check for other cols in views = {}.{}", request.getViewTable().getKeySpace(), request.getViewTable().getName());
                    List<Row> resultsViewtable = CassandraClientUtilities.getAllRows(request.getViewTable().getKeySpace(), request.getViewTable().getName(), QueryBuilder.eq("colaggkey_x", colAggKeyCurDeltaView));

                    logger.debug(" Results from view table | ", resultsViewtable);
                    logger.debug(" Size of results | ", resultsViewtable.size());
                    response.setIsSuccess(updateViewIfAggKeyAlreadyExists(age, request, columns, existingRecordDeltaView, resultsViewtable.get(0), true));
                    return response;
                } else {
                    response.setIsSuccess(handleOverwritingInsert(age, colAggKey, request, existingRecordDeltaView));
                    return response;
                }
            }


            // Check whether the entered colaggkey exists or not in the view table

            // TODO: Make the primary value configurable in the view config file.

            logger.debug("********************************************");
            logger.debug("Making query in table {}.{} with colaggkey = {} ", request.getViewTable().getKeySpace(), request.getViewTable().getName(), colAggKeyCurDeltaView);
            List<Row> resultsCurViewtable = CassandraClientUtilities.getAllRows(request.getViewTable().getKeySpace(), request.getViewTable().getName(), QueryBuilder.eq("colaggkey_x", colAggKeyCurDeltaView));


            logger.debug("Response for query using colaggkey_cur in view getResultSet: {} ", resultsCurViewtable);
            logger.debug("Response for query using colaggkey_cur in view getResultSet size: {} ", resultsCurViewtable.size());

            if (resultsCurViewtable.size() > 0) {
                // The colaggkey_x exists in the view table.
                // Update the view table by incrementing the sum
                isIncrementQuerySucc = updateViewIfAggKeyAlreadyExists(age, request, columns, existingRecordDeltaView, resultsCurViewtable.get(0), false);

            } else {
                // The colaggkey_x does not exist in the view table.
                // Insert a row with key colaggkey_x with sum as 1 in the view table.
                isIncrementQuerySucc = insertIntoViewIfAggKeyDoesNotExists(age, colAggKey, request, columns, resultsDeltaTable.get(0).getString("colaggkey_x_cur"));
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.debug("Error !!! Stacktrace: \n" + CassandraClientUtilities.getStackTrace(e));
        }
        response.setIsSuccess(isIncrementQuerySucc);
        return response;
    }

    private boolean decrementSumInView(int age, List<Column> viewTableCols, TriggerRequest request, Row resultsLastSumView) {
        boolean isDecrementQuerySucc = false;
        StringBuffer updateDecrementQuery = new StringBuffer("update " + request.getViewTable().getKeySpace() + "." +
                request.getViewTable().getName() + " set ");
        for (int i = 0; i < viewTableCols.size(); i++) {
            Column tempCol = viewTableCols.get(i);
            if (tempCol.getName().equalsIgnoreCase("colaggkey_x")) {

            } else if (tempCol.getName().equalsIgnoreCase("sum_view1_age")) {
                updateDecrementQuery.append(tempCol.getName() + " = " + (resultsLastSumView.getInt
                        (tempCol.getName()) - age) + ", ");

            } else if (tempCol.getName().equalsIgnoreCase("sum_view2_age")) {
                String constraintArr[] = tempCol.getConstraint().split(" ");
                int constraintSumGreater = Integer.parseInt(constraintArr[1]);
                if (age > constraintSumGreater) {
                    updateDecrementQuery.append(tempCol.getName() + " = " + (resultsLastSumView.getInt(tempCol.getName()) - age) + ", ");
                }
            } else if (tempCol.getName().equalsIgnoreCase("sum_view3_age")) {
                String constraintArr[] = tempCol.getConstraint().split(" ");
                int constraintSumLess = Integer.parseInt(constraintArr[1]);
                if (age < constraintSumLess) {
                    updateDecrementQuery.append(tempCol.getName() + " = " + (resultsLastSumView.getInt
                            (tempCol.getName()) - age) + ", ");
                }
            }
        }

        if (updateDecrementQuery.lastIndexOf(", ") == updateDecrementQuery.length() - 2) {
            updateDecrementQuery.delete(updateDecrementQuery.length() - 2, updateDecrementQuery.length());
        }
        updateDecrementQuery.append(" where colaggkey_x = '" + resultsLastSumView.getString("colaggkey_x") + "'");
        logger.debug("Decrement update query: {}", updateDecrementQuery.toString());
        isDecrementQuerySucc = CassandraClientUtilities.commandExecution("localhost", updateDecrementQuery.toString());

        return isDecrementQuerySucc;

    }

    private boolean updateViewIfAggKeyAlreadyExists(int age, TriggerRequest request, List<Column> viewTableCols, Row existingRecordDeltaView, Row existingRecordViewTable, boolean isCurAggKeyLastAggKeyColSame) {
        logger.debug("**********updateViewIfAggKeyAlreadyExists***********");
        // This is the case when the colAggKey of the entered query exists
        // Update by incrementing the sum for this key
        boolean isIncrementQuerySucc = false;
        int age_cur = existingRecordDeltaView.getInt("age_cur");
        int age_last = existingRecordDeltaView.getInt("age_last");
        String basedOn_deltaView_Cur = existingRecordDeltaView.getString(request.getViewTable().getBasedOn() + DELTAVIEW_SUFFIX_CURRENT);
        String basedOn_deltaView_Last = existingRecordDeltaView.getString(request.getViewTable().getBasedOn() + DELTAVIEW_SUFFIX_LAST);
        StringBuffer updateIncrementQuery = new StringBuffer("update " + request.getViewTable().getKeySpace() + "." + request.getViewTable().getName() + " set ");
        for (int i = 0; i < viewTableCols.size(); i++) {
            Column tempCol = viewTableCols.get(i);
            if (tempCol.getName().equalsIgnoreCase("colaggkey_x")) {

            } else if (tempCol.getName().equalsIgnoreCase("sum_view1_age")) {
                if (basedOn_deltaView_Cur.equalsIgnoreCase(basedOn_deltaView_Last)) {
                    if (age_cur != age_last) {
                        int diff_age = age_cur - age_last;
                        updateIncrementQuery.append(tempCol.getName() + " = " + (existingRecordViewTable.getInt(tempCol.getName()) + diff_age) + ", ");
                    }
                } else {
                    updateIncrementQuery.append(tempCol.getName() + " = " + (existingRecordViewTable.getInt(tempCol.getName()) + age) + ", ");

                }

            } else if (tempCol.getName().equalsIgnoreCase("sum_view2_age")) {
                String constraintArr[] = tempCol.getConstraint().split(" ");
                int constraintSumGreater = Integer.parseInt(constraintArr[1]);
                if ((age > constraintSumGreater && !isCurAggKeyLastAggKeyColSame) || (isCurAggKeyLastAggKeyColSame && age_last < constraintSumGreater && age_cur > constraintSumGreater)) {
                    updateIncrementQuery.append(tempCol.getName() + " = " + (existingRecordViewTable.getInt
                            (tempCol.getName()) + age) + ", ");
                } else if (isCurAggKeyLastAggKeyColSame && age_last > constraintSumGreater && age_cur < constraintSumGreater) {
                    updateIncrementQuery.append(tempCol.getName() + " = " + (existingRecordViewTable.getInt
                            (tempCol.getName()) - age_last) + ", ");
                } else if (isCurAggKeyLastAggKeyColSame && age_last > constraintSumGreater && age_cur > constraintSumGreater) {
                    if ( age_last != age_cur) {
                        int diff_age = age_cur - age_last;
                        updateIncrementQuery.append(tempCol.getName() + " = " + (existingRecordViewTable.getInt
                                (tempCol.getName()) + diff_age) + ", ");
                    }
                }

            } else if (tempCol.getName().equalsIgnoreCase("sum_view3_age")) {
                String constraintArr[] = tempCol.getConstraint().split(" ");
                int constraintSumLess = Integer.parseInt(constraintArr[1]);
                if ((age < constraintSumLess && !isCurAggKeyLastAggKeyColSame) || (isCurAggKeyLastAggKeyColSame && age_last > constraintSumLess && age_cur < constraintSumLess)) {
                    updateIncrementQuery.append(tempCol.getName() + " = " + (existingRecordViewTable.getInt
                            (tempCol.getName()) + age) + ", ");
                } else if (isCurAggKeyLastAggKeyColSame && age_last < constraintSumLess && age_cur > constraintSumLess) {
                    updateIncrementQuery.append(tempCol.getName() + " = " + (existingRecordViewTable.getInt
                            (tempCol.getName()) - age_last) + ", ");
                } else if (isCurAggKeyLastAggKeyColSame && age_last < constraintSumLess && age_cur < constraintSumLess) {
                    if ( age_last != age_cur) {
                        int diff_age = age_cur - age_last;
                        updateIncrementQuery.append(tempCol.getName() + " = " + (existingRecordViewTable.getInt
                                (tempCol.getName()) + diff_age) + ", ");
                    }
                }
            }
        }
        if (updateIncrementQuery.lastIndexOf(", ") == updateIncrementQuery.length() - 2) {
            updateIncrementQuery.delete(updateIncrementQuery.length() - 2, updateIncrementQuery.length());
        }
        updateIncrementQuery.append(" where colaggkey_x = '" + existingRecordDeltaView.getString(request.getViewTable().getBasedOn() + DELTAVIEW_SUFFIX_CURRENT) + "'");
        logger.debug(" Update increment query for overwriting case: {}", updateIncrementQuery.toString());
        isIncrementQuerySucc = CassandraClientUtilities.commandExecution("localhost", updateIncrementQuery.toString());
        return isIncrementQuerySucc;
    }


    private boolean insertIntoViewIfAggKeyDoesNotExists(int age, String colAggKey, TriggerRequest request, List<Column> viewTableCols, String colAggKeyDelta_cur) {
        // This is the case when the colAggKey of the entered query does not exist
        // Insert a new record with 1 for this key.

        boolean isIncrementQuerySucc = false;
        StringBuffer insertIncrementQuery = new StringBuffer("insert into " + request.getViewTable().getKeySpace() + "." + request.getViewTable().getName() + " ( ");
        StringBuffer valuesQuery = new StringBuffer("values ( ");
        for (int i = 0; i < viewTableCols.size(); i++) {
            Column tempCol = viewTableCols.get(i);
            if (tempCol.getName().equalsIgnoreCase("colaggkey_x")) {
                insertIncrementQuery.append(tempCol.getName() + ", ");
                valuesQuery.append("'" + colAggKeyDelta_cur + "', ");

            } else if (tempCol.getName().equalsIgnoreCase("sum_view1_age")) {
                insertIncrementQuery.append(tempCol.getName() + ", ");
                valuesQuery.append(age + ", ");

            } else if (tempCol.getName().equalsIgnoreCase("sum_view2_age")) {
                String constraintArr[] = tempCol.getConstraint().split(" ");
                int constraintsumGreater = Integer.parseInt(constraintArr[1]);
                if (age > constraintsumGreater) {
                    insertIncrementQuery.append(tempCol.getName() + ", ");
                    valuesQuery.append(age + ", ");
                } else {
                    insertIncrementQuery.append(tempCol.getName() + ", ");
                    valuesQuery.append("0, ");
                }
            } else if (tempCol.getName().equalsIgnoreCase("sum_view3_age")) {
                String constraintArr[] = tempCol.getConstraint().split(" ");
                int constraintsumLess = Integer.parseInt(constraintArr[1]);
                if (age < constraintsumLess) {
                    insertIncrementQuery.append(tempCol.getName() + ", ");
                    valuesQuery.append(age + ", ");
                } else {
                    insertIncrementQuery.append(tempCol.getName() + ", ");
                    valuesQuery.append("0, ");
                }
            }
        }
        if (insertIncrementQuery.lastIndexOf(", ") == insertIncrementQuery.length() - 2) {
            insertIncrementQuery.delete(insertIncrementQuery.length() - 2, insertIncrementQuery.length());
        }

        if (valuesQuery.lastIndexOf(", ") == valuesQuery.length() - 2) {
            valuesQuery.delete(valuesQuery.length() - 2, valuesQuery.length());
        }

        insertIncrementQuery.append(" ) " + valuesQuery.toString() + " ) ");
        logger.debug(" Insert increment query : {}", insertIncrementQuery.toString());
        isIncrementQuerySucc = CassandraClientUtilities.commandExecution("localhost", insertIncrementQuery.toString());

        return isIncrementQuerySucc;

    }

    private boolean handleOverwritingInsert(int age, String colAggKey, TriggerRequest request, Row existingRecordDeltaView) {
        logger.debug("--------- Inside handleOverwritingInsert ----------");
        boolean isResultSucc = false;
        boolean isIncrementQuerySucc = false;
        String colAggKeyDelta_cur = existingRecordDeltaView.getString("colaggkey_x_cur"); // Note: this is same as colAggKey entered in the query.
        String colAggKeyDelta_last = existingRecordDeltaView.getString("colaggkey_x_last");
        List<Column> columns = request.getViewTable().getColumns();
        // Query in the count view table "vt2"(here) to check whether the entered colAggKey exists or not
        List<Row> resultsCurSumView = CassandraClientUtilities.getAllRows(request.getViewKeyspace(), request.getViewTable().getName(),
                QueryBuilder.eq("colaggkey_x", colAggKeyDelta_cur));
        logger.debug("Results after querying sum table | " + resultsCurSumView);
        if (resultsCurSumView.size() > 0) {
            // This is the case when the colAggKey of the entered query exists
            // Update by incrementing the sum for this key

            isIncrementQuerySucc = updateViewIfAggKeyAlreadyExists(age, request, columns, existingRecordDeltaView, resultsCurSumView.get(0), false);

        } else {
            // This is the case when the colAggKey of the entered query does not exist
            // Insert a new record with 1 for this key.

            isIncrementQuerySucc = insertIntoViewIfAggKeyDoesNotExists(age, colAggKey, request, columns, colAggKeyDelta_cur);
        }


        // Logic to decrement the last value for the previous aggregate key

        Row resultsLastsumView = CassandraClientUtilities.getAllRows(request.getViewKeyspace(), request.getViewTable().getName(), QueryBuilder.eq("colaggkey_x", colAggKeyDelta_last)).get(0);

        boolean isDecrementQuerySucc = decrementSumInView(age, columns, request, resultsLastsumView);

        if (isDecrementQuerySucc && isIncrementQuerySucc) {
            isResultSucc = true;
        }
        return isResultSucc;
    }

    /*
    *
    * This method is triggered when an update query is made by a client.
    * It ensures that the sum views are consistent.
    *
    */
    @Override
    public TriggerResponse updateTrigger(TriggerRequest request) {
        logger.debug("**********Inside sum Update Trigger for view maintenance**********");
        TriggerResponse response = insertTrigger(request);
        return response;
    }

    /*
    *
    * This method is triggered when an delete query is made by a client.
    * It ensures that the sum views are consistent.
    *
    */
    @Override
    public TriggerResponse deleteTrigger(TriggerRequest request) {
        logger.debug("**********Inside sum Delete Trigger for view maintenance**********");
        Row rowDeletedDeltaView = request.getDeletedRowDeltaView();
        Table viewTable = request.getViewTable();
        TriggerResponse response = new TriggerResponse();
        String colAggKey_cur = rowDeletedDeltaView.getString("colaggkey_x_cur");
        List<Column> columns = viewTable.getColumns();
        int age = rowDeletedDeltaView.getInt("age_cur");
        Row resultsLastSumView = CassandraClientUtilities.getAllRows(request.getViewKeyspace(), request.getViewTable().getName(), QueryBuilder.eq("colaggkey_x", colAggKey_cur)).get(0);
        boolean isDecrementQuerySucc = decrementSumInView(age, columns, request, resultsLastSumView);
        response.setIsSuccess(isDecrementQuerySucc);

        return response;
    }
}
