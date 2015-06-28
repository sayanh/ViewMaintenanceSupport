package de.tum.viewmaintenance.trigger;

import com.datastax.driver.core.ResultSet;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClient;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.ConstraintsTypes;
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
        LinkedTreeMap dataJson = request.getDataJson();
        boolean isResultSuccessful = false;
        Table table = request.getViewTable();
        StringBuilder query = new StringBuilder();
        StringBuilder valuesPartQuery = new StringBuilder("values ( ");
        int countAll = 0, countGreaterThan = 0, countLessThan = 0;

        List<Column> columns = table.getColumns();
        Set keySet = dataJson.keySet();
        Iterator dataIter = keySet.iterator();
        String tempUserId = "";
        int age = 0;

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

        // Check whether the record exists or not
        // Assumption: The primary key is 1 for table emp.
        // TODO: Make the primary value configurable in the view config file.
        String selectQuery = "select * from " + request.getViewTable().getKeySpace() + "." + request.getViewTable().getName() + " where k = 1";
        ResultSet resultSet = CassandraClientUtilities.getResultSet("localhost", selectQuery);
        if (resultSet.all().size() > 0) {
            logger.debug("Size of the resultSet={}", resultSet.all().size());
            countAll = resultSet.one().getInt(1);
            countGreaterThan = resultSet.one().getInt(2);
            countLessThan = resultSet.one().getInt(3);
            logger.debug("Response after select query: countAll={}, countGreaterThan={}, countLessThan={}", countAll, countGreaterThan, countLessThan);
            query.append("update " + table.getKeySpace() + "." + table.getName() +
                    " set ");
            for (int i = 0; i < columns.size(); i++) {
                Column tempCol = columns.get(i);
                if (tempCol.getName().equalsIgnoreCase("k")) {
                    query.append(tempCol.getName() + " = " + "1, ");

                } else if (tempCol.getName().equalsIgnoreCase("count_view1_age")) {
                    countAll = +1;
                    query.append(tempCol.getName() + "=" + countAll + ", ");

                } else if (tempCol.getName().equalsIgnoreCase("count_view2_age")) {
                    String constraintArr[] = tempCol.getConstraint().split(" ");
                    int constraintNum = Integer.parseInt(constraintArr[1]);
                    if (constraintArr.length == 2) {
                        if (constraintArr[0].equalsIgnoreCase(
                                ConstraintsTypes.Constraint.getValue(
                                        ConstraintsTypes.Constraint.
                                                GREATER_THAN))) {
                            if (age > constraintNum) {
                                countGreaterThan += 1;
                            }
                        }
                    }
                    query.append(tempCol.getName() + " = " + countGreaterThan + ", ");

                } else if (tempCol.getName().equalsIgnoreCase("count_view3_age")) {
                    query.append(tempCol.getName() + ", ");
                    String constraintArr[] = tempCol.getConstraint().split(" ");
                    int constraintNum = Integer.parseInt(constraintArr[1]);
                    if (constraintArr.length == 2) {
                        if (constraintArr[0].equalsIgnoreCase(
                                ConstraintsTypes.Constraint.getValue(
                                        ConstraintsTypes.Constraint.
                                                LESS_THAN))) {
                            if (age < constraintNum) {
                                countLessThan += 1;
                            }
                        }
                    }
                    query.append(tempCol.getName() + " = " + countLessThan + ", ");
                }
            }
            if (query.lastIndexOf(", ") == query.length() - 2) {
                query.delete(query.length() - 2, query.length());
            }

        } else {
            logger.debug("There is no existing record!!");
            query.append("insert into " + table.getKeySpace() + "." + table.getName()
                    + " (");

            for (int i = 0; i < columns.size(); i++) {
                Column tempCol = columns.get(i);
                if (tempCol.getName().equalsIgnoreCase("k")) {
                    query.append(tempCol.getName() + ", ");
                    valuesPartQuery.append("1, ");

                } else if (tempCol.getName().equalsIgnoreCase("count_view1_age")) {
                    countAll+=1;
                    query.append(tempCol.getName() + ", ");
                    valuesPartQuery.append(countAll + ", ");

                } else if (tempCol.getName().equalsIgnoreCase("count_view2_age")) {
                    String constraintArr[] = tempCol.getConstraint().split(" ");
                    int constraintNum = Integer.parseInt(constraintArr[1]);
                    if (constraintArr.length == 2) {
                        if (constraintArr[0].equalsIgnoreCase(
                                ConstraintsTypes.Constraint.getValue(
                                        ConstraintsTypes.Constraint.
                                                GREATER_THAN))) {
                            if (age > constraintNum) {
                                countGreaterThan+=1;
                            }
                        }
                    }
                    query.append(tempCol.getName() + ", ");
                    valuesPartQuery.append(countGreaterThan + ", ");

                } else if (tempCol.getName().equalsIgnoreCase("count_view3_age")) {
                    String constraintArr[] = tempCol.getConstraint().split(" ");
                    int constraintNum = Integer.parseInt(constraintArr[1]);
                    if (constraintArr.length == 2) {
                        if (constraintArr[0].equalsIgnoreCase(
                                ConstraintsTypes.Constraint.getValue(
                                        ConstraintsTypes.Constraint.
                                                LESS_THAN))) {
                            if (age < constraintNum) {
                                countLessThan+=1;
                            }
                        }
                    }
                    query.append(tempCol.getName() + ", ");
                    valuesPartQuery.append(countLessThan + ", ");

                }
            }
            if (valuesPartQuery.lastIndexOf(", ") == valuesPartQuery.length() - 2) {
                valuesPartQuery.delete(valuesPartQuery.length() - 2, valuesPartQuery.length());
            }

            if (query.lastIndexOf(", ") == query.length() - 2) {
                query.delete(query.length() - 2, query.length());
            }

            query.append(" ) " + valuesPartQuery.toString() + " )");
        }


        logger.debug("******* Query : " + query.toString());
        isResultSuccessful = CassandraClientUtilities.commandExecution("localhost", query.toString());

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
        return null;
    }

    /*
    *
    * This method is triggered when an delete query is made by a client.
    * It ensures that the count views are consistent.
    *
    */
    @Override
    public TriggerResponse deleteTrigger(TriggerRequest request) {
        return null;
    }
}
