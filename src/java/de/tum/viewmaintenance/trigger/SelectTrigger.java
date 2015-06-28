package de.tum.viewmaintenance.trigger;

import com.google.gson.internal.LinkedTreeMap;
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
public class SelectTrigger extends TriggerProcess {
    private static final Logger logger = LoggerFactory.getLogger(SelectTrigger.class);

    /*
    *
    * This method is triggered when an insert query is made by a client.
    * It ensures that the select views are consistent.
    *
    */
    @Override
    public TriggerResponse insertTrigger(TriggerRequest request) {
        // vt1 -> This view performs "select"
        logger.debug("**********Inside Select Insert Trigger for view maintenance**********");
        LinkedTreeMap dataJson = request.getDataJson();
        boolean isResultSuccessful = false;
        Table table = request.getViewTable();
        StringBuilder query = new StringBuilder("Insert into " + request.getKeyspace() + "." + table.getName() + " ( ");
        StringBuilder valuesPartQuery = new StringBuilder("values ( ");
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

        // Traversing the view table based on its columns
        for (int i = 0; i < columns.size(); i++) {
            Column tempCol = columns.get(i);
            if (tempCol.getName().equalsIgnoreCase("select_view1_age")) {
                // No constraint case
                valuesPartQuery.append(age + ", ");
                query.append(tempCol.getName() + ", ");
            } else if (tempCol.getName().equalsIgnoreCase("select_view2_age")) {
                // greater than case
                String constraintArr[] = tempCol.getConstraint().split(" ");
                if (constraintArr.length == 2) {
                    if (constraintArr[0].equalsIgnoreCase(ConstraintsTypes.Constraint.
                            getValue(ConstraintsTypes.Constraint.GREATER_THAN))) {
                        int constraintNum = Integer.parseInt(constraintArr[1]);
                        if (age > constraintNum) {
                            valuesPartQuery.append(age + ", ");
                            query.append(tempCol.getName() + ", ");
                        } else {
                            query.append(tempCol.getName() + ", ");
                            valuesPartQuery.append("null" + ", ");
                        }
                    }
                }


            } else if (tempCol.getName().equalsIgnoreCase("select_view3_age")) {
                // less than case

                String constraintArr[] = tempCol.getConstraint().split(" ");
                if (constraintArr.length == 2) {
                    if (constraintArr[0].equalsIgnoreCase(ConstraintsTypes.Constraint.
                            getValue(ConstraintsTypes.Constraint.LESS_THAN))) {
                        int constraintNum = Integer.parseInt(constraintArr[1]);
                        if (age < constraintNum) {
                            valuesPartQuery.append(age + ", ");
                            query.append(tempCol.getName() + ", ");
                        } else {
                            query.append(tempCol.getName() + ", ");
                            valuesPartQuery.append("null" + ", ");
                        }
                    }
                }

            } else if (tempCol.getName().equalsIgnoreCase("k")) {
                // primary key case
                query.append(tempCol.getName() + ", ");
                valuesPartQuery.append(tempUserId + ", ");
            }
        }

        //TODO: Run time construction of the query by checking for the table structure and primary key
        if (valuesPartQuery.lastIndexOf(", ") == valuesPartQuery.length() - 2) {
            valuesPartQuery.delete(valuesPartQuery.length() - 2, valuesPartQuery.length());
        }

        if (query.lastIndexOf(", ") == query.length() - 2) {
            query.delete(query.length() - 2, query.length());
        }

        query.append(" ) " + valuesPartQuery.toString() + " )");
        logger.debug("******* Query : " + query.toString());
        isResultSuccessful = CassandraClientUtilities.commandExecution("localhost", query.toString());
        TriggerResponse response = new TriggerResponse();
        response.setIsSuccess(true);
        response.setIsSuccess(isResultSuccessful);
        return response;
    }

    /*
    *
    * This method is triggered when an update query is made by a client.
    * It ensures that the select views are consistent.
    *
    */

    @Override
    public TriggerResponse updateTrigger(TriggerRequest request) {
        // vt1 -> This view performs select
        logger.debug("**********Update Select Insert Trigger for view maintenance**********");
        LinkedTreeMap dataJson = request.getDataJson();
        boolean isResultSuccessful = false;
        Table table = request.getViewTable();
        StringBuilder query = new StringBuilder("update " + request.getKeyspace() + "." + table.getName() + " set ");
//        StringBuilder valuesPartQuery = new StringBuilder("values ( ");
        List<Column> columns = table.getColumns();
        Set keySet = dataJson.keySet();
        Iterator dataIter = keySet.iterator();
        String tempUserId = "";
        int age = 0;
        String whereString = request.getWhereString();
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

        // Traversing the view table based on its columns
        for (int i = 0; i < columns.size(); i++) {
            Column tempCol = columns.get(i);
            if (tempCol.getName().equalsIgnoreCase("select_view1_age")) {
                // No constraint case
                query.append(tempCol.getName() + " = " + age + ", ");
            } else if (tempCol.getName().equalsIgnoreCase("select_view2_age")) {
                // greater than case
                String constraintArr[] = tempCol.getConstraint().split(" ");
                if (constraintArr.length == 2) {
                    if (constraintArr[0].equalsIgnoreCase(ConstraintsTypes.Constraint.
                            getValue(ConstraintsTypes.Constraint.GREATER_THAN))) {
                        int constraintNum = Integer.parseInt(constraintArr[1]);
                        if (age > constraintNum) {
                            query.append(tempCol.getName() + " = " + age + ", ");
                        } else {
                            query.append(tempCol.getName() + " = null, ");
                        }
                    }
                }


            } else if (tempCol.getName().equalsIgnoreCase("select_view3_age")) {
                // less than case

                String constraintArr[] = tempCol.getConstraint().split(" ");
                if (constraintArr.length == 2) {
                    if (constraintArr[0].equalsIgnoreCase(ConstraintsTypes.Constraint.
                            getValue(ConstraintsTypes.Constraint.LESS_THAN))) {
                        int constraintNum = Integer.parseInt(constraintArr[1]);
                        if (age < constraintNum) {
                            query.append(tempCol.getName() + " = " + age + ", ");
                        } else {
                            query.append(tempCol.getName() + " = null, ");
                        }
                    }
                }

            }
        }

        //TODO: Run time construction of the query by checking for the table structure and primary key
        if (query.lastIndexOf(", ") == query.length() - 2) {
            query.delete(query.length() - 2, query.length());
        }

        // TODO: Need to configure primary keys for both base table and view table. Right now it is hardcoded.
        query.append(" " + whereString.replace("user_id", "k"));
        logger.debug("******* Query : " + query.toString());
        isResultSuccessful = CassandraClientUtilities.commandExecution("localhost", query.toString());
        TriggerResponse response = new TriggerResponse();
//        response.setIsSuccess(true);
        response.setIsSuccess(isResultSuccessful);
        return response;
    }



    /*
    *
    * This method is triggered when an delete query is made by a client.
    * It ensures that the select views are consistent.
    *
    */

    @Override
    public TriggerResponse deleteTrigger(TriggerRequest request) {
        //TODO: Need to implement delete trigger
        TriggerResponse response = null;

        return response;
    }



}
