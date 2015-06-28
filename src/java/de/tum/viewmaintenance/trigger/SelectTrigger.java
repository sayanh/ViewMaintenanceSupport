package de.tum.viewmaintenance.trigger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.ConstraintsTypes;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.view_table_structure.Column;
import de.tum.viewmaintenance.view_table_structure.Table;
import de.tum.viewmaintenance.view_table_structure.Views;
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

    @Override
    public TriggerResponse insertTrigger(TriggerRequest request) {
        // vt1 -> This view performs select
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
        Cluster cluster = CassandraClientUtilities.getConnection("localhost");
        isResultSuccessful = CassandraClientUtilities.commandExecution(cluster, query.toString());
        CassandraClientUtilities.closeConnection(cluster);
        TriggerResponse response = new TriggerResponse();
        response.setIsSuccess(true);
        response.setIsSuccess(isResultSuccessful);
        return response;
    }

    @Override
    public TriggerResponse updateTrigger(TriggerRequest request) {
        // vt1 -> This view performs select
        logger.debug("**********Inside Select Update Trigger for view maintenance**********");
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
            query.append(tempCol.getName() + ", ");
            if (tempCol.getName().equalsIgnoreCase("select_view1_age")) {
                // No constraint case
                if (i == columns.size() - 1) {
                    valuesPartQuery.append(age);
                    query.append(tempCol.getName());
                } else {
                    valuesPartQuery.append(age + ", ");
                    query.append(tempCol.getName() + ", ");
                }
            } else if (tempCol.getName().equalsIgnoreCase("select_view2_age")) {
                // greater than case
                String constraintArr[] = tempCol.getConstraint().split(" ");
                if (constraintArr.length == 2) {
                    if (constraintArr[0].equalsIgnoreCase(ConstraintsTypes.Constraint.
                            getValue(ConstraintsTypes.Constraint.GREATER_THAN))) {
                        int constraintNum = Integer.parseInt(constraintArr[1]);
                        if (age > constraintNum) {
                            if (i == columns.size() - 1) {
                                valuesPartQuery.append(age);
                                query.append(tempCol.getName());
                            } else {
                                valuesPartQuery.append(age + ", ");
                                query.append(tempCol.getName() + ", ");
                            }
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
                            if (i == columns.size() - 1) {
                                valuesPartQuery.append(age);
                                query.append(tempCol.getName());
                            } else {
                                valuesPartQuery.append(age + ", ");
                                query.append(tempCol.getName() + ", ");
                            }
                        }
                    }
                }

            } else if (tempCol.getName().equalsIgnoreCase("k")) {
                // primary key case
                if (i == columns.size() - 1) {
                    query.append(tempCol.getName());
                    valuesPartQuery.append(tempUserId);

                } else {
                    query.append(tempCol.getName() + ", ");
                    valuesPartQuery.append(tempUserId + ", ");
                }
            }
        }


        //TODO: Run time construction of the query by checking for the table structure and primary key

        query.append(" ) " + valuesPartQuery.toString() + " )");
        System.out.println("Query : " + query.toString());
        Cluster cluster = CassandraClientUtilities.getConnection("localhost");
        isResultSuccessful = CassandraClientUtilities.commandExecution(cluster, query.toString());
        CassandraClientUtilities.closeConnection(cluster);
        TriggerResponse response = new TriggerResponse();
        response.setIsSuccess(isResultSuccessful);
        return response;
    }

    @Override
    public TriggerResponse deleteTrigger(TriggerRequest request) {
        // vt1 -> This view performs select
        logger.debug("**********Inside Select Delete Trigger for view maintenance**********");
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
            query.append(tempCol.getName() + ", ");
            if (tempCol.getName().equalsIgnoreCase("select_view1_age")) {
                // No constraint case
                if (i == columns.size() - 1) {
                    valuesPartQuery.append(age);
                    query.append(tempCol.getName());
                } else {
                    valuesPartQuery.append(age + ", ");
                    query.append(tempCol.getName() + ", ");
                }
            } else if (tempCol.getName().equalsIgnoreCase("select_view2_age")) {
                // greater than case
                String constraintArr[] = tempCol.getConstraint().split(" ");
                if (constraintArr.length == 2) {
                    if (constraintArr[0].equalsIgnoreCase(ConstraintsTypes.Constraint.
                            getValue(ConstraintsTypes.Constraint.GREATER_THAN))) {
                        int constraintNum = Integer.parseInt(constraintArr[1]);
                        if (age > constraintNum) {
                            if (i == columns.size() - 1) {
                                valuesPartQuery.append(age);
                                query.append(tempCol.getName());
                            } else {
                                valuesPartQuery.append(age + ", ");
                                query.append(tempCol.getName() + ", ");
                            }
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
                            if (i == columns.size() - 1) {
                                valuesPartQuery.append(age);
                                query.append(tempCol.getName());
                            } else {
                                valuesPartQuery.append(age + ", ");
                                query.append(tempCol.getName() + ", ");
                            }
                        }
                    }
                }

            } else if (tempCol.getName().equalsIgnoreCase("k")) {
                // primary key case
                if (i == columns.size() - 1) {
                    query.append(tempCol.getName());
                    valuesPartQuery.append(tempUserId);

                } else {
                    query.append(tempCol.getName() + ", ");
                    valuesPartQuery.append(tempUserId + ", ");
                }
            }
        }


        //TODO: Run time construction of the query by checking for the table structure and primary key

        query.append(" ) " + valuesPartQuery.toString() + " )");
        System.out.println("Query : " + query.toString());
        Cluster cluster = CassandraClientUtilities.getConnection("localhost");
        isResultSuccessful = CassandraClientUtilities.commandExecution(cluster, query.toString());
        CassandraClientUtilities.closeConnection(cluster);
        TriggerResponse response = new TriggerResponse();
        response.setIsSuccess(isResultSuccessful);
        return response;
    }
}
