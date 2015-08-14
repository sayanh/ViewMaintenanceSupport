package de.tum.viewmaintenance.trigger;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import de.tum.viewmaintenance.view_table_structure.Column;
import org.apache.cassandra.config.ColumnDefinition;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by shazra on 7/4/15.
 */
public class DeltaViewTrigger extends TriggerProcess {
    private static final Logger logger = LoggerFactory.getLogger(DeltaViewTrigger.class);
    public static final String DELTAVIEW_SUFFIX = "_deltaview";
    public static final String CURRENT = "_cur";
    public static final String LAST = "_last";

    /*
    *   This method is supposed to maintain delta view for all the tables for an insert operation.
    */
    @Override
    public TriggerResponse insertTrigger(TriggerRequest request) {
        logger.debug("---------- Inside Insert DeltaViewTrigger ----------");
        TriggerResponse response = new TriggerResponse();
        try {
            LinkedTreeMap dataMap = request.getDataJson();
            Set keySet = dataMap.keySet();
            Iterator dataIter = keySet.iterator();

            String baseTableName = request.getBaseTableName();
            String baseTableKeySpace = request.getBaseTableKeySpace();
            Map<String, Column> columnMapBaseTable = new HashMap<>();
            String primaryKey = "";
            String primaryKeyCassandraDataType = "";

            Map<String, ColumnDefinition> tableStrucMap = ViewMaintenanceUtilities.getTableDefinitition(baseTableKeySpace, baseTableName);

            for (Map.Entry<String, ColumnDefinition> entry : tableStrucMap.entrySet()) {
                String columnName = entry.getKey();
                ColumnDefinition columnDefinition = entry.getValue();
                logger.debug("Column name in the map= " + columnName);
                while (dataIter.hasNext()) {
                    String columnNameData = (String) dataIter.next();

//                    logger.debug("Column name in the data: " + columnNameData);
//                    logger.debug("Value: " + dataMap.get(columnNameData));
                    if (columnName.equalsIgnoreCase(columnNameData)) {
//                        logger.debug("Matched!!");
                        Column col = new Column();
                        col.setName(columnName);
                        col.setDataType(columnDefinition.type + "");

                        col.setIsPrimaryKey(columnDefinition.isPartitionKey());
                        if (columnDefinition.isPartitionKey()) {
                            primaryKey = columnName;
                            primaryKeyCassandraDataType = columnDefinition.type + "";
                        }
                        String corrJavaClass = ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnDefinition.type + "");
                        col.setJavaDataType(corrJavaClass);

                        if (corrJavaClass != null && !"".equalsIgnoreCase(corrJavaClass)) {
                            if ("Integer".equalsIgnoreCase(corrJavaClass)) {
                                col.setValue(Integer.parseInt((String) dataMap.get(columnNameData)));
                            } else if ("String".equalsIgnoreCase(corrJavaClass)) {
                                col.setValue(((String) dataMap.get(columnNameData)));
                            }
                        }

                        columnMapBaseTable.put(columnName, col);
                        break;
                    }
                }
            }

            // Making a select query to delta view to check whether the record for the primary key already exists

            Statement selectQuery = QueryBuilder.select().from(request.getBaseTableKeySpace(),
                    request.getBaseTableName() + DELTAVIEW_SUFFIX)
                    .where(QueryBuilder.eq(primaryKey, columnMapBaseTable.get(primaryKey).getValue()));


            logger.debug("Deltaview | insertTrigger | selectQuery : " + selectQuery);
            List<Row> existingRecordList = CassandraClientUtilities.commandExecution("localhost", selectQuery);
            logger.debug("Result | existing row | " + existingRecordList);

            if (existingRecordList.size() > 0) {
                // Update the delta table with the latest information for this key.
                updateExistingRow(existingRecordList.get(0), request, columnMapBaseTable, primaryKey);

            } else {
                // Insert a new row for this key.
                insertNewRow(request, columnMapBaseTable, primaryKey);
            }


            // Getting the row in the delta table which was inserted or updated

            String javaTypePrimaryKey = columnMapBaseTable.get(primaryKey).getJavaDataType();
            logger.debug("Computed Java type for extracted value {} is {}", primaryKeyCassandraDataType, javaTypePrimaryKey);

            Row existingRecordInDeltaView = CassandraClientUtilities.getAllRows(request.getBaseTableKeySpace(),
                    request.getBaseTableName() + DELTAVIEW_SUFFIX, QueryBuilder.eq(primaryKey,
                            columnMapBaseTable.get(primaryKey).getValue())).get(0);

            logger.debug("Inserted/Updated Record which for the entered primary key = " + existingRecordInDeltaView);
            response.setDeltaViewUpdatedRow(existingRecordInDeltaView);

            response.setIsSuccess(true);
        } catch (Exception e) {
            logger.error("Error !!" + CassandraClientUtilities.getStackTrace(e));
        }
        return response;
    }


    /*
    * This method is supposed to update an existing row in the delta view
    *
    */
    private static void updateExistingRow(Row existingRow, TriggerRequest request, Map<String, Column> currentDataMap, String primaryKey) {
//        Statement updateQuery = QueryBuilder.update(request.getBaseTableKeySpace(), request.getBaseTableName() + DELTAVIEW_SUFFIX)
//                .with(QueryBuilder.set())

        StringBuffer updateQuery = new StringBuffer("update " + request.getBaseTableKeySpace() + "."
                + request.getBaseTableName() + DELTAVIEW_SUFFIX + " set ");

        for (Map.Entry<String, Column> entry : currentDataMap.entrySet()) {
            if (!entry.getValue().isPrimaryKey()) {
                updateQuery.append(entry.getKey() + CURRENT + " = " + entry.getValue().getValue() + ", ");
                if (entry.getValue().getJavaDataType().equalsIgnoreCase("Integer")) {
                    updateQuery.append(entry.getKey() + LAST + " = " +
                            getStringBasedOnType(entry.getValue().getJavaDataType(),
                                    existingRow.getInt(entry.getKey() + CURRENT) + "", false) + ", ");

                } else if (entry.getValue().getJavaDataType().equalsIgnoreCase("String")) {
                    updateQuery.append(entry.getKey() + LAST + " = " +
                            getStringBasedOnType(entry.getValue().getJavaDataType(),
                                    existingRow.getString(entry.getKey() + CURRENT) + "", false) + ", ");
                }
            }
        }
        if (updateQuery.lastIndexOf(", ") == updateQuery.length() - 2) {
            updateQuery.delete(updateQuery.length() - 2, updateQuery.length());
        }

        updateQuery.append(" where " + currentDataMap.get(primaryKey).getName() + " = "
                + getStringBasedOnType(currentDataMap.get(primaryKey).getJavaDataType(),
                currentDataMap.get(primaryKey).getValue() + "", true));

        logger.debug("updateExistingRow : " + updateQuery.toString());
        CassandraClientUtilities.commandExecution("localhost", updateQuery.toString());
    }

    /**
    * This method is supposed to insert a new row in the delta view
    *
    **/

    // TODO: primaryKey is never used.
    private static void insertNewRow(TriggerRequest request, Map<String, Column> currentDataMap, String primaryKey) {

        StringBuffer insertQuery = new StringBuffer("insert into " + request.getBaseTableKeySpace()
                + "." + request.getBaseTableName() + DELTAVIEW_SUFFIX + " ( ");
        StringBuffer valuesPartInsertQuery = new StringBuffer(" values ( ");

        for (Map.Entry<String, Column> entry : currentDataMap.entrySet()) {
            if (entry.getValue().isPrimaryKey()) {
                insertQuery.append(entry.getKey() + ", ");
                valuesPartInsertQuery.append(getStringBasedOnType(entry.getValue()
                        .getJavaDataType(), entry.getValue().getValue() + "", true) + ", ");
            } else {
                insertQuery.append(entry.getKey() + CURRENT + ", ");
                valuesPartInsertQuery.append(getStringBasedOnType(entry.getValue()
                        .getJavaDataType(), entry.getValue().getValue() + "", true) + ", ");
            }
        }

        if (insertQuery.lastIndexOf(", ") == insertQuery.length() - 2) {
            insertQuery.delete(insertQuery.length() - 2, insertQuery.length());
        }

        if (valuesPartInsertQuery.lastIndexOf(", ") == valuesPartInsertQuery.length() - 2) {
            valuesPartInsertQuery.delete(valuesPartInsertQuery.length() - 2, valuesPartInsertQuery.length());
        }

        insertQuery.append(") " + valuesPartInsertQuery + " ) ");

        logger.debug("insertNewRow : " + insertQuery);
        CassandraClientUtilities.commandExecution("localhost", insertQuery.toString());

    }


    private static String getStringBasedOnType(String type, String targetString, boolean isCurr) {
        if (type.equalsIgnoreCase("Integer")) {
            return targetString;
        } else if (type.equalsIgnoreCase("String")) {
            if (isCurr) {
                return targetString;
            } else {
                return "'" + targetString + "'";
            }
        }

        return "";
    }

    // TODO
    @Override
    public TriggerResponse updateTrigger(TriggerRequest request) {
        return null;
    }


    public TriggerResponse updateTriggerOld(TriggerRequest request) {
        logger.debug("---------- Inside Update DeltaViewTrigger ----------");
        TriggerResponse response = new TriggerResponse();
        boolean isSuccess = false;
        try {

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

            List<Row> results = CassandraClientUtilities.getAllRows(request.getBaseTableKeySpace(), request.getBaseTableName() + DELTAVIEW_SUFFIX, QueryBuilder.eq("user_id", Integer.parseInt(tempUserId)));
            logger.debug("Existing record = " + results);

            Row existingRecord = results.get(0);
            String colaggkey_x_last = existingRecord.getString("colaggkey_x_cur");
            int age_last = existingRecord.getInt("age_cur");

            // TODO: Get the column name dynamically from the table description of Cassandra.
            StringBuffer updateQueryToDeltaView = new StringBuffer("update " + request.getBaseTableKeySpace() + "." + request.getBaseTableName() + DELTAVIEW_SUFFIX + " set ");
            if (changedFields.contains("age")) {
                updateQueryToDeltaView.append("age_cur=" + age + ", age_last=" + age_last + ",");
            }
            if (changedFields.contains("colaggkey_x")) {
                updateQueryToDeltaView.append("colaggkey_x_cur=" + colAggKey + ", colaggkey_x_last='" + colaggkey_x_last + "',");
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
        logger.debug("---------- Inside Delete DeltaViewTrigger ----------");
        TriggerResponse response = new TriggerResponse();
        try {
            String baseTableKeySpace = request.getBaseTableKeySpace();
            String baseTableName = request.getBaseTableName();
            Map<String, ColumnDefinition> tableStrucMap = ViewMaintenanceUtilities.getTableDefinitition(baseTableKeySpace, baseTableName);
            String primaryKeyValue = "";
            String primaryKeyName = "";
            String whereString = request.getWhereString().replace("where ", "");
            logger.debug("The where string = " + whereString);

            String whereStrArr[] = whereString.trim().split(" ");
            if (whereStrArr.length == 3) {
                primaryKeyValue = whereStrArr[2];
                primaryKeyName = whereStrArr[0];
            }
            Statement deleteQuery = null;
            String javaTypePrimaryKey = ViewMaintenanceUtilities.getJavaTypeFromCassandraType(tableStrucMap.get(primaryKeyName).type + "");
            logger.debug("Computed Java type for extracted value {} is {}", tableStrucMap.get(primaryKeyName), javaTypePrimaryKey);

            Row existingRecordInDeltaView = null;

            if (javaTypePrimaryKey.equalsIgnoreCase("Integer")) {
                existingRecordInDeltaView = CassandraClientUtilities.getAllRows(request.getBaseTableKeySpace(),
                        request.getBaseTableName() + DELTAVIEW_SUFFIX, QueryBuilder.eq(primaryKeyName,
                                Integer.parseInt(primaryKeyValue))).get(0);
            } else if (javaTypePrimaryKey.equalsIgnoreCase("String")) {
                existingRecordInDeltaView = CassandraClientUtilities.getAllRows(request.getBaseTableKeySpace(),
                        request.getBaseTableName() + DELTAVIEW_SUFFIX, QueryBuilder.eq(primaryKeyName,
                                primaryKeyValue)).get(0);
            }

            logger.debug("Record to be deleted = " + existingRecordInDeltaView);
            response.setDeletedRowFromDeltaView(existingRecordInDeltaView);

            if (tableStrucMap.get(primaryKeyName).isPartitionKey()
                    && javaTypePrimaryKey.equalsIgnoreCase("Integer")) {
                deleteQuery = QueryBuilder.delete().from(baseTableKeySpace, baseTableName + DELTAVIEW_SUFFIX)
                        .where(QueryBuilder.eq(primaryKeyName, Integer.parseInt(primaryKeyValue)));
            } else if (tableStrucMap.get(primaryKeyName).isPartitionKey()
                    && javaTypePrimaryKey.equalsIgnoreCase("String")) {
                deleteQuery = QueryBuilder.delete().from(baseTableKeySpace, baseTableName + DELTAVIEW_SUFFIX)
                        .where(QueryBuilder.eq(primaryKeyName, primaryKeyValue));
            }

            logger.debug(" DeleteQuery to Delta View: " + deleteQuery);
            CassandraClientUtilities.commandExecution("localhost", deleteQuery);
            response.setIsSuccess(true);
        } catch (Exception e) {
            logger.error(" Error !!!" + CassandraClientUtilities.getStackTrace(e));
        }
        return  response;
    }
}
