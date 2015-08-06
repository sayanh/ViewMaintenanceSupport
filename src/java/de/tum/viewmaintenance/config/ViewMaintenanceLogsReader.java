package de.tum.viewmaintenance.config;

import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.trigger.*;
import de.tum.viewmaintenance.view_table_structure.Table;
import de.tum.viewmaintenance.view_table_structure.Views;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DataTracker;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by shazra on 6/19/15.
 * <p/>
 * This class is meant to read the commitlogsv2 and take actions to maintain the views based on the events.
 */
public class ViewMaintenanceLogsReader extends Thread {

    private static volatile ViewMaintenanceLogsReader instance;
    private static final String STATUS_FILE = "viewmaintenance_status.txt"; // Stores the operation_id last processed
    private static final String LOG_FILE = "viewMaintenceCommitLogsv2.log";
    private static final String LOG_FILE_LOCATION = System.getProperty("cassandra.home") + "/logs/";
    private static final Logger logger = LoggerFactory.getLogger(ViewMaintenanceLogsReader.class);
    private static final int SLEEP_INTERVAL = 10000;

    public ViewMaintenanceLogsReader() {
        logger.debug("************************ Starting view maintenance infrastructure set up ******************");
        ViewMaintenanceConfig.readViewConfigFromFile();
        ViewMaintenanceConfig.setupViewMaintenanceInfrastructure();
        this.start();

    }

    private static void updateStatusFile(String lastOpertationIdProcessed) throws IOException {
        logger.debug("Inside updateStatusFile");
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(System.getProperty("cassandra.home") + "/data/" + STATUS_FILE));
        bufferedWriter.write(lastOpertationIdProcessed + "");
        bufferedWriter.flush();
        bufferedWriter.close();
    }

    @Override
    public void run() {
        int lastOpertationIdProcessed;
        BufferedReader bufferedReader = null;
        logger.debug("Running the View Maintenance Engine outside true!!!");
        while (true) {
            try {
                File statusFile = new File(System.getProperty("cassandra.home") + "/data/" + STATUS_FILE);
                if (statusFile.exists()) {
                    lastOpertationIdProcessed = Integer.parseInt(new String(Files.readAllBytes(Paths.get(System.getProperty("cassandra.home") + "/data/" + STATUS_FILE))));
                } else {
                    lastOpertationIdProcessed = 0;
                }

                logger.debug("Running the View Maintenance Engine !!!");
                bufferedReader = new BufferedReader(new FileReader(LOG_FILE_LOCATION + LOG_FILE));
                String tempLine = "";
                List<String> linesCommitLog = new ArrayList<>();
                while ((tempLine = bufferedReader.readLine()) != null) {
                    linesCommitLog.add(tempLine);
                }

                //TODO: use this List<String> stringList = Files.readAllLines(Paths.get(filePath), Charset.defaultCharset());
                try {
                    if (bufferedReader != null) {
                        bufferedReader.close();
                    }
                } catch (IOException e) {
                    logger.error("Error !! Stacktrace: \n " + CassandraClientUtilities.getStackTrace(e));
                }
//                logger.debug("printing the list of tasks " + linesCommitLog);

                for (String lineActivity : linesCommitLog) {
                    JSONObject json;
                    Map<String, Object> retMap = new Gson().fromJson(lineActivity, new TypeToken<HashMap<String, Object>>() {
                    }.getType());
                    LinkedTreeMap dataJson = null;
                    String whereString = null;
                    String type = "";
                    int operation_id = 0;
                    String tableName = "";
                    TriggerRequest request = new TriggerRequest();


                    for (Map.Entry<String, Object> entry : retMap.entrySet()) {
                        String key = entry.getKey();
                        String value = "";
//                            logger.debug(key + "/" + entry.getValue());
                        if (key.equalsIgnoreCase("type")) {
                            type = (String) entry.getValue();
                            request.setType(type);
                        } else if (key.equalsIgnoreCase("operation_id")) {
                            operation_id = ((Double) entry.getValue()).intValue();
                        } else if (key.equalsIgnoreCase("data")) {
                            dataJson = (LinkedTreeMap) entry.getValue();
                            request.setDataJson(dataJson);
                        } else if (key.equalsIgnoreCase("table")) {
                            tableName = (String) entry.getValue();
                            if (tableName.contains(".")) {
                                String tableNameArr[] = tableName.split("\\.");
                                if (tableNameArr.length == 2) {
                                    request.setBaseTableKeySpace(tableNameArr[0]);
                                    request.setBaseTableName(tableNameArr[1]);
                                }
                            } else {
                                request.setBaseTableName(tableName);
                            }

                        } else if (key.equalsIgnoreCase("where")) {
//                                logger.debug("************** getting class ***************" + entry.getValue().getClass());
                            if (entry.getValue() instanceof String) {
                                whereString = (String) entry.getValue();
                                request.setWhereString(whereString);
                            }
                        }
                    }
                    if (lastOpertationIdProcessed < operation_id) {
                        // Action section
                        // Updating the delta view

                        DeltaViewTrigger deltaViewTrigger = new DeltaViewTrigger();
                        TriggerResponse deltaViewTriggerResponse = null;
                        if (type.equalsIgnoreCase("insert")) {
                            deltaViewTriggerResponse = deltaViewTrigger.insertTrigger(request);
                        } else if (type.equalsIgnoreCase("update")) {
                            deltaViewTriggerResponse = deltaViewTrigger.updateTrigger(request);
                        } else if (type.equalsIgnoreCase("delete")) {
                            deltaViewTriggerResponse = deltaViewTrigger.deleteTrigger(request);
                        }

                        if (!deltaViewTriggerResponse.isSuccess()) {
                            break;
                        }

                        Views views = Views.getInstance();
                        List<Table> tables = views.getTables();
                        TriggerProcess triggerProcess = null;
                        TriggerResponse triggerResponse = null;
                        request.setViewKeyspace(views.getKeyspace());

                        // Printing Views
                        logger.debug("************** Views **************");
                        logger.debug(" Is the view retrieved = " + views);
                        logger.debug(" View properties = keyspace:{} with tableslist size:{} ", views.getKeyspace(), views.getTables().size());


                        //TODO : Read from the configuration which base table should be used to fill the views

                        // Usecase: Reverse Join View: "schematest.salary" is the second table for join.
                        if (tableName.contains("salary")) {
                            for (int i = 0; i < tables.size(); i++) {
                                if (tables.get(i).getName().equalsIgnoreCase("vt5")) {
                                    request.setViewTable(tables.get(i));
                                    triggerProcess = new ReverseJoinViewTrigger();
                                    // Updating the reverse join view
                                    if ("insert".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.insertTrigger(request);
//                                        } else if ("update".equalsIgnoreCase(type)) {
//                                            triggerResponse = triggerProcess.updateTrigger(request);
                                    } else if ("delete".equalsIgnoreCase(type)) {
                                        request.setDeletedRowDeltaView(deltaViewTriggerResponse.getDeletedRowFromDeltaView());
                                        triggerResponse = triggerProcess.deleteTrigger(request);
                                    }
                                }
                            }
                            if (triggerResponse.isSuccess()) {
                                // If result is successful
                                // Update the lastOperationProcessed variable
                                updateStatusFile(operation_id + "");
                            }
                        }
                        if (tableName.contains("emp")) {
                            for (int i = 0; i < tables.size(); i++) {
                                if (tables.get(i).getName().equalsIgnoreCase("vt1")) {
                                    triggerProcess = new SelectTrigger();
                                    request.setViewTable(tables.get(i));
                                    if ("insert".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.insertTrigger(request);
                                    } else if ("update".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.updateTrigger(request);
                                    } else if ("delete".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.deleteTrigger(request);
                                    }
                                } else if (tables.get(i).getName().equalsIgnoreCase("vt2")) {
                                    request.setViewTable(tables.get(i));
                                    triggerProcess = new CountTrigger();
                                    if ("insert".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.insertTrigger(request);
//                                        } else if ("update".equalsIgnoreCase(type)) {
//                                            triggerResponse = triggerProcess.updateTrigger(request);
                                    } else if ("delete".equalsIgnoreCase(type)) {
                                        request.setDeletedRowDeltaView(deltaViewTriggerResponse.getDeletedRowFromDeltaView());
                                        triggerResponse = triggerProcess.deleteTrigger(request);
                                    }
                                } else if (tables.get(i).getName().equalsIgnoreCase("vt3")) {
                                    request.setViewTable(tables.get(i));
                                    triggerProcess = new SumTrigger();
                                    if ("insert".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.insertTrigger(request);
//                                        } else if ("update".equalsIgnoreCase(type)) {
//                                            triggerResponse = triggerProcess.updateTrigger(request);
                                    } else if ("delete".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.deleteTrigger(request);
                                    }

                                } else if (tables.get(i).getName().equalsIgnoreCase("vt4")) {
                                    request.setViewTable(tables.get(i));
                                    triggerProcess = new PreAggregationTrigger();
                                    if ("insert".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.insertTrigger(request);
                                    } else if ("update".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.updateTrigger(request);
                                    } else if ("delete".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.deleteTrigger(request);
                                    }

                                } else if (tables.get(i).getName().equalsIgnoreCase("vt5")) {
                                    request.setViewTable(tables.get(i));
                                    triggerProcess = new ReverseJoinViewTrigger();
                                    // Updating the reverse join view
                                    if ("insert".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.insertTrigger(request);
//                                        } else if ("update".equalsIgnoreCase(type)) {
//                                            triggerResponse = triggerProcess.updateTrigger(request);
                                    } else if ("delete".equalsIgnoreCase(type)) {
                                        request.setDeletedRowDeltaView(deltaViewTriggerResponse.getDeletedRowFromDeltaView());
                                        triggerResponse = triggerProcess.deleteTrigger(request);
                                    }
                                } else if (tables.get(i).getName().equalsIgnoreCase("vt6")) {
                                    request.setViewTable(tables.get(i));
                                    triggerProcess = new SumCountTrigger();
                                    // Updating the reverse join view
                                    if ("insert".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.insertTrigger(request);
//                                        } else if ("update".equalsIgnoreCase(type)) {
//                                            triggerResponse = triggerProcess.updateTrigger(request);
                                    } else if ("delete".equalsIgnoreCase(type)) {
                                        request.setDeletedRowDeltaView(deltaViewTriggerResponse.getDeletedRowFromDeltaView());
                                        triggerResponse = triggerProcess.deleteTrigger(request);
                                    }
                                } else if (tables.get(i).getName().equalsIgnoreCase("vt7")) {
                                    request.setViewTable(tables.get(i));
                                    triggerProcess = new SQLViewMaintenanceTrigger();
                                    if (tables.get(i).getSqlString() != null || tables.get(i).getSqlString().equalsIgnoreCase("")) {
                                        processSQLViewMaintenance(type, tables.get(i));
                                    }

                                    if ("insert".equalsIgnoreCase(type)) {
                                        triggerResponse = triggerProcess.insertTrigger(request);
//                                        } else if ("update".equalsIgnoreCase(type)) {
//                                            triggerResponse = triggerProcess.updateTrigger(request);
                                    } else if ("delete".equalsIgnoreCase(type)) {
                                        request.setDeletedRowDeltaView(deltaViewTriggerResponse.getDeletedRowFromDeltaView());
                                        triggerResponse = triggerProcess.deleteTrigger(request);
                                    }
                                }
                                if (!triggerResponse.isSuccess()) {
                                    break;
                                }
                            }

                            if (triggerResponse.isSuccess()) {
//                                    // If result is successful update the lastOperationProcessed variable and the status file
                                updateStatusFile(operation_id + "");
                            }
                        }
                    } else {
                        logger.debug("The view maintenance system has already processed id=" + operation_id);
                    }

                }
            } catch (Exception e) {
                logger.error("Error !! Stacktrace: \n " + CassandraClientUtilities.getStackTrace(e));
            } finally {
                try {
                    logger.debug("Going to sleep now");
                    this.sleep(SLEEP_INTERVAL);
                } catch (InterruptedException e) {
                    logger.error("Error !! Stacktrace: \n " + CassandraClientUtilities.getStackTrace(e));
                }
            }
//        */
        }
    }

    public static ViewMaintenanceLogsReader getInstance() {
        if (instance == null) {
            synchronized (ViewMaintenanceLogsReader.class) {
                if (instance == null) {
                    instance = new ViewMaintenanceLogsReader();
                }
            }
        }
        return instance;
    }


    /**
     * Here viewConfig means the view config read from the config file.
     * This is not a view table as the other standalone views.
     * This in turn produces a series of views.
     *
     * **/
    public static void processSQLViewMaintenance(String type, Table viewConfig) {
        /**
         *  Decides the view table names, structure.
         * */
        boolean isCreationViewTableCompleted = false;

        processSelectSQL(viewConfig);

    }

    private static Map<String, Table> processSelectSQL(Table viewConfig) {

        Map<String, Table> finalViewTablesList = new HashMap<>();
        Map<String, Map<String, ColumnDefinition>> baseTables = new HashMap<>();
        try{
            String sqlString = viewConfig.getSqlString();
            Map<String, String> operationsInvolved = new HashMap<>();
            List<String> listSelectItems = new ArrayList<>();
            String baseFromTableName = "";
            String baseFromKeySpace = "";
            Statement stmt = CCJSqlParserUtil.parse(sqlString);
            PlainSelect plainSelect = null;
            if (stmt instanceof Select) {
                Select select = (Select) stmt;
                TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
                List<String> tableList = tablesNamesFinder.getTableList(select);
                plainSelect = (PlainSelect) select.getSelectBody();
            }

            if (plainSelect.getWhere() != null) {
                String whereColName = "";
                Expression expression = plainSelect.getWhere();
                if (expression instanceof MinorThan) {
                    MinorThan minorThan = (MinorThan) expression;
                    whereColName = ((Column)minorThan.getLeftExpression()).getColumnName();
                    minorThan.getRightExpression();


                } else if (expression instanceof MinorThanEquals) {
                    MinorThanEquals minorThanEquals = (MinorThanEquals) expression;
                    whereColName = ((Column)minorThanEquals.getLeftExpression()).getColumnName();
                    minorThanEquals.getRightExpression();
                } else if (expression instanceof GreaterThan) {
                    GreaterThan greaterThan = (GreaterThan) expression;
                    whereColName = ((Column)greaterThan.getLeftExpression()).getColumnName();
                    greaterThan.getRightExpression();

                } else if (expression instanceof GreaterThanEquals) {
                    GreaterThanEquals greaterThanEquals = (GreaterThanEquals) expression;
                    whereColName = ((Column)greaterThanEquals.getLeftExpression()).getColumnName();
                    greaterThanEquals.getRightExpression();
                }

                operationsInvolved.put("where", whereColName);
            }

            if (plainSelect.getFromItem() instanceof Table) {
                baseFromTableName = ((Table) plainSelect.getFromItem()).getName();
                if (baseFromTableName.contains(".")) {
                    String tempArr[] = baseFromTableName.split("\\.");
                    baseFromKeySpace = tempArr[0];
                    baseFromTableName = tempArr[1];
                }
                operationsInvolved.put("from", baseFromTableName);
            }

            if (plainSelect.getJoins() != null) {
                /**
                 * Assuming only one join will be present
                 **/
                operationsInvolved.put("join", getJoinType(plainSelect.getJoins().get(0)));
            }

            if (plainSelect.getGroupByColumnReferences() != null) {
                String groupByColName = ((Column)plainSelect.getGroupByColumnReferences().get(0)).getColumnName();
                operationsInvolved.put("groupby", groupByColName);
            }

            if (plainSelect.getHaving() != null) {
                operationsInvolved.put("having", plainSelect.getHaving().toString());
                Expression expression = plainSelect.getHaving();
                if (expression instanceof MinorThan) {

                } else if (expression instanceof MinorThanEquals) {

                } else if (expression instanceof GreaterThan) {

                } else if (expression instanceof GreaterThanEquals) {

                }
            }

            // Creation of views based on the functions present

            Table resultTable = finalResultCreation(viewConfig, plainSelect);
            finalViewTablesList.put(resultTable.getName(), resultTable);

        } catch (JSQLParserException e) {
            logger.error("Error !!! " + CassandraClientUtilities.getStackTrace(e));
        }

        return finalViewTablesList;
    }


    /**
     * Creates the final result view table which needs to be materialized
     **/
    private static Table finalResultCreation(Table viewConfig, PlainSelect plainSelect) {
        Map<String, Map<String, ColumnDefinition>> baseTables = new HashMap<>();
        String baseFromTableName = "";
        String baseFromKeySpace = "";
        Table resultTable = new Table();
        List<de.tum.viewmaintenance.view_table_structure.Column> columns = new ArrayList<>();
        resultTable.setName(viewConfig.getName() + "_result");
        resultTable.setKeySpace(viewConfig.getKeySpace());
        if (plainSelect.getSelectItems() instanceof AllColumns) {

            Map<String, ColumnDefinition>  baseFromTableDef = ViewMaintenanceUtilities.getTableDefinitition(baseFromKeySpace, baseFromTableName);
            baseTables.put(baseFromKeySpace + "." + baseFromTableName, baseFromTableDef);
            for (String key: baseFromTableDef.keySet()) {
                ColumnDefinition columnDefinition = baseFromTableDef.get(key);
//                    listSelectItems.add(columnDefinition.name.toString());
                de.tum.viewmaintenance.view_table_structure.Column column = new de.tum.viewmaintenance.view_table_structure.Column();
                column.setName(columnDefinition.name.toString());
                column.setDataType(ViewMaintenanceUtilities.getJavaTypeFromCassandraType(columnDefinition.type.toString()));
                column.setIsPrimaryKey(columnDefinition.isPartitionKey());
                columns.add(column);

            }
        } else {
            List<SelectItem> selectItems = plainSelect.getSelectItems();
            for (SelectItem selectItem : selectItems) {
                if (selectItem instanceof SelectExpressionItem) {
                    SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
                    boolean isAggregateProjPresent = false;
                    boolean isPrimaryKeyCalculated = false;
                    if (selectExpressionItem.getExpression() instanceof Column) {
                        Column colNameForExpression = (Column) selectExpressionItem.getExpression();
                        String columnName = colNameForExpression.getColumnName();
                        String tableName = colNameForExpression.getTable().getName();

                        /**
                         * Checking whether we already have the table information in the baseTables list.
                         * If not we get from ViewMaintenanceUtilities and store it in baseTables list for further use.
                         **/

                        Map<String, ColumnDefinition> tableDesc = null;
                        if (baseTables.containsKey(tableName)) {
                            tableDesc = baseTables.get(tableName);
                        } else {
                            for (String name : viewConfig.getRefBaseTables()) {
                                String[] completeName = CassandraClientUtilities.getKeyspaceAndTableNameInAnArray(name);
                                if (name.equalsIgnoreCase(completeName[1])) {
                                    tableDesc = ViewMaintenanceUtilities.getTableDefinitition(completeName[0], completeName[1]);
                                    baseTables.put(name, tableDesc);
                                    break;
                                }
                            }
                        }
                        de.tum.viewmaintenance.view_table_structure.Column reqdColumn = new de.tum.viewmaintenance.view_table_structure.Column();
                        reqdColumn.setName(columnName);
                        reqdColumn.setDataType(ViewMaintenanceUtilities.getCQL3DataTypeFromCassandraInternalDataType(tableDesc.get(columnName).type.toString()));
                        columns.add(reqdColumn);


                    } else if (selectExpressionItem.getExpression() instanceof Function) {
                        Function function = (Function)selectExpressionItem.getExpression();

                        String completeTableNamesArr[] = null; // This will contain the complete name of the table in the function involved here.
                        /**
                         * Assuming there will be one expression in the ExpressionList for a function
                         *
                         * If there is a function then the aggregation key is the primary key
                         *
                         * Note: colNameForFunctionWithTable contains "table1.col1"
                         * We need to find the structure for table1 for which we need the keyspace.
                         * The keyspace is found in the viewConfig.getRefBaseTables.
                         **/
                        String colNameForFunctionWithTable = function.getParameters().getExpressions().get(0).toString();
                        String colNameForFunctionWithTableArr[] = null;
                        if (colNameForFunctionWithTable.contains(".")) {
                            colNameForFunctionWithTableArr = colNameForFunctionWithTable.split("\\.");
                        }

                        // Getting the keyspace and table name from the view config file
                        for (String completeTableNames : viewConfig.getRefBaseTables()) {
                            if (completeTableNames.contains(colNameForFunctionWithTableArr[0])) {
                                completeTableNamesArr = completeTableNames.split("\\.");
                            }

                        }


                        Map<String, ColumnDefinition> mapDesc = baseTables.get(completeTableNamesArr[0]
                                + "." + completeTableNamesArr[1]);
                        if (mapDesc == null) {
                            mapDesc = ViewMaintenanceUtilities.getTableDefinitition(completeTableNamesArr[0], completeTableNamesArr[1]);
                            baseTables.put(completeTableNamesArr[0] + "." + completeTableNamesArr[1], mapDesc);
                        }

                        /**
                         * Creating a column for the function projection. E.g. sum_c1
                         **/

                        de.tum.viewmaintenance.view_table_structure.Column reqdCol = new de.tum.viewmaintenance.view_table_structure.Column();
                        reqdCol.setName(function.getName().toLowerCase() + "_" + colNameForFunctionWithTableArr[1]);
                        reqdCol.setDataType("float");
                        columns.add(reqdCol);

                        /**
                         * Assumption: If aggregate function is present then column c1 always is the primary key
                         * as it contains the aggregate key.
                         **/
                        de.tum.viewmaintenance.view_table_structure.Column primaryKeyCol = new de.tum.viewmaintenance.view_table_structure.Column();
                        primaryKeyCol.setDataType(ViewMaintenanceUtilities
                                .getCQL3DataTypeFromCassandraInternalDataType(mapDesc
                                        .get(colNameForFunctionWithTableArr[1])
                                        .name
                                        .toString()));

                        primaryKeyCol.setName("c1");
                        primaryKeyCol.setIsPrimaryKey(true);
                        isPrimaryKeyCalculated = true;

                    }

                    if (!isPrimaryKeyCalculated) {
                        /**
                         * If aggregate function is not present then the primary key is the same as the table in the "from" section
                         *
                         **/

                        Map<String, ColumnDefinition> fromTableDesc = baseTables.get(baseFromKeySpace + "." +
                                baseFromTableName);

                        if (fromTableDesc == null) {
                            fromTableDesc = ViewMaintenanceUtilities.getTableDefinitition(baseFromKeySpace, baseFromTableName);
                        }


                        /**
                         * Looping through column list in the base table and currently collected columns for
                         * resultTable to check for the matching column name which is a primary key in the
                         * base table.
                         **/
                        for (de.tum.viewmaintenance.view_table_structure.Column column: columns) {
                            for (String colName : fromTableDesc.keySet()) {
                                ColumnDefinition colDef = fromTableDesc.get(colName);
                                if (column.getName().equalsIgnoreCase(colDef.name.toString())) {
                                    if (colDef.isPartitionKey()) {
                                        column.setIsPrimaryKey(true);
                                        isPrimaryKeyCalculated = true;
                                        break;
                                    }
                                }
                            }
                            if (isPrimaryKeyCalculated) {
                                break;
                            }
                        }

                        /**
                         * It may be possible that the primary key from the from_base_table is not there in the projection list
                         * then this field should be created in the resultTable
                         **/

                        if (!isPrimaryKeyCalculated) {
                            de.tum.viewmaintenance.view_table_structure.Column primaryKeyColumn
                                    = new de.tum.viewmaintenance.view_table_structure.Column();
                            for (String colName : fromTableDesc.keySet()) {
                                ColumnDefinition tempColDef = fromTableDesc.get(colName);
                                if (tempColDef.isPartitionKey()) {
                                    primaryKeyColumn.setIsPrimaryKey(true);
                                    primaryKeyColumn.setName(tempColDef.name.toString());
                                    primaryKeyColumn.setDataType(ViewMaintenanceUtilities.getCQL3DataTypeFromCassandraInternalDataType(
                                            tempColDef.type.toString()
                                    ));
                                    columns.add(primaryKeyColumn);
                                    isPrimaryKeyCalculated = true;
                                    break;
                                }
                            }

                        }


                    }
                }
            }
        }
        resultTable.setColumns(columns);
        logger.debug("Result table structure :: " + resultTable);
        return resultTable;
    }


    private static String getJoinType(Join join) {
        if (join.isCross()) {
            return "CrossJoin";
        } else if (join.isFull()) {
            return "FullJoin";
        } else if (join.isInner()) {
            return "InnerJoin";
        } else if (join.isLeft()) {
            return "LeftJoin";
        } else if (join.isRight()) {
            return "RightJoin";
        }

        return "";

    }



    private static void traverseSQL() {}


}