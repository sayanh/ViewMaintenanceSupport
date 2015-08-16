package de.tum.viewmaintenance.config;

import com.datastax.driver.core.Row;
import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import de.tum.viewmaintenance.Operations.*;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.trigger.*;
import de.tum.viewmaintenance.view_table_structure.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
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
    private static final String OPERATIONS_FILENAME = "logicalplan.json";
    private static final Logger logger = LoggerFactory.getLogger(ViewMaintenanceLogsReader.class);
    private static final int SLEEP_INTERVAL = 10000;
    private static Map<String, Boolean> operationsFileMap = null;
    private static List<GenericOperation> operationQueue = new ArrayList<>();

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
                    logger.error("Error !! Stacktrace: \n " + ViewMaintenanceUtilities.getStackTrace(e));
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
                                    Row deltaViewRow = null;
                                    if ("delete".equalsIgnoreCase(type)) {
                                        deltaViewRow = deltaViewTriggerResponse.getDeletedRowFromDeltaView();
                                    } else {
                                        deltaViewRow = deltaViewTriggerResponse.getDeltaViewUpdatedRow();
                                    }
                                    if (tables.get(i).getSqlString() != null || tables.get(i).getSqlString().equalsIgnoreCase("")) {
                                        processSQLViewMaintenance(type, tables.get(i), deltaViewRow);
                                    }

//                                    if ("insert".equalsIgnoreCase(type)) {
//                                        triggerResponse = triggerProcess.insertTrigger(request);
////                                        } else if ("update".equalsIgnoreCase(type)) {
////                                            triggerResponse = triggerProcess.updateTrigger(request);
//                                    } else if ("delete".equalsIgnoreCase(type)) {
//                                        request.setDeletedRowDeltaView(deltaViewTriggerResponse.getDeletedRowFromDeltaView());
//                                        triggerResponse = triggerProcess.deleteTrigger(request);
//                                    }
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
                logger.error("Error !! Stacktrace: \n " + ViewMaintenanceUtilities.getStackTrace(e));
            } finally {
                try {
                    logger.debug("Going to sleep now");
                    this.sleep(SLEEP_INTERVAL);
                } catch (InterruptedException e) {
                    logger.error("Error !! Stacktrace: \n " + ViewMaintenanceUtilities.getStackTrace(e));
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
     **/
    public static void processSQLViewMaintenance(String type, Table viewConfig, Row deltaTableViewRow) throws IOException, JSQLParserException {
        logger.debug("ProcessSQLViewMaintenace | with type: {} , viewConfig {} , deltaTableViewRow {} ", type, viewConfig, deltaTableViewRow);
        /**
         *  Decides the view table names, structure.
         * */
        boolean isCreationViewTableCompleted = false;

        if (operationQueue.size() == 0) {
            createSQLTables(viewConfig, deltaTableViewRow);
        }

//        processTriggersForViewMaintenance(type);
    }

    private static void processTriggersForViewMaintenance(String type) {
        logger.debug("#### processTriggersForViewMaintenance ###");
        for (GenericOperation operation: operationQueue){
//            if (operation instanceof WhereOperation) {
//                operation = (WhereOperation)operation;
//                operation.processOperation(type);
//            } else if (operation instanceof PreAggOperation) {
//                operation = (PreAggOperation)operation;
//                operation.processOperation(type);
//            } else if (operation instanceof AggOperation) {
//                operation = (AggOperation)operation;
//                operation.processOperation(type);
//            } else if (operation instanceof ReverseJoinOperation) {
//                operation = (ReverseJoinOperation)operation;
//                operation.processOperation(type);
//            } else if (operation instanceof InnerJoinOperation) {
//                operation = (InnerJoinOperation)operation;
//                operation.processOperation(type);
//            } else if (operation instanceof ResultViewOperation) {
//                operation = (ResultViewOperation)operation;
//                operation.processOperation(type);
//            }

            operation.processOperation(type);
        }
    }


    /**
     * TODO: This will not work for nested SELECT statements. For that following action should be taken.
     * <view_name>_<operation_name>_<basetable_name>_<counter>
     * Each time due to a nested select query, it reaches here, the counter increases.
     *
     * **/
    private static List<Table> createSQLTables(Table viewConfig, Row deltaTableViewRow) throws IOException, JSQLParserException {
        logger.debug(" ***** Inside createSQLTables() .....");
        ResultViewTable resultViewTable = null;

//        try {
            String sqlString = viewConfig.getSqlString();

            // operationsInvolved facilitates random and quick check on the presence of the clauses present.
            Map<String, String> operationsInvolved = new HashMap<>();


            List<Function> functionList = new ArrayList<>();
            List<Expression> listSelectExpressions = new ArrayList<>();

            String baseFromTableName = "";
            String baseFromKeySpace = "";
            Statement stmt = CCJSqlParserUtil.parse(sqlString);
            PlainSelect plainSelect = null;

            WhereViewTable whereViewTable = null;
            ReverseJoinViewTable reverseJoinViewTable = null;
            InnerJoinViewTable innerJoinViewTable = null;
            InnerJoinOperation innerJoinOperation = null;
            PreAggViewTable preAggViewTable = null;
            AggViewTable aggViewTable = null;

            logger.debug("### The current status of the operationQueue is ### " + operationQueue);
            logger.debug("### The current size of the operationQueue is ### " + operationQueue.size());


            if (operationQueue.size() == 0) {

                logger.debug(" ****** Operation Queue is null:: Entering here for first time ******");
                if (stmt instanceof Select) {
                    Select select = (Select) stmt;
                    TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
                    List<String> tableList = tablesNamesFinder.getTableList(select);
                    plainSelect = (PlainSelect) select.getSelectBody();
                }

                logger.debug("### ### ### ### ###");
                logger.debug("### State of art ###");
                logger.debug("### Where clause: " + plainSelect.getWhere());
                logger.debug("### From clause: " + plainSelect.getFromItem());
                logger.debug("### Select items clause: " + plainSelect.getSelectItems());
                logger.debug("### Join clause: " + plainSelect.getJoins());
                logger.debug("### GroupBy clause: " + plainSelect.getGroupByColumnReferences());
                logger.debug("### Having clause: " + plainSelect.getHaving());


                /**
                 * Checking for where clause
                 **/


                if (plainSelect.getWhere() != null) {
                    logger.debug("### Computing the where clause ###");
                    String whereColName = "";

                    Expression expression = plainSelect.getWhere();

                    operationsInvolved.put("where", whereColName);


                    for (SelectItem selectItem: plainSelect.getSelectItems()) {
                        if (selectItem instanceof SelectExpressionItem) {
                            SelectExpressionItem expressionItem = (SelectExpressionItem) selectItem;
                            listSelectExpressions.add(expressionItem.getExpression());
                            if (expressionItem.getExpression() instanceof Function) {
                                Function function = (Function)expressionItem.getExpression();
                                functionList.add(function);
                            }
                        }
                    }

                    whereViewTable = new WhereViewTable();
                    whereViewTable.setWhereExpressions(expression);
                    whereViewTable.setShouldBeMaterialized(getMapOperations().get("where"));
                    whereViewTable.setViewConfig(viewConfig);
                    List<Table> whereTablesCreated = whereViewTable.createTable();
                    if (whereViewTable.shouldBeMaterialized()) {
                        whereViewTable.materialize();
                    } else {
                        //TODO: yet to be implemented.
                        whereViewTable.createInMemory(whereTablesCreated);
                    }
                    WhereOperation whereOperation = WhereOperation.getInstance(deltaTableViewRow, null, whereTablesCreated);
                    operationQueue.add(whereOperation);
                    logger.debug("### After adding where operation in operationQueue :: " + operationQueue);
                }

                if (plainSelect.getFromItem() instanceof Table) {
                    baseFromTableName = ((Table) plainSelect.getFromItem()).getName();
                    String baseFromTableNameArr[] = ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(baseFromTableName);
                    baseFromKeySpace = baseFromTableNameArr[0];
                    baseFromTableName = baseFromTableNameArr[1];
                    operationsInvolved.put("from", baseFromTableName);
                }


                /**
                 * Checking for joins
                 **/



                if (plainSelect.getJoins() != null) {
                    /**
                     * Note: Assuming only one join will be present
                     **/

                    // Creating ReverseJoin View

                    reverseJoinViewTable = new ReverseJoinViewTable();
                    reverseJoinViewTable.setJoins(plainSelect.getJoins());
                    reverseJoinViewTable.setViewConfig(viewConfig);
                    reverseJoinViewTable.setShouldBeMaterialized(getMapOperations().get("reversejoin"));
                    reverseJoinViewTable.setFromBaseTable(baseFromKeySpace + "." + baseFromTableName);
                    List<Table> reverseJoinTablesCreated = reverseJoinViewTable.createTable();

                    if (reverseJoinViewTable.shouldBeMaterialized()) {
                        reverseJoinViewTable.materialize();
                    } else {
                        //TODO: yet to be implemented.
                        reverseJoinViewTable.createInMemory(reverseJoinTablesCreated);
                    }

                    ReverseJoinOperation reverseJoinOperation = ReverseJoinOperation.getInstance(deltaTableViewRow,
                            whereViewTable.getTables(),reverseJoinTablesCreated);
                    operationQueue.add(reverseJoinOperation);
                    operationsInvolved.put("join", getJoinType(plainSelect.getJoins().get(0)));

                    // Creating Required Join View
                    // Note: Only Inner Join works now

                    List<Table> innerJoinTablesCreated = null;
                    for (Join join: reverseJoinViewTable.getJoins()) {
                        if (join.isInner()) {
                            innerJoinViewTable = new InnerJoinViewTable();
                            innerJoinViewTable.setShouldBeMaterialized(getMapOperations().get("join"));
                            innerJoinViewTable.setInputReverseJoinTableStruc(
                                    reverseJoinViewTable.getTables().get(0));
                            innerJoinViewTable.setViewConfig(viewConfig);
                            innerJoinTablesCreated = innerJoinViewTable.createTable();
                            if (innerJoinViewTable.shouldBeMaterialized()) {
                                innerJoinViewTable.materialize();
                            } else {
                                //TODO: yet to be implemented.
                                innerJoinViewTable.createInMemory(innerJoinTablesCreated);
                            }
                        }
                    }

                    innerJoinOperation = InnerJoinOperation.getInstance(deltaTableViewRow,
                            reverseJoinViewTable.getTables(), innerJoinTablesCreated);
                    operationQueue.add(innerJoinOperation);

                }


                /**
                 * Checking for aggregate functions
                 * Clauses to check: aggregate functions in projection items, group by
                 * Note: Only ONE groupBy reference works now.
                 *
                 * **/



                if (plainSelect.getGroupByColumnReferences() != null) {
                    List<Expression> groupByExpressions = plainSelect.getGroupByColumnReferences();
                    preAggViewTable = new PreAggViewTable();
                    preAggViewTable.setDeltaTableRecord(deltaTableViewRow);
                    preAggViewTable.setShouldBeMaterialized(getMapOperations().get("preaggregation"));
                    if (operationsInvolved.get("join") != null) {
                        logger.debug(" ***** Join is present hence adding join view table :: " + innerJoinViewTable);
                        preAggViewTable.setInputViewTable(innerJoinViewTable);
                    } else {
                        logger.debug(" ***** Join is present hence adding join view table :: " + whereViewTable);
                        preAggViewTable.setInputViewTable(whereViewTable);
                    }
                    preAggViewTable.setViewConfig(viewConfig);
                    preAggViewTable.setGroupByExpressions(groupByExpressions);
                    preAggViewTable.setFunctionExpressions(functionList);
                    preAggViewTable.setBaseTableName(baseFromKeySpace + "." + baseFromTableName);

                    List<Table> preAggTablesCreated = preAggViewTable.createTable();

                    if (preAggViewTable.shouldBeMaterialized()) {
                        preAggViewTable.materialize();
                    } else {
                        //TODO: yet to be implemented.
                        preAggViewTable.createInMemory(preAggTablesCreated);
                    }

                    if (operationsInvolved.get("join") != null) {
                        PreAggOperation preAggOperation = PreAggOperation.getInstance(deltaTableViewRow,
                                innerJoinViewTable.getTables(), preAggTablesCreated);
                    } else {
                        PreAggOperation preAggOperation = PreAggOperation.getInstance(deltaTableViewRow,
                                whereViewTable.getTables(), preAggTablesCreated);
                    }



                    // Storing the expression in the operationsInvolved list.

                    operationsInvolved.put("groupBy", groupByExpressions.get(0).toString());
                }

                /**
                 * For cases when there is NO groupBy but there is an aggregate function
                 * in the select item
                 **/

//                if (!operationsInvolved.containsKey("groupBy") && ) {
//
//                }


                /**
                 * Computing the aggregate view table
                 **/

                if (plainSelect.getHaving() != null) {
                    operationsInvolved.put("having", plainSelect.getHaving().toString());
                    Expression expressionHaving = plainSelect.getHaving();

                    aggViewTable = new AggViewTable();
                    aggViewTable.setViewConfig(viewConfig);
                    aggViewTable.setShouldBeMaterialized(getMapOperations().get("aggregation"));

                    List<Table> aggViewTableCreated = aggViewTable.createTable();

                    if (aggViewTable.shouldBeMaterialized()) {
                        aggViewTable.materialize();
                    } else {
                        //TODO: yet to be implemented.
                        aggViewTable.createInMemory(aggViewTableCreated);
                    }

                    AggOperation aggOperation = AggOperation.getInstance(deltaTableViewRow,
                            preAggViewTable.getTables(), aggViewTableCreated);
                    operationsInvolved.put("having", expressionHaving.toString());
                    operationQueue.add(aggOperation);
                }

                // Creation of views based on the functions present

                resultViewTable = new ResultViewTable();
                resultViewTable.setViewConfig(viewConfig);
                resultViewTable.setPlainSelect(plainSelect);


                List<Table> resultTableCreated = resultViewTable.createTable();
                resultViewTable.materialize();

                ResultViewOperation resultViewOperation = null;
                if (operationsInvolved.containsValue("having")) {
                    resultViewOperation = ResultViewOperation.getInstance(deltaTableViewRow,
                            aggViewTable.getTables(), resultTableCreated);
                } else if (operationsInvolved.containsValue("groupBy")){
                    resultViewOperation = ResultViewOperation.getInstance(deltaTableViewRow,
                            preAggViewTable.getTables(), resultTableCreated);
                } else if (operationsInvolved.containsValue("join")){
                    resultViewOperation = ResultViewOperation.getInstance(deltaTableViewRow,
                            innerJoinViewTable.getTables(), resultTableCreated);
                } else if (operationsInvolved.containsValue("where")){
                    resultViewOperation = ResultViewOperation.getInstance(deltaTableViewRow,
                            whereViewTable.getTables(), resultTableCreated);
                }

                operationQueue.add(resultViewOperation);
            }

//        } catch (JSQLParserException e) {
//            logger.error("Error !!! " + ViewMaintenanceUtilities.getStackTrace(e));
//        }

        return resultViewTable.getTables();
    }


    private static Map<String, Boolean> getMapOperations() throws IOException {
        if (operationsFileMap == null) {
            logger.debug("***** System.getProperty(\"user.dir\") = " + System.getProperty("user.dir") );
            String stringList = new String(Files.readAllBytes(Paths.get(OPERATIONS_FILENAME)));
            operationsFileMap = new Gson().fromJson(stringList, new TypeToken<HashMap<String, Object>>() {
            }.getType());
        }

        return operationsFileMap;
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

}