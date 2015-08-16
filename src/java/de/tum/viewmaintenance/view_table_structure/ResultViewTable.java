package de.tum.viewmaintenance.view_table_structure;

import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.cassandra.config.ColumnDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shazra on 8/16/15.
 */
public class ResultViewTable implements ViewTable {

    private Table viewConfig;
    private PlainSelect plainSelect;
    List<Table> tables;

    private static final Logger logger = LoggerFactory.getLogger(ResultViewTable.class);

    @Override
    public List<Table> createTable() {
        List<Table> tablesCreated = new ArrayList<>();
        Map<String, Map<String, ColumnDefinition>> baseTables = new HashMap<>();
        String baseFromTableName = "";
        String baseFromKeySpace = "";
        Table resultTable = new Table();
        List<de.tum.viewmaintenance.view_table_structure.Column> columns = new ArrayList<>();
        resultTable.setName(viewConfig.getName() + "_result");
        resultTable.setKeySpace(viewConfig.getKeySpace());
        if (plainSelect.getSelectItems() instanceof AllColumns) {

            Map<String, ColumnDefinition> baseFromTableDef = ViewMaintenanceUtilities.getTableDefinitition(baseFromKeySpace, baseFromTableName);
            baseTables.put(baseFromKeySpace + "." + baseFromTableName, baseFromTableDef);
            for (String key : baseFromTableDef.keySet()) {
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
                    if (selectExpressionItem.getExpression() instanceof net.sf.jsqlparser.schema.Column) {
                        net.sf.jsqlparser.schema.Column colNameForExpression = (net.sf.jsqlparser.schema.Column) selectExpressionItem.getExpression();
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
                                String[] completeName = ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(name);
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
                        Function function = (Function) selectExpressionItem.getExpression();

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
                        for (de.tum.viewmaintenance.view_table_structure.Column column : columns) {
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
        tablesCreated.add(resultTable);
        tables = tablesCreated;
        return tables;
    }
    @Override
    public void deleteTable() {

    }

    @Override
    public void materialize() {

    }


    @Override
    public boolean shouldBeMaterialized() {
        return false;
    }

    @Override
    public void createInMemory(List<Table> tables) {

    }

    public Table getViewConfig() {
        return viewConfig;
    }

    public void setViewConfig(Table viewConfig) {
        this.viewConfig = viewConfig;
    }

    public PlainSelect getPlainSelect() {
        return plainSelect;
    }

    public void setPlainSelect(PlainSelect plainSelect) {
        this.plainSelect = plainSelect;
    }

    public List<Table> getTables() {
        return tables;
    }

    private void setTables(List<Table> tables) {
        this.tables = tables;
    }
}
