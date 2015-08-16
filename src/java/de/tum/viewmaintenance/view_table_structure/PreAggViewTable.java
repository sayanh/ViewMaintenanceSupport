package de.tum.viewmaintenance.view_table_structure;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import org.apache.cassandra.config.ColumnDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by anarchy on 8/14/15.
 */
public class PreAggViewTable implements ViewTable {
    private static final Logger logger = LoggerFactory.getLogger(PreAggViewTable.class);

    private boolean shouldBeMaterialized = false;

    private List<Function> functionExpressions = null;

    private List<Expression> groupByExpressions = null;

    private Row deltaTableRecord;

    private ViewTable inputViewTable;

    private Table viewConfig;
    private List<Table> tables;

    private String baseTableName;

    private String TABLE_PREFIX;

    /**
     * Naming convention for preagg view tables: <view_name>_preagg
     **/

    public Row getDeltaTableRecord() {
        return deltaTableRecord;
    }

    public void setDeltaTableRecord(Row deltaTableRecord) {
        this.deltaTableRecord = deltaTableRecord;
    }
    public ViewTable getInputViewTable() {
        return inputViewTable;
    }

    public void setInputViewTable(ViewTable inputViewTable) {
        this.inputViewTable = inputViewTable;
    }

    @Override
    public List<Table> createTable() {
        List<Table> tablesCreated = new ArrayList<>();
        List<Column> newColumnsForNewTable = new ArrayList<>();
        for (Function function: functionExpressions) {
            Column tempCol = new Column();
            tempCol.setName(function.getName());
            if (function.getName().equalsIgnoreCase("SUM")) {
                tempCol.setDataType("integer");
            } else if (function.getName().equalsIgnoreCase("COUNT")) {
                tempCol.setDataType("integer");
            } else if (function.getName().equalsIgnoreCase("MAX")) {
                tempCol.setDataType("integer");
            } else if (function.getName().equalsIgnoreCase("MIN")) {
                tempCol.setDataType("integer");
            }
            // Aggregate functions on all columns are not possible.
            if (function.isAllColumns()) {
                tempCol.setIsPrimaryKey(true);
            }
            newColumnsForNewTable.add(tempCol);

            // Creating a column for which the function exists

            if (!function.isAllColumns()) {

                net.sf.jsqlparser.schema.Column functionCol = (net.sf.jsqlparser.schema.Column)
                        function.getParameters().getExpressions().get(0);

                Column primaryKeyColfunctionColumn = new Column();

                primaryKeyColfunctionColumn.setIsPrimaryKey(true);

                primaryKeyColfunctionColumn.setName(functionCol.getTable().getName() + "_" +
                        functionCol.getColumnName());

                String baseTableNameArr[] = ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(baseTableName);

                Map<String, ColumnDefinition> baseTableDesc = ViewMaintenanceUtilities
                        .getTableDefinitition(baseTableNameArr[0], baseTableNameArr[1]);

                primaryKeyColfunctionColumn.setDataType(ViewMaintenanceUtilities
                        .getCQL3DataTypeFromCassandraInternalDataType(baseTableDesc.get(
                                functionCol.getColumnName()).type + ""));

                newColumnsForNewTable.add(primaryKeyColfunctionColumn);
            }


         }

        Table newTableCreated = new Table();
        newTableCreated.setName(TABLE_PREFIX);
        newTableCreated.setColumns(newColumnsForNewTable);
        newTableCreated.setKeySpace(viewConfig.getKeySpace());
        tablesCreated.add(newTableCreated);
        tables = tablesCreated;

        return tables;
    }

    @Override
    public void deleteTable() {

    }

    @Override
    public void materialize() {
        for (Table newTable : getTables()) {
            logger.debug(" Table getting materialized :: " + newTable);
            Cluster cluster = CassandraClientUtilities.getConnection("localhost");
            CassandraClientUtilities.createTable(cluster, newTable);
            CassandraClientUtilities.closeConnection(cluster);
        }
    }

    @Override
    public boolean shouldBeMaterialized() {
        return false;
    }

    @Override
    public void createInMemory(List<Table> tables) {

    }

    public boolean isShouldBeMaterialized() {
        return shouldBeMaterialized;
    }

    public void setShouldBeMaterialized(boolean shouldBeMaterialized) {
        this.shouldBeMaterialized = shouldBeMaterialized;
    }

    public Table getViewConfig() {
        return viewConfig;
    }

    public void setViewConfig(Table viewConfig) {
        this.viewConfig = viewConfig;
        TABLE_PREFIX = viewConfig.getName() + "_preagg";
    }

    public List<Table> getTables() {
        return tables;
    }

    private void setTables(List<Table> tables) {
        this.tables = tables;
    }


    public List<Function> getFunctionExpressions() {
        return functionExpressions;
    }

    public void setFunctionExpressions(List<Function> functionExpressions) {
        this.functionExpressions = functionExpressions;
    }

    public List<Expression> getGroupByExpressions() {
        return groupByExpressions;
    }

    public void setGroupByExpressions(List<Expression> groupByExpressions) {
        this.groupByExpressions = groupByExpressions;
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public void setBaseTableName(String baseTableName) {
        this.baseTableName = baseTableName;
    }

}
