package de.tum.viewmaintenance.view_table_structure;

import com.datastax.driver.core.Cluster;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.Join;
import org.apache.cassandra.config.ColumnDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shazra on 8/14/15.
 */
public class ReverseJoinViewTable implements ViewTable {

    List<Table> tables;
    List<Join> joins;
    String fromBaseTable;

    private boolean shouldBeMaterialized = false;

    private Table viewConfig;
    private String TABLE_PREFIX;
    private static final Logger logger = LoggerFactory.getLogger(ReverseJoinViewTable.class);

    /**
     * Naming convention for reverse join view tables: <view_name>_reversejoin_<base_table_name1>_<base_table_name2>
     * Note: This will ONLY work for a single join case.
     * For multiple joins there should be an identifier generated for each join.
     * Caveat: Works for only one join in the view SQL query
     **/


    public List<Table> getTables() {
        return tables;
    }


    private void setTables(List<Table> tables) {
        this.tables = tables;
    }

    public String getFromBaseTable() {
        return fromBaseTable;
    }


    public void setFromBaseTable(String fromBaseTable) {
        this.fromBaseTable = fromBaseTable;
    }

    public Table getViewConfig() {
        return viewConfig;
    }

    public void setViewConfig(Table viewConfig) {
        this.viewConfig = viewConfig;
        TABLE_PREFIX = viewConfig.getName() + "_reversejoin_";
    }

    @Override
    public List<Table> createTable() {
        List<Table> tablesCreated = new ArrayList<>();
        List<String> tableNames = new ArrayList<>();
        String tableName2 = ((net.sf.jsqlparser.schema.Table) joins.get(0).getRightItem()).getName();
        EqualsTo equalsToOnExpression = (EqualsTo) joins.get(0).getOnExpression();
        Column leftCol = ((net.sf.jsqlparser.schema.Column) equalsToOnExpression.getLeftExpression());
        Column rightCol = ((net.sf.jsqlparser.schema.Column) equalsToOnExpression.getRightExpression());
        tableNames.add(fromBaseTable);
        for (String tableInvolved : viewConfig.getRefBaseTables()) {
            if (tableInvolved.contains(tableName2)) {
                tableNames.add(tableInvolved);
            }
        }

        logger.debug(" ***** Tables involved in the join :: " + tableNames);

        Map<String, Map<String, ColumnDefinition>> baseTablesDefinitionsMap = new HashMap<>();

        for (String tableInvolved : tableNames) {
            String tableNameArr[] = ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(tableInvolved);
            baseTablesDefinitionsMap.put(tableInvolved, ViewMaintenanceUtilities
                    .getTableDefinitition(tableNameArr[0], tableNameArr[1]));
        }

        logger.debug("***** baseTablesDefinitionsMap for the tables involved {}", baseTablesDefinitionsMap);


        Table newViewTable = new Table();
        newViewTable.setName(TABLE_PREFIX +
                leftCol.getTable().getName() + "_" + rightCol.getTable().getName());
        newViewTable.setKeySpace(viewConfig.getKeySpace());


        List<de.tum.viewmaintenance.view_table_structure.Column> columnList = new ArrayList<>();
        boolean isPrimaryColCreated = false;
        for (Map.Entry<String, Map<String, ColumnDefinition>> table : baseTablesDefinitionsMap.entrySet()) {

            for (Map.Entry<String, ColumnDefinition> columnDefinitionEntry : table.getValue().entrySet()) {
                de.tum.viewmaintenance.view_table_structure.Column column = new de.tum.viewmaintenance.view_table_structure.Column();
                if (leftCol.getColumnName().equalsIgnoreCase(columnDefinitionEntry.getValue().name + "")) {
                    if (!isPrimaryColCreated) {
                        column.setName(leftCol.getColumnName() + "_" + rightCol.getColumnName());
                        column.setIsPrimaryKey(true);
                        column.setDataType(ViewMaintenanceUtilities
                                .getCQL3DataTypeFromCassandraInternalDataType(columnDefinitionEntry
                                        .getValue().type + ""));
                        columnList.add(column);
                        isPrimaryColCreated = true;
                    }
                    continue;
                } else if (rightCol.getColumnName().equalsIgnoreCase(columnDefinitionEntry.getValue().name + "")) {
                    if (!isPrimaryColCreated) {
                        column.setName(leftCol.getColumnName() + "_" + rightCol.getColumnName());
                        column.setIsPrimaryKey(true);
                        column.setDataType(ViewMaintenanceUtilities
                                .getCQL3DataTypeFromCassandraInternalDataType(columnDefinitionEntry
                                        .getValue().type + ""));
                        columnList.add(column);
                        isPrimaryColCreated = true;
                    }
                    continue;
                }

                column.setName(columnDefinitionEntry.getValue().name + "");
                column.setDataType(ViewMaintenanceUtilities
                        .getCQL3DataTypeFromCassandraInternalDataType(columnDefinitionEntry
                                .getValue().type + ""));
                columnList.add(column);
            }
        }

        newViewTable.setColumns(columnList);

        logger.debug("***** Newly created table for reverse join :: " + newViewTable);
        tablesCreated.add(newViewTable);
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

    public List<Join> getJoins() {
        return joins;
    }


    public void setJoins(List<Join> joins) {
        this.joins = joins;
    }

    public boolean isShouldBeMaterialized() {
        return shouldBeMaterialized;
    }

    public void setShouldBeMaterialized(boolean shouldBeMaterialized) {
        this.shouldBeMaterialized = shouldBeMaterialized;
    }

}
