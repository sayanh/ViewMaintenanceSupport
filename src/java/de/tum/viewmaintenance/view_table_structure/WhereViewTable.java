package de.tum.viewmaintenance.view_table_structure;

import com.datastax.driver.core.Cluster;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import org.apache.cassandra.config.ColumnDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by shazra on 8/14/15.
 */
public class WhereViewTable implements ViewTable {
    List<Table> tables;
    List<Expression> whereSubExpressions;
    Expression whereExpression;
    private boolean shouldBeMaterialized = false;
    private Table viewConfig;
    private String TABLE_PREFIX = "";
    private static final Logger logger = LoggerFactory.getLogger(WhereViewTable.class);

    public Table getViewConfig() {
        return viewConfig;
    }

    public void setViewConfig(Table viewConfig) {
        this.viewConfig = viewConfig;
        TABLE_PREFIX = viewConfig.getName() + "_where_";
    }

    public List<Table> getTables() {
        return tables;
    }

    private void setTables(List<Table> tables) {
        this.tables = tables;
    }

    public Expression getWhereExpressions() {
        return whereExpression;
    }

    public void setWhereExpressions(Expression whereExpression) {
        this.whereExpression = whereExpression;
    }

    @Override
    public List<Table> createTable() {
        logger.debug("###### Creating table for where clause #########");
        List<Table> tablesCreated = new ArrayList<>();
        Set<String> tableNames = new HashSet<>();
        whereSubExpressions = parseWhereExpression(this.getWhereExpressions());
        logger.debug("### Where sub expressions ### " + whereSubExpressions);
        Map<String, Map<String, ColumnDefinition>> baseTablesDefinitionsMap = new HashMap<>();
        // Get unique table names from the where clause

        for (Expression exp : whereSubExpressions) {
            net.sf.jsqlparser.schema.Column column = null;
            if (exp instanceof MinorThan) {
                column = ((Column)((MinorThan) exp).getLeftExpression());

            } else if (exp instanceof GreaterThan) {
                column = ((Column)((GreaterThan) exp).getLeftExpression());

            } else if (exp instanceof MinorThanEquals) {
                column = ((Column)((MinorThanEquals) exp).getLeftExpression());

            } else if (exp instanceof GreaterThanEquals) {
                column = ((Column)((GreaterThanEquals) exp).getLeftExpression());

            } else if (exp instanceof EqualsTo) {
                column = ((Column)((EqualsTo) exp).getLeftExpression());
            }

            tableNames.add(column.getTable().getName());
        }

        logger.debug("### TableNames found in the where clause ### " + tableNames);

        List<String> referenceBaseTables = getViewConfig().getRefBaseTables();
        logger.debug("### Reference Base tables from view config ### " + referenceBaseTables);
        // Getting the base table structures
        for (String tableName: tableNames) {
            for (String referenceTableName : referenceBaseTables) {
                logger.debug("### Getting the desc for reference table : {} getting matched with {}", referenceTableName,
                        tableName);
                if (referenceTableName.contains(tableName)) {

                    String tempTableNameArr[] = ViewMaintenanceUtilities
                            .getKeyspaceAndTableNameInAnArray(referenceTableName);

                    baseTablesDefinitionsMap.put(referenceTableName,
                            ViewMaintenanceUtilities.getTableDefinitition(tempTableNameArr[0], tempTableNameArr[1]));

                }
            }
        }

        logger.debug("### Base table definitions ### " + baseTablesDefinitionsMap);
        logger.debug("### Base table definitions : Map size ### " + baseTablesDefinitionsMap.size());

        for (Map.Entry<String, Map<String, ColumnDefinition>> table : baseTablesDefinitionsMap.entrySet()) {
            Table newViewTable = new Table();
            newViewTable.setName(TABLE_PREFIX +
                    ViewMaintenanceUtilities.getKeyspaceAndTableNameInAnArray(table.getKey())[1]);
            newViewTable.setKeySpace(viewConfig.getKeySpace());
            List<de.tum.viewmaintenance.view_table_structure.Column> columnList = new ArrayList<>();
            for (Map.Entry<String, ColumnDefinition> column : table.getValue().entrySet()) {
                de.tum.viewmaintenance.view_table_structure.Column newCol = new de.tum.viewmaintenance.view_table_structure.Column();
                newCol.setName(column.getKey());
                newCol.setIsPrimaryKey(column.getValue().isPartitionKey());
                newCol.setDataType(ViewMaintenanceUtilities
                        .getCQL3DataTypeFromCassandraInternalDataType(column.getValue().type + ""));
                columnList.add(newCol);
            }
            newViewTable.setColumns(columnList);

            tablesCreated.add(newViewTable);
        }

        tables = tablesCreated;
        logger.debug("*** Newly created \"where\" view tables :: " + tables);
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
    public void createInMemory(List<Table> realTablesinDB) {

    }

    @Override
    public boolean shouldBeMaterialized() {
        return shouldBeMaterialized;
    }

    public void setShouldBeMaterialized(boolean shouldBeMaterialized) {
        this.shouldBeMaterialized = shouldBeMaterialized;
    }

    private List<Expression> parseWhereExpression(Expression whereExpression) {

        if (whereExpression != null && (!(whereExpression instanceof AndExpression) && !(whereExpression instanceof OrExpression))) {
            List<Expression> temp = new ArrayList<>();
            temp.add(whereExpression);
            return temp;
        }

        List<Expression> whereExpressions = new ArrayList<>();
        if (whereExpression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) whereExpression;

            if (andExpression.getLeftExpression() instanceof AndExpression ||
                    andExpression.getLeftExpression() instanceof OrExpression) {
                for (Expression exp : parseWhereExpression(andExpression.getLeftExpression())) {
                    whereExpressions.add(exp);
                }
            } else {
                whereExpressions.add(andExpression.getLeftExpression());
            }

            if (andExpression.getRightExpression() instanceof AndExpression ||
                    andExpression.getRightExpression() instanceof OrExpression) {
                for (Expression exp : parseWhereExpression(andExpression.getRightExpression())) {
                    whereExpressions.add(exp);
                }
            } else {
                whereExpressions.add(andExpression.getRightExpression());
            }

        } else if (whereExpression instanceof OrExpression) {
            OrExpression orExpression = (OrExpression) whereExpression;

            if (orExpression.getLeftExpression() instanceof OrExpression ||
                    orExpression.getLeftExpression() instanceof AndExpression) {
                for (Expression exp : parseWhereExpression(orExpression.getLeftExpression())) {
                    whereExpressions.add(exp);
                }
            } else {
                whereExpressions.add(orExpression.getLeftExpression());
            }

            if (orExpression.getRightExpression() instanceof OrExpression ||
                    orExpression.getRightExpression() instanceof AndExpression) {
                for (Expression exp : parseWhereExpression(orExpression.getRightExpression())) {
                    whereExpressions.add(exp);
                }
            } else {
                whereExpressions.add(orExpression.getRightExpression());
            }

        }

        return whereExpressions;

    }




}
