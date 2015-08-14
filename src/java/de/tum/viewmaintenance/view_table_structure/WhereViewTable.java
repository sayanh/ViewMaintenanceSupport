package de.tum.viewmaintenance.view_table_structure;

import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.config.ViewMaintenanceUtilities;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.schema.Column;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.thrift.ColumnDef;

import java.util.*;

/**
 * Created by shazra on 8/14/15.
 */
public class WhereViewTable implements ViewTable {
    List<Table> table;
    List<Expression> whereSubExpressions;
    Expression whereExpression;
    private boolean shouldBeMaterialized = false;
    private Table viewConfig;
    private String iterationRandomStr = "";
    private final String TABLE_PREFIX = "where_" + iterationRandomStr + "_";

    public Table getViewConfig() {
        return viewConfig;
    }

    public void setViewConfig(Table viewConfig) {
        this.viewConfig = viewConfig;
    }

    public String getIterationRandomStr() {
        return iterationRandomStr;
    }

    public void setIterationRandomStr(String iterationRandomStr) {
        this.iterationRandomStr = iterationRandomStr;
    }

    public List<Table> getTable() {
        return table;
    }

    public void setTable(List<Table> table) {
        this.table = table;
    }

    public Expression getWhereExpressions() {
        return whereExpression;
    }

    public void setWhereExpressions(Expression whereExpression) {
        this.whereExpression = whereExpression;
    }

    @Override
    public List<Table> createTable() {
        List<Table> whereTables = new ArrayList<>();
        Set<String> tableNames = new HashSet<>();
        whereSubExpressions = parseWhereExpression(this.getWhereExpressions());
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

        List<String> referenceBaseTables = getViewConfig().getRefBaseTables();
        // Getting the base table structures
        for (String tableName: tableNames) {
            for (String referenceTableName : referenceBaseTables) {
                if (tableName.contains(referenceTableName)) {
                    baseTablesDefinitionsMap.put(referenceTableName,
                            ViewMaintenanceUtilities.getTableDefinitition(CassandraClientUtilities
                                    .getKeyspaceAndTableNameInAnArray(referenceTableName)[0], CassandraClientUtilities
                                    .getKeyspaceAndTableNameInAnArray(referenceTableName)[1]));
                }
            }
        }

        for (Map.Entry<String, Map<String, ColumnDefinition>> table : baseTablesDefinitionsMap.entrySet()) {
            Table newViewTable = new Table();
            newViewTable.setName(TABLE_PREFIX +
                    CassandraClientUtilities.getKeyspaceAndTableNameInAnArray(table.getKey())[1]);
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
            
        }




        return whereTables;
    }

    @Override
    public boolean deleteTable() {
        return false;
    }

    @Override
    public boolean materializeTable() {
        return false;
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
