package de.tum.viewmaintenance.view_table_structure;

/**
 * Created by shazra on 6/21/15.
 */

import java.util.ArrayList;
import java.util.List;

public class Table {
    private String name;
    private List<Column> columns;
    private String keySpace;
    private String basedOn;
    private String actionType;
    //TODO: Merge refBaseTables and refBaseTable, I did not want to disturb the working code.

    private List<String> refBaseTables;

    private String refBaseTable;
    public String sqlString;


    public List<String> getRefBaseTables() {
        if (refBaseTables == null || refBaseTables.size() == 0) {
            if (refBaseTables == null){
                refBaseTables = new ArrayList<>();
            }
            String tempRefBaseTableArr[] = refBaseTable.split(",");
            for (String refTableName: tempRefBaseTableArr) {
                refBaseTables.add(refTableName);
            }
        }
        return refBaseTables;
    }

    public void setRefBaseTables(List<String> refBaseTables) {
        this.refBaseTables = refBaseTables;
    }

    public String getSqlString() {
        return sqlString;
    }

    public void setSqlString(String sqlString) {
        this.sqlString = sqlString;
    }

    public String getRefBaseTable() {
        return refBaseTable;
    }

    public void setRefBaseTable(String refBaseTable) {
        this.refBaseTable = refBaseTable;
    }

    public String getBasedOn() {
        return basedOn;
    }

    public void setBasedOn(String basedOn) {
        this.basedOn = basedOn;
    }

    public String getActionType() {
        return actionType;
    }

    public void setActionType(String actionType) {
        this.actionType = actionType;
    }

    public String getKeySpace() {
        return keySpace;
    }

    public void setKeySpace(String keySpace) {
        this.keySpace = keySpace;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }



    @Override
    public String toString() {
        return "Table name: " + name +
                ", BasedOn: " + basedOn +
                ", ActionType: " + actionType +
                ", refBaseTable: " + refBaseTable +
                ", Columns: " + columns +
                ", SQL: " + sqlString;
    }
}
