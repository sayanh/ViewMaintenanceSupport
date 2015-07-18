package de.tum.viewmaintenance.viewsTableStructure;

import java.util.List;

/**
 * Created by shazra on 6/21/15.
 */

import java.util.List;

public class Table {
    private String name;
    private List<Column> columns;
    private String keySpace;
    private String basedOn;
    private String actionType;

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
                " Columns: " + columns;
    }
}
