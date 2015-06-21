package de.tum.viewmaintenance.view_table_structure;

/**
 * Created by anarchy on 6/21/15.
 */

import java.util.List;

public class Table {
    private String name;
    private List<Column> columns;
    private String keySpace;

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
                " Columns: " + columns;
    }
}
