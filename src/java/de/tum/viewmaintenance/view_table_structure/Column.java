package de.tum.viewmaintenance.view_table_structure;

/**
 * Created by shazra on 6/21/15.
 */

public class Column {
    private String name;
    private String type;
    private boolean isPrimaryKey = false;
    private String constraint;
    private String dataType;
    private String correspondingColumn;
    private Object value;
    private String javaDataType;

    public String getJavaDataType() {
        return javaDataType;
    }

    public void setJavaDataType(String javaDataType) {
        this.javaDataType = javaDataType;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public String getCorrespondingColumn() {
        return correspondingColumn;
    }

    public void setCorrespondingColumn(String correspondingColumn) {
        this.correspondingColumn = correspondingColumn;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public void setIsPrimaryKey(boolean isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey;
    }

    public String getConstraint() {
        return constraint;
    }

    public void setConstraint(String constraint) {
        this.constraint = constraint;
    }

    @Override
    public String toString() {
        return "Column - name : " + name +
                ", type : " + type +
                ", dataType : " + dataType +
                ", javaDataType : " + javaDataType +
                ", value : " + value +
                ", isPrimaryKey : " + isPrimaryKey;
    }
}

