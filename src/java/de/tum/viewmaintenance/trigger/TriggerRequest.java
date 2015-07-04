package de.tum.viewmaintenance.trigger;

import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.view_table_structure.Table;

/**
 * Created by anarchy on 6/27/15.
 */
public class TriggerRequest {
    private LinkedTreeMap dataJson;
    private String whereString;
    private String type;
    private String baseTableName;
    private String baseTableKeySpace;

    public String getBaseTableKeySpace() {
        return baseTableKeySpace;
    }

    public void setBaseTableKeySpace(String baseTableKeySpace) {
        this.baseTableKeySpace = baseTableKeySpace;
    }

    private Table viewTable;
//    private Table baseTable; TODO: Derive the base table structure and set up the base table object using ColumnFamily

//    public Table getBaseTable() {
//        return baseTable;
//    }
//
//    public void setBaseTable(Table baseTable) {
//        this.baseTable = baseTable;
//    }

    private String keyspace;

    public String getViewKeyspace() {
        return keyspace;
    }

    public void setViewKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public Table getViewTable() {
        return viewTable;
    }

    public void setViewTable(Table viewTable) {
        this.viewTable = viewTable;
    }

    public LinkedTreeMap getDataJson() {
        return dataJson;
    }

    public void setDataJson(LinkedTreeMap dataJson) {
        this.dataJson = dataJson;
    }

    public String getWhereString() {
        return whereString;
    }

    public void setWhereString(String whereString) {
        this.whereString = whereString;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getBaseTableName() {
        return baseTableName;
    }

    public void setBaseTableName(String baseTableName) {
        this.baseTableName = baseTableName;
    }
}
