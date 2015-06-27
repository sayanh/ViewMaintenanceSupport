package de.tum.viewmaintenance.trigger;

import com.google.gson.internal.LinkedTreeMap;

/**
 * Created by anarchy on 6/27/15.
 */
public class TriggerRequest {
    private LinkedTreeMap dataJson;
    private String whereString;
    private String type;
    private String baseTableName;

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
