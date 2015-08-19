package de.tum.viewmaintenance.client;


import de.tum.viewmaintenance.view_table_structure.Table;

import java.util.List;

/**
 * Created by shazra on 6/26/15.
 */
public class Load {
    private List<Table> tables;
    private List<String> ips;
    private int numTokensPerNode;

    public String schemaName;

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    private String strategy="uniform";

    public List<Table> getTables() {
        return tables;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
    }

    public List<String> getIps() {
        return ips;
    }

    public void setIps(List<String> ips) {
        this.ips = ips;
    }

    public int getNumTokensPerNode() {
        return numTokensPerNode;
    }

    public void setNumTokensPerNode(int numTokensPerNode) {
        this.numTokensPerNode = numTokensPerNode;
    }
}
