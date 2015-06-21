package de.tum.viewmaintenance.view_table_structure;

import java.util.List;

/*
 * Created by shazra on 6/20/15.
 * This singleton class holds the tables(with its structure) intended for the views.
 */

public class Views {
    private static volatile Views instance;
    private List<Table> tables;
    private String keyspace;

    public List<Table> getTables() {
        return tables;
    }

    public void setTables(List<Table> tables) {
        this.tables = tables;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public static Views getInstance() {
        if (instance == null ) {
            synchronized (Views.class) {
                if (instance == null) {
                    instance = new Views();
                }
            }
        }

        return instance;
    }

}
