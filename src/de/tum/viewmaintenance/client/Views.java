package de.tum.viewmaintenance.client;

import de.tum.viewmaintenance.viewsTableStructure.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/*
 * Created by shazra on 6/20/15.
 * This singleton class holds the tables(with its structure) intended for the views.
 */

public class Views {
    protected static final Logger logger = LoggerFactory.getLogger(Views.class);
    private static volatile Views instance;
    private List<Table> tables;
    private String keyspace;

    private Views() {
        logger.debug("Created instance for views= "+ instance);
    }

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
