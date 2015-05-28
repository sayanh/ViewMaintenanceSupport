package de.tum.viewmaintenance.client;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 * @author shazra on 5/27/15.
 */
public class DatabaseConfig {
    public static void main(String[] args) {
        XMLConfiguration config = new XMLConfiguration();
        config.setDelimiterParsingDisabled(true);
        try {
            config.load("VMDatabaseConfig.xml");
            List<String> tabledefs = config.getList("dbSchema.tableDefinition.name");
            for (int i = 0; i < tabledefs.size(); i++) {
                String name = config.getString("dbSchema.tableDefinition("+i+").name");

                String keyPrefix = config.getString("dbSchema.tableDefinition("+i+").primaryKey.prefix");
                String keyStartRange = config.getString("dbSchema.tableDefinition("+i+").primaryKey.startRange");
                String keyEndRange = config.getString("dbSchema.tableDefinition(" + i + ").primaryKey.endRange");
                System.out.println("table details: " + name + "|" + keyPrefix + "|" + keyStartRange + "|" + keyEndRange);
                List<String> coldefs = config.getList("dbSchema.tableDefinition("+i+").column.name");
                for (int x = 0; x < coldefs.size(); x++) {
                    String colName = config.getString("dbSchema.tableDefinition(" + i + ").column(" + x + ").name");
                    String colFamily = config.getString("dbSchema.tableDefinition(" + i + ").column(" + x + ").family");
                    String colPrefix = config.getString("dbSchema.tableDefinition(" + i + ").column(" + x + ").prefix");
                    String colStartRange = config.getString("dbSchema.tableDefinition(" + i + ").column(" + x + ").startRange");
                    String colEndRange = config.getString("dbSchema.tableDefinition(" + i + ").column(" + x + ").endRange");
                    System.out.println(colName + "|" + colFamily + "|" + colPrefix + "|" + colStartRange + "|" + colEndRange);
                }

                // Creating table
//                frameCreateTableCQL();
// Create table <table name> ( <primary key name> int PRIMARY KEY, colAggKey int, colAggVal int ) ;
            }
        } catch (ConfigurationException cex) {
            cex.printStackTrace();
        }
    }

//    public static String frameCreateTable() {
//
//    }
}
