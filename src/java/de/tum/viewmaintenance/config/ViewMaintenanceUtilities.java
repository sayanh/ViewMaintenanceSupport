package de.tum.viewmaintenance.config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.gson.internal.LinkedTreeMap;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.trigger.TriggerRequest;
import de.tum.viewmaintenance.view_table_structure.Table;
import de.tum.viewmaintenance.view_table_structure.Views;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by shazra on 6/27/15.
 *
 * This class contains various utilities pertaining to the view maintenance.
 */
public class ViewMaintenanceUtilities {
    private static final Logger logger = LoggerFactory.getLogger(ViewMaintenanceUtilities.class);

    public static JSONObject getTableDefinitition(String keyspaceName, String tableName) {
        JSONObject finalJsonTableDef = new JSONObject();
        // Getting the CFMetadata for a particular table
        CFMetaData cfMetaData = Schema.instance.getCFMetaData(keyspaceName, tableName);
        Collection<ColumnDefinition> columnFamilyCollection = cfMetaData.allColumns();
        for (ColumnDefinition columnDefinition : columnFamilyCollection) {
            logger.debug("ViewMaintenanceUtilities | Column Definition : {}", columnDefinition);
            finalJsonTableDef.put(columnDefinition.name, columnDefinition);
        }

        logger.debug("The json definition for table ={}", finalJsonTableDef);

        return finalJsonTableDef;
    }
}
