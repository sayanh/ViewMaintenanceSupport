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

    public static Map<String, ColumnDefinition> getTableDefinitition(String keyspaceName, String tableName) {
        Map<String, ColumnDefinition> tableStrucMap = new HashMap<>();
        // Getting the CFMetadata for a particular table
        CFMetaData cfMetaData = Schema.instance.getCFMetaData(keyspaceName, tableName);
        Collection<ColumnDefinition> columnFamilyCollection = cfMetaData.allColumns();
        for (ColumnDefinition columnDefinition : columnFamilyCollection) {
//            logger.debug("ViewMaintenanceUtilities | Column Definition : {}", columnDefinition);
            tableStrucMap.put(columnDefinition.name + "", columnDefinition);
        }

//        logger.debug("The Map of the table::{} definition ={}", keyspaceName + "." + tableName, tableStrucMap);
        return tableStrucMap;
    }


    /**
    *  It returns an equivalent Java datatype for an entered Cassandra type
    **/
    public static String getJavaTypeFromCassandraType(String cassandraType) {
        String javaType = "";
        logger.debug(" The cassandra type received is " + cassandraType);

        if (cassandraType.equalsIgnoreCase("org.apache.cassandra.db.marshal.UTF8Type")) {
            javaType = "String";
        } else if (cassandraType.equalsIgnoreCase("org.apache.cassandra.db.marshal.Int32Type")) {
            javaType = "Integer";
        }
        return javaType;
    }

    /**
     * It returns an equivalent CQL3 data type from Cassandra's internal data type
     **/
    public static String getCQL3DataTypeFromCassandraInternalDataType(String internalDataType) {
        String cql3Type = "";
        logger.debug(" The cassandra type received is " + internalDataType);

        if (internalDataType.equalsIgnoreCase("org.apache.cassandra.db.marshal.UTF8Type")) {
            cql3Type = "text";
        } else if (internalDataType.equalsIgnoreCase("org.apache.cassandra.db.marshal.Int32Type")) {
            cql3Type = "int";
        }
        return cql3Type;
    }

}
