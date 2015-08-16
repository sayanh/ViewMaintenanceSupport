package de.tum.viewmaintenance.view_table_structure;

import com.datastax.driver.core.Cluster;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import net.sf.jsqlparser.statement.select.Join;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by shazra on 8/14/15.
 */


public class InnerJoinViewTable implements ViewTable {
    private List<Table> tables;
    private Table inputReverseJoinTableStruc;

    private boolean shouldBeMaterialized = false;

    private Table viewConfig;

    private String TABLE_PREFIX;

    private static final Logger logger = LoggerFactory.getLogger(InnerJoinViewTable.class);
    /**
     * Naming convention for inner join view tables: <view_name>_innerjoin_<base_table_name1>_<base_table_name2>
     **/

    @Override
    public List<Table> createTable() {
        List<Table> tablesCreated = new ArrayList<>();
        Table newViewTable = new Table();
        newViewTable.setName(inputReverseJoinTableStruc.getName().replaceAll("reverse", "inner"));
        newViewTable.setColumns(inputReverseJoinTableStruc.getColumns());
        tablesCreated.add(newViewTable);
        tables = tablesCreated;
        return tables;
    }

    @Override
    public void deleteTable() {

    }

    @Override
    public void materialize() {
        for (Table newTable : getTables()) {
            logger.debug(" Table getting materialized :: " + newTable);
            Cluster cluster = CassandraClientUtilities.getConnection("localhost");
            CassandraClientUtilities.createTable(cluster, newTable);
            CassandraClientUtilities.closeConnection(cluster);
        }
    }


    @Override
    public boolean shouldBeMaterialized() {
        return false;
    }

    @Override
    public void createInMemory(List<Table> tables) {

    }

    public List<Table> getTables() {
        return tables;
    }

    private void setTables(List<Table> tables) {
        this.tables = tables;
    }

    public Table getInputReverseJoinTableStruc() {
        return inputReverseJoinTableStruc;
    }

    public void setInputReverseJoinTableStruc(Table inputReverseJoinTableStruc) {
        this.inputReverseJoinTableStruc = inputReverseJoinTableStruc;
    }

    public boolean isShouldBeMaterialized() {
        return shouldBeMaterialized;
    }

    public void setShouldBeMaterialized(boolean shouldBeMaterialized) {
        this.shouldBeMaterialized = shouldBeMaterialized;
    }

    public Table getViewConfig() {
        return viewConfig;
    }

    public void setViewConfig(Table viewConfig) {
        this.viewConfig = viewConfig;
        TABLE_PREFIX = viewConfig.getName() + "_innerjoin_";
    }

}
