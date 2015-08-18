package de.tum.viewmaintenance.view_table_structure;

import com.datastax.driver.core.Cluster;
import de.tum.viewmaintenance.Operations.AggOperation;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import net.sf.jsqlparser.expression.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.List;

/**
 * Created by anarchy on 8/14/15.
 */
public class AggViewTable implements ViewTable{
    private List<Table> tables;
    private Table inputPreAggTableStruc;

    private boolean shouldBeMaterialized = false;

    private Table viewConfig;

    private String TABLE_PREFIX;
    private static final Logger logger = LoggerFactory.getLogger(AggOperation.class);

    /**
     * Naming convention for inner join view tables: <view_name>_agg
     **/

    @Override
    public List<Table> createTable() {
        logger.debug("###### Creating table for AggViewTable ######");
        List<Table> newTablesCreated = new ArrayList<>();
        Table newViewTable = new Table();
        newViewTable.setName(TABLE_PREFIX);
        newViewTable.setColumns(inputPreAggTableStruc.getColumns());
        newTablesCreated.add(newViewTable);
        logger.debug("### AggViewTable structure created as :: " + newTablesCreated );
        tables = newTablesCreated;
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
        return shouldBeMaterialized;
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

    public Table getViewConfig() {
        return viewConfig;
    }

    public void setViewConfig(Table viewConfig) {
        this.viewConfig = viewConfig;
        TABLE_PREFIX = viewConfig.getName() + "_agg";
    }

    public Table getInputPreAggTableStruc() {

        return inputPreAggTableStruc;
    }

    public void setInputPreAggTableStruc(Table inputPreAggTableStruc) {
        this.inputPreAggTableStruc = inputPreAggTableStruc;
    }

    public void setShouldBeMaterialized(boolean shouldBeMaterialized) {
        this.shouldBeMaterialized = shouldBeMaterialized;
    }

}
