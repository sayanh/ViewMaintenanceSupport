package de.tum.viewmaintenance.view_table_structure;

import java.util.List;

/**
 * Created by shazra on 8/14/15.
 */
public interface ViewTable {
    public List<Table> createTable();
    public void deleteTable();
    public void materialize();
    public boolean shouldBeMaterialized();
    public void createInMemory(List<Table> tables);
}
