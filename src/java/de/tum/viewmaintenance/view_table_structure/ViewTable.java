package de.tum.viewmaintenance.view_table_structure;

import java.util.List;

/**
 * Created by shazra on 8/14/15.
 */
public interface ViewTable {
    public List<Table> createTable();
    public boolean deleteTable();
    public boolean materializeTable();
    public boolean shouldBeMaterialized();
}
