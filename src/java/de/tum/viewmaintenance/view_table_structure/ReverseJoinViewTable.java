package de.tum.viewmaintenance.view_table_structure;

import java.util.List;

/**
 * Created by shazra on 8/14/15.
 */
public class ReverseJoinViewTable implements ViewTable{
    @Override
    public List<Table> createTable() {
        return null;
    }

    @Override
    public boolean deleteTable() {
        return false;
    }

    @Override
    public boolean materializeTable() {
        return false;
    }

    @Override
    public boolean shouldBeMaterialized() {
        return false;
    }
}
