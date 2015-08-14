package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import de.tum.viewmaintenance.view_table_structure.Table;

/**
 * Created by anarchy on 8/13/15.
 */
public abstract class GenericOperation {
    private Row deltaTableRecord;
    private GenericOperation sqlOperation;
    private Table inputViewTable;
    private Table operationViewTable;

    public abstract boolean insertTrigger();
    public abstract boolean updateTrigger();
    public abstract boolean deleteTrigger();

}
