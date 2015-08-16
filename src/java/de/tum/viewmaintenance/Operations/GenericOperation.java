package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import de.tum.viewmaintenance.view_table_structure.Table;

import java.util.List;

/**
 * Created by anarchy on 8/13/15.
 */
public abstract class GenericOperation {
    private Row deltaTableRecord;
    private GenericOperation sqlOperation;
    private List<Table> inputViewTable;
    private List<Table> operationViewTable;

    public void processOperation(String type) {
        if (type.equalsIgnoreCase("insert")) {
            insertTrigger();
        } else if (type.equalsIgnoreCase("update")) {
            updateTrigger();
        } else if (type.equalsIgnoreCase("delete")) {
            deleteTrigger();
        }
    }
    public abstract boolean insertTrigger();
    public abstract boolean updateTrigger();
    public abstract boolean deleteTrigger();

}
