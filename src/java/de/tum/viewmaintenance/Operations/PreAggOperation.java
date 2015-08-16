package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import de.tum.viewmaintenance.view_table_structure.Table;

import java.util.List;

/**
 * Created by shazra on 8/14/15.
 */
public class PreAggOperation extends GenericOperation {

    private Row deltaTableRecord;
    private List<Table> inputViewTables;
    private List<Table> operationViewTables;

    public List<Table> getInputViewTables() {
        return inputViewTables;
    }

    public void setInputViewTables(List<Table> inputViewTables) {
        this.inputViewTables = inputViewTables;
    }

    public List<Table> getOperationViewTables() {
        return operationViewTables;
    }

    public void setOperationViewTables(List<Table> operationViewTables) {
        this.operationViewTables = operationViewTables;
    }

    public Row getDeltaTableRecord() {
        return deltaTableRecord;
    }

    public void setDeltaTableRecord(Row deltaTableRecord) {
        this.deltaTableRecord = deltaTableRecord;
    }

    public static PreAggOperation getInstance(Row deltaTableRecord, List<Table> inputViewTables,
                                              List<Table> operationViewTables) {
        PreAggOperation preAggOperation = new PreAggOperation();
        preAggOperation.setDeltaTableRecord(deltaTableRecord);
        preAggOperation.setInputViewTables(inputViewTables);
        preAggOperation.setOperationViewTables(operationViewTables);
        return preAggOperation;
    }

    @Override
    public boolean insertTrigger() {
        return false;
    }

    @Override
    public boolean updateTrigger() {
        return false;
    }

    @Override
    public boolean deleteTrigger() {
        return false;
    }

}
