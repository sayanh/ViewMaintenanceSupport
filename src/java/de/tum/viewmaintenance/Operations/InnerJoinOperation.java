package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import de.tum.viewmaintenance.view_table_structure.Table;

import java.util.List;

/**
 * Created by shazra on 8/14/15.
 */
public class InnerJoinOperation extends GenericOperation {

    private Row deltaTableRecord;
    private List<Table> inputViewTables;
    private List<Table> operationViewTable;

    public Row getDeltaTableRecord() {
        return deltaTableRecord;
    }

    public List<Table> getInputViewTable() {
        return inputViewTables;
    }

    public void setInputViewTable(List<Table> inputViewTables) {
        this.inputViewTables = inputViewTables;
    }

    public List<Table> getOperationViewTable() {
        return operationViewTable;
    }

    public void setOperationViewTable(List<Table> operationViewTable) {
        this.operationViewTable = operationViewTable;
    }

    public void setDeltaTableRecord(Row deltaTableRecord) {
        this.deltaTableRecord = deltaTableRecord;
    }

    public static InnerJoinOperation getInstance(Row deltaTableRecord, List<Table> inputViewTable,
                                                 List<Table> operationViewTable) {
        InnerJoinOperation innerJoinOperation = new InnerJoinOperation();
        innerJoinOperation.setDeltaTableRecord(deltaTableRecord);
        innerJoinOperation.setInputViewTable(inputViewTable);
        innerJoinOperation.setOperationViewTable(operationViewTable);
        return innerJoinOperation;
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
