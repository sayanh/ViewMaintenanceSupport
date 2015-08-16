package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by shazra on 8/16/15.
 */
public class ResultViewOperation extends GenericOperation{

    private static final Logger logger = LoggerFactory.getLogger(ResultViewOperation.class);
    private Row deltaTableRecord;
    private List<Table> inputViewTables;
    private List<Table> operationViewTables;

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

    public static ResultViewOperation getInstance(Row deltaTableRecord, List<Table> inputViewTables,
                                                  List<Table> operationViewTables) {
        ResultViewOperation resultViewOperation = new ResultViewOperation();
        resultViewOperation.setInputViewTables(inputViewTables);

        return resultViewOperation;
    }

    public Row getDeltaTableRecord() {
        return deltaTableRecord;
    }

    public void setDeltaTableRecord(Row deltaTableRecord) {
        this.deltaTableRecord = deltaTableRecord;
    }

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

}
