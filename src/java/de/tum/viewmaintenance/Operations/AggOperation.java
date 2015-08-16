package de.tum.viewmaintenance.Operations;

import com.datastax.driver.core.Row;
import de.tum.viewmaintenance.view_table_structure.Table;
import net.sf.jsqlparser.expression.Expression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by shazra on 8/14/15.
 */
public class AggOperation extends GenericOperation {

    private Row deltaTableRecord;
    private List<Table> inputViewTables;
    private List<Table> operationViewTables;
    private Expression havingExpression;
    private static final Logger logger = LoggerFactory.getLogger(AggOperation.class);

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

    public Expression getHavingExpression() {
        return havingExpression;
    }

    public void setHavingExpression(Expression havingExpression) {
        this.havingExpression = havingExpression;
    }

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
        return operationViewTables;
    }

    public void setOperationViewTable(List<Table> operationViewTables) {
        this.operationViewTables = operationViewTables;
    }

    public void setDeltaTableRecord(Row deltaTableRecord) {
        this.deltaTableRecord = deltaTableRecord;
    }

    public static AggOperation getInstance(Row deltaTableRecord, List<Table> inputViewTables,
                                                 List<Table> operationViewTables) {
        AggOperation aggOperation = new AggOperation();
        aggOperation.setDeltaTableRecord(deltaTableRecord);
        aggOperation.setInputViewTable(inputViewTables);
        aggOperation.setOperationViewTable(operationViewTables);
        return aggOperation;
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
