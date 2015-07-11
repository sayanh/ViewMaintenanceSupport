package de.tum.viewmaintenance.trigger;

import com.datastax.driver.core.Row;

/**
 * Created by anarchy on 6/27/15.
 */
public class TriggerResponse {
    private boolean isSuccess = false;

    private Row deletedRowFromDeltaView = null;

    public Row getDeletedRowFromDeltaView() {
        return deletedRowFromDeltaView;
    }

    public void setDeletedRowFromDeltaView(Row deletedRowFromDeltaView) {
        this.deletedRowFromDeltaView = deletedRowFromDeltaView;
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public void setIsSuccess(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }
}
