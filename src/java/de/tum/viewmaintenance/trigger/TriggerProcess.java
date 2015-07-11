package de.tum.viewmaintenance.trigger;

/**
 * Created by shazra on 6/27/15.
 */

public abstract class TriggerProcess {
    public abstract TriggerResponse insertTrigger(TriggerRequest request);
    public abstract TriggerResponse updateTrigger(TriggerRequest request);
    public abstract TriggerResponse deleteTrigger(TriggerRequest request);
}
