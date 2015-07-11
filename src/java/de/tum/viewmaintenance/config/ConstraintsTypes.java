package de.tum.viewmaintenance.config;

/**
 * Created by shazra on 6/27/15.
 */
public class ConstraintsTypes {
    public enum Constraint {
        GREATER_THAN,
        LESS_THAN,
        EQUAL_TO;
        public static String getValue(Constraint constraint) {
            switch (constraint) {
                case GREATER_THAN:
                    return "greaterthan";
                case LESS_THAN:
                    return "lessthan";
                case EQUAL_TO:
                    return "equalto";
                default:
                    return "";
            }
        }

    }
}
