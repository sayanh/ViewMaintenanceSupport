package de.tum.viewmaintenance.OperationsManagement;

import de.tum.viewmaintenance.client.CassandraClient;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * Created by shazra on 10/2/15.
 */
public final class OperationsUtils {
    private static final Logger logger = LoggerFactory.getLogger(OperationsUtils.class);

    public static int getRandomInteger(int aStart, int aEnd) {
        if ( aStart > aEnd ) {
            throw new IllegalArgumentException("Start cannot exceed End.");
        }
        Random aRandom = new Random();
        //get the range, casting to long to avoid overflow problems
        long range = (long) aEnd - (long) aStart + 1;
        // compute a fraction of the range, 0 <= frac < range
        long fraction = (long) (range * aRandom.nextDouble());
        int randomNumber = (int) (fraction + aStart);
        System.out.printf("Generated : " + randomNumber);
        return randomNumber;
    }


    public static void displayOperationsList(List<String> operationsList) {
        logger.debug("##  operations list ## ");
        for ( String operation : operationsList ) {
            logger.debug(" query: " + operation);
        }
    }


    public static void pumpInOperationsIntoCassandra(String ip, List<String> operationsList,
                                                     int intervalBetweenOperations) {
        logger.debug("#### Pumping operations at the rate of " + intervalBetweenOperations);

        for ( String operation : operationsList ) {
            System.out.println("Query :: " + operation);
            CassandraClientUtilities.commandExecution(ip, operation);

            try {
                Thread.sleep(intervalBetweenOperations);
            } catch ( InterruptedException e ) {
                e.printStackTrace();
            }
        }
    }

    public static String getOperationNameForPlots(String viewName) {

        String finalName = "";

        switch (viewName) {
            case "vt1":
                finalName = "select";
                break;
            case "vt2":
                finalName = "count";
                break;
            case "vt3":
                finalName = "sum";
                break;
            case "vt4":
                finalName = "pre_aggregation";
                break;
            case "vt5":
                finalName = "reverse_join";
                break;
            case "vt6":
                finalName = "count_sum_combined";
                break;
            case "vt7":
                finalName = "pattern_select_all_where";
                break;
            case "vt8":
                finalName = "pattern_select_where";
                break;
            case "vt9":
                finalName = "pattern_select_where2";
                break;
            case "vt10":
                finalName = "pattern_select_count";
                break;
            case "vt11":
                finalName = "pattern_select_sum";
                break;
            case "vt12":
                finalName = "pattern_select_count_where";
                break;
            case "vt13":
                finalName = "pattern_select_sum_where_having";
                break;
            case "vt14":
                finalName = "pattern_select_innerjoin_where";
                break;
            case "vt15":
                finalName = "pattern_select_innerjoin_sum_where_having";
                break;
            case "vt16":
                finalName = "max";
                break;
            case "vt17":
                finalName = "min";
                break;
        }

        return finalName;

    }

}
