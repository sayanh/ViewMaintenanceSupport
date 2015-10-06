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
    public static int getRandomInteger(int aStart, int aEnd){
        if (aStart > aEnd) {
            throw new IllegalArgumentException("Start cannot exceed End.");
        }
        Random aRandom = new Random();
        //get the range, casting to long to avoid overflow problems
        long range = (long)aEnd - (long)aStart + 1;
        // compute a fraction of the range, 0 <= frac < range
        long fraction = (long)(range * aRandom.nextDouble());
        int randomNumber =  (int)(fraction + aStart);
        System.out.printf("Generated : " + randomNumber);
        return randomNumber;
    }


    public  static void displayOperationsList(List<String> operationsList) {
        logger.debug("##  operations list ## ");
        for ( String operation: operationsList) {
            logger.debug(" query: " + operation);
        }
    }


    public static void pumpInOperationsIntoCassandra(String ip, List<String> operationsList,
                                                     int intervalBetweenOperations) {
        logger.debug("#### Pumping operations at the rate of " + intervalBetweenOperations);

        for (String operation: operationsList) {
            CassandraClientUtilities.commandExecution(ip, operation);

            try {
                Thread.sleep(intervalBetweenOperations);
            } catch ( InterruptedException e ) {
                e.printStackTrace();
            }
        }
    }

}
