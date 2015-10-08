package de.tum.viewmaintenance.Evaluation;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import de.tum.viewmaintenance.OperationsManagement.OperationsGenerator;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shazra on 10/4/15.
 */
public class BatchProcessing {

    private static final Logger logger = Logger.getLogger("EVALUATION");
    OperationsGenerator operationsGenerator = null;
    Table viewConfig = null;
    String ipInUse = "";

    public BatchProcessing(OperationsGenerator operationsGenerator, Table viewConfig) {
        this.operationsGenerator = operationsGenerator;
        this.viewConfig = viewConfig;
        this.ipInUse = operationsGenerator.getIpsInvolved().get(0);
    }

    public void executeView1() {
        long startBatchProcTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from("schematest", "emp");
        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);
        logger.info("Total initial records fetched in Batch Processing = " + records.size());
        int totalRecords = 0;

        for ( Row record : records ) {
            if ( record.getInt("age") > 35 ) {
                totalRecords++;
            }
        }
        long stopBatchProcTimer = System.currentTimeMillis();
        logger.info("### Batch time stats: " + (stopBatchProcTimer - startBatchProcTimer));
        logger.info("### (BP)View1 stats for " + operationsGenerator.getNumOfKeys() + " keys per node and " +
                operationsGenerator.getNumOfOperations() + " operations #### ");
        logger.info("Total records(after batch processing) = " + totalRecords);
    }

    public void executeView2() {
        long startBatchProcTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from("schematest", "emp");
        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);
        logger.info("Total initial records fetched in Batch Processing = " + records.size());
        int totalRecords = 0;
        Map<String, Integer> result = new HashMap<>();
        for ( Row record : records ) {
            if ( record.getInt("age") > 35 ) {
                if (result.containsKey(record.getString("colaggkey_x"))) {
                    result.put(record.getString("colaggkey_x"), (result.get(record.getString("colaggkey_x")) + 1));
                } else {
                    result.put(record.getString("colaggkey_x"), 1);
                }

                totalRecords++;
            }
        }
        long stopBatchProcTimer = System.currentTimeMillis();
        logger.info("### Batch time stats: " + (stopBatchProcTimer - startBatchProcTimer));
        logger.info("### Batch processing results :: " + result);
        logger.info("### (BP)View1 stats for " + operationsGenerator.getNumOfKeys() + " keys per node and " +
                operationsGenerator.getNumOfOperations() + " operations #### ");
        logger.info("Total records(after batch processing) = " + totalRecords);
    }

    public void executeView3() {
        long startBatchProcTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from("schematest", "emp");
        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);
        logger.info("Total initial records fetched in Batch Processing = " + records.size());
        int totalRecords = 0;
        Map<String, Integer> result = new HashMap<>();
        for ( Row record : records ) {
            if ( record.getInt("age") > 35 ) {
                if (result.containsKey(record.getString("colaggkey_x"))) {
                    result.put(record.getString("colaggkey_x"), (result.get(record.getString("colaggkey_x"))
                            + record.getInt("age")));
                } else {
                    result.put(record.getString("colaggkey_x"), record.getInt("age"));
                }

                totalRecords++;
            }
        }
        long stopBatchProcTimer = System.currentTimeMillis();
        logger.info("### Batch time stats: " + (stopBatchProcTimer - startBatchProcTimer));
        logger.info("### Batch processing results :: " + result);
        logger.info("### (BP)View1 stats for " + operationsGenerator.getNumOfKeys() + " keys per node and " +
                operationsGenerator.getNumOfOperations() + " operations #### ");
        logger.info("Total records(after batch processing) = " + totalRecords);
    }

    public void executeView4() {

    }

    public void executeView5() {
        long startBatchProcTimer = System.currentTimeMillis();
        Statement viewFetchQueryEmp = QueryBuilder.select().all().from("schematest", "emp");
        Statement viewFetchQuerySal = QueryBuilder.select().all().from("schematest", "salary");
        List<Row> empRecords = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQueryEmp);
        List<Row> salRecords = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQueryEmp);
        logger.info("Total initial Emp records fetched in Batch Processing = " + empRecords.size());
        logger.info("Total initial sal records fetched in Batch Processing = " + salRecords.size());

        int totalRecords = 0;
        Map<String, Integer> result = new HashMap<>();
        for ( Row record : empRecords ) {
            if ( record.getInt("age") > 35 ) {
                if (result.containsKey(record.getString("colaggkey_x"))) {
                    result.put(record.getString("colaggkey_x"), (result.get(record.getString("colaggkey_x"))
                            + record.getInt("age")));
                } else {
                    result.put(record.getString("colaggkey_x"), record.getInt("age"));
                }

                totalRecords++;
            }
        }
        long stopBatchProcTimer = System.currentTimeMillis();
        logger.info("### Batch time stats: " + (stopBatchProcTimer - startBatchProcTimer));
        logger.info("### Batch processing results :: " + result);
        logger.info("### (BP)View1 stats for " + operationsGenerator.getNumOfKeys() + " keys per node and " +
                operationsGenerator.getNumOfOperations() + " operations #### ");
        logger.info("Total records(after batch processing) = " + totalRecords);
    }

    public void executeView6() {

    }

    public void executeView7() {

    }

    public void executeView8() {

    }

    public void executeView9() {

    }

    public void executeView10() {

    }

    public void executeView11() {

    }

    public void executeView12() {

    }

    public void executeView13() {

    }

    public void executeView14() {

    }

    public void executeView15() {

    }

}
