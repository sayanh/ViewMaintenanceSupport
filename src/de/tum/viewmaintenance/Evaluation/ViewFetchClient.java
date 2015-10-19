package de.tum.viewmaintenance.Evaluation;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import de.tum.viewmaintenance.OperationsManagement.OperationsGenerator;
import de.tum.viewmaintenance.client.CassandraClient;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by shazra on 10/4/15.
 */
public class ViewFetchClient {

    private static final Logger logger = Logger.getLogger("EVALUATION");
    OperationsGenerator operationsGenerator = null;
    Table viewConfig = null;
    String ipInUse = "";

    public ViewFetchClient(OperationsGenerator operationsGenerator, Table viewConfig) {
        this.operationsGenerator = operationsGenerator;
        this.viewConfig = viewConfig;
        this.ipInUse = operationsGenerator.getIpsInvolved().get(0);
    }

    public void executeView1() {
        long startViewTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from(viewConfig.getKeySpace(), viewConfig.getName());

        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);

        long stopViewTimer = System.currentTimeMillis();
        logger.info("### View time stats: " + (stopViewTimer - startViewTimer));
        logger.info("Total fetch records(From Views) = " + records.size());

    }

    public void executeView2() {
        long startViewTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from(viewConfig.getKeySpace(), viewConfig.getName());

        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);

        long stopViewTimer = System.currentTimeMillis();
        Map<String, Integer> result = new HashMap<>();
        for ( Row record : records ) {
            result.put(record.getString("colaggkey_x"), record.getInt("count_view2_age"));
        }

        logger.info("### View time stats: " + (stopViewTimer - startViewTimer));
        logger.info("Output (From Views) = " + result);
    }

    public void executeView3() {
        long startViewTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from(viewConfig.getKeySpace(), viewConfig.getName());

        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);

        long stopViewTimer = System.currentTimeMillis();
        Map<String, Integer> result = new HashMap<>();
        for ( Row record : records ) {
            result.put(record.getString("colaggkey_x"), record.getInt("sum_view2_age"));
        }

        logger.info("### View time stats: " + (stopViewTimer - startViewTimer));
        logger.info("Output (From Views) = " + result);
    }

    public void executeView4() {
        long startViewTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from(viewConfig.getKeySpace(), viewConfig.getName());

        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);


        Map<String, Integer> resultSum = new HashMap<>();
        Map<String, Integer> resultCount = new HashMap<>();
        Map<String, Integer> resultMax = new HashMap<>();
        Map<String, Integer> resultMin = new HashMap<>();

        for ( Row record : records ) {
            int sum = 0;
            int count = 0;
            int max = 0;
            int min = 999;
            Map<Integer, String> tempMap = record.getMap("schematest_emp", Integer.class, String.class);
            for ( Map.Entry<Integer, String> tempMapEntry : tempMap.entrySet() ) {
                int tempAge = Integer.parseInt(tempMapEntry.getValue());
                if ( tempAge > 35 ) {
                    sum = sum + tempAge;
                    count = count + 1;
                    if (max < tempAge) {
                        max = tempAge;
                    }
                    if (min > tempAge) {
                        min = tempAge;
                    }
                }
            }

            resultSum.put(record.getString("colaggkey_x"), sum);
            resultCount.put(record.getString("colaggkey_x"), count);
            resultMax.put(record.getString("colaggkey_x"), max);
            resultMin.put(record.getString("colaggkey_x"), min);
        }

        long stopViewTimer = System.currentTimeMillis();
        logger.info("### View time stats: " + (stopViewTimer - startViewTimer));
        logger.info("Output (From Views) sum= " + resultSum);
        logger.info("Output (From Views) count= " + resultCount);
        logger.info("Output (From Views) max= " + resultMax);
        logger.info("Output (From Views) min= " + resultMin);

    }

    public void executeView5() {
        long startViewTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from(viewConfig.getKeySpace(), viewConfig.getName());

        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);

        long stopViewTimer = System.currentTimeMillis();


        logger.info("### View time stats: " + (stopViewTimer - startViewTimer));
        logger.info("Output (From Views) = " + records);
        logger.info("Output size (From Views) = " + records.size());
    }

    public static void executeView6() {

    }

    public static void executeView7() {

    }

    public void executeView8() {
        long startViewTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from(viewConfig.getKeySpace(), "vt8_result");

        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);

        long stopViewTimer = System.currentTimeMillis();
        logger.info("### View time stats: " + (stopViewTimer - startViewTimer));
        logger.info("Output (From Views) = " + records.size());
    }

    public void executeView9() {
        long startViewTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from(viewConfig.getKeySpace(), "vt9_result");

        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);

        long stopViewTimer = System.currentTimeMillis();
        logger.info("### View time stats: " + (stopViewTimer - startViewTimer));
        logger.info("Output (From Views) = " + records.size());
    }

    public void executeView10() {
        long startViewTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from(viewConfig.getKeySpace(), "vt10_result");

        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);

        long stopViewTimer = System.currentTimeMillis();
        logger.info("### View time stats: " + (stopViewTimer - startViewTimer));
        logger.info("Output (From Views) = " + records);

    }

    public void executeView11() {

        long startViewTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from(viewConfig.getKeySpace(), "vt11_result");
        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);
        long stopViewTimer = System.currentTimeMillis();
        logger.info("### View time stats: " + (stopViewTimer - startViewTimer));
        logger.info("Output (From Views) = " + records);
    }

    public void executeView12() {
        long startViewTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from(viewConfig.getKeySpace(), "vt12_result");
        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);
        long stopViewTimer = System.currentTimeMillis();
        logger.info("### View time stats: " + (stopViewTimer - startViewTimer));
        logger.info("Output (From Views) = " + records);
    }

    public void executeView13() {
        long startViewTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from(viewConfig.getKeySpace(), "vt13_result");
        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);
        long stopViewTimer = System.currentTimeMillis();
        logger.info("### View time stats: " + (stopViewTimer - startViewTimer));
        logger.info("Output (From Views) = " + records);
    }

    public void executeView14() {
        long startViewTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from(viewConfig.getKeySpace(), "vt14_innerjoin_emp_salary");
        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);
        long stopViewTimer = System.currentTimeMillis();
        logger.info("### View time stats: " + (stopViewTimer - startViewTimer));
        logger.info("Output (From Views) = " + records);
    }

    public void executeView15() {
        long startViewTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from(viewConfig.getKeySpace(), "vt15_agg");
        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);
        long stopViewTimer = System.currentTimeMillis();
        logger.info("### View time stats: " + (stopViewTimer - startViewTimer));
        logger.info("Output (From Views) = " + records);
    }

    public void executeView16() {

    }

    public void executeView17() {

    }


}
