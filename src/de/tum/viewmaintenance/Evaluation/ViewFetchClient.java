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
        for ( Row record: records) {
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
        for ( Row record: records) {
            result.put(record.getString("colaggkey_x"), record.getInt("sum_view2_age"));
        }

        logger.info("### View time stats: " + (stopViewTimer - startViewTimer));
        logger.info("Output (From Views) = " + result);
    }

    public static void executeView4() {

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

    public static void executeView9() {

    }

    public static void executeView10() {

    }

    public static void executeView11() {

    }

    public static void executeView12() {

    }

    public static void executeView13() {

    }

    public static void executeView14() {

    }

    public static void executeView15() {

    }

}
