package de.tum.viewmaintenance.Evaluation;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import de.tum.viewmaintenance.OperationsManagement.OperationsGenerator;
import de.tum.viewmaintenance.client.CassandraClientUtilities;
import de.tum.viewmaintenance.view_table_structure.Table;
import org.apache.log4j.Logger;

import java.util.ArrayList;
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
                if ( result.containsKey(record.getString("colaggkey_x")) ) {
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
                if ( result.containsKey(record.getString("colaggkey_x")) ) {
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
        long startBatchProcTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from("schematest", "emp");
        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);
        logger.info("Total initial records fetched in Batch Processing = " + records.size());
        int totalRecords = 0;
        Map<String, Integer> result = new HashMap<>();
        Map<String, Integer> resultCount = new HashMap<>();
        Map<String, Integer> resultMax = new HashMap<>();
        Map<String, Integer> resultMin = new HashMap<>();
        for ( Row record : records ) {
            if ( record.getInt("age") > 35 ) {
                if ( resultMax.containsKey(record.getString("colaggkey_x")) ) {
                    if ( resultMax.get(record.getString("colaggkey_x")) < record.getInt("age") ) {
                    }
                    resultMax.put(record.getString("colaggkey_x"), record.getInt("age"));
                } else {
                    resultMax.put(record.getString("colaggkey_x"), record.getInt("age"));
                }

                if ( resultMin.containsKey(record.getString("colaggkey_x")) ) {
                    if ( record.getInt("age") < resultMin.get(record.getString("colaggkey_x")) ) {
                        resultMin.put(record.getString("colaggkey_x"), record.getInt("age") );
                    }
                } else {
                    resultMin.put(record.getString("colaggkey_x"), record.getInt("age"));
                }
                if ( result.containsKey(record.getString("colaggkey_x")) ) {
                    result.put(record.getString("colaggkey_x"), (result.get(record.getString("colaggkey_x"))
                            + record.getInt("age")));
                } else {
                    result.put(record.getString("colaggkey_x"), record.getInt("age"));
                }

                if ( resultCount.containsKey(record.getString("colaggkey_x")) ) {
                    resultCount.put(record.getString("colaggkey_x"), (resultCount.get(record.getString("colaggkey_x")) + 1));
                } else {
                    resultCount.put(record.getString("colaggkey_x"), 1);
                }
            }
        }
        long stopBatchProcTimer = System.currentTimeMillis();
        logger.info("### Batch time stats: " + (stopBatchProcTimer - startBatchProcTimer));
        logger.info("### Batch processing results :: " + result);
        logger.info("### (BP)View1 stats for " + operationsGenerator.getNumOfKeys() + " keys per node and " +
                operationsGenerator.getNumOfOperations() + " operations #### ");
        logger.info("Batch processing Count = " + resultCount);
        logger.info("Batch processing Max = " + resultMax);
        logger.info("Batch processing Min = " + resultMin);
    }

    public void executeView5() {
        long startBatchProcTimer = System.currentTimeMillis();
        Statement viewFetchQueryEmp = QueryBuilder.select().all().from("schematest", "emp");
        Statement viewFetchQuerySal = QueryBuilder.select().all().from("schematest", "salary");
        List<Row> empRecords = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQueryEmp);
        List<Row> salRecords = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuerySal);
        logger.info("Total initial Emp records fetched in Batch Processing = " + empRecords.size());
        logger.info("Total initial sal records fetched in Batch Processing = " + salRecords.size());

        Map<String, List<String>> joinkeyAndData = new HashMap<>(); // Format: joinkey : <emp_primarykey, age, sal_primary
        // key, salvalue >
        for ( Row record : empRecords ) {
            String joinKey = record.getString("colaggkey_x");
            for ( Row recordSal : salRecords ) {
                if ( joinKey.equalsIgnoreCase(recordSal.getString("colaggkey_x")) ) {
                    if ( joinkeyAndData.containsKey(recordSal.getString("colaggkey_x")) ) {
                        List<String> tempList = joinkeyAndData.get(record.getString("colaggkey_x"));
                        tempList.add(record.getInt("user_id") + "," + record.getInt("age") + "," + recordSal.getInt("user_id") +
                                "," + recordSal.getInt("salaryval"));
                        joinkeyAndData.put(record.getString("colaggkey_x"), tempList);
                    } else {
                        List<String> tempList = new ArrayList<>();
                        tempList.add(record.getInt("user_id") + "," + record.getInt("age") + "," + recordSal.getInt("user_id") +
                                "," + recordSal.getInt("salaryval"));
                        joinkeyAndData.put(record.getString("colaggkey_x"), tempList);
                    }
                }
            }
        }
        long stopBatchProcTimer = System.currentTimeMillis();
        logger.info("### Batch time stats: " + (stopBatchProcTimer - startBatchProcTimer));
        logger.info("### Batch processing results :: " + joinkeyAndData);
        logger.info("### (BP)View1 stats for " + operationsGenerator.getNumOfKeys() + " keys per node and " +
                operationsGenerator.getNumOfOperations() + " operations #### ");
        logger.info("Total records(after batch processing) = " + joinkeyAndData.size());
    }

    public void executeView6() {

    }

    public void executeView7() {

    }

    public void executeView8() {
        long startBatchProcTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from("schematest", "emp");
        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);
        logger.info("Total initial records fetched in Batch Processing = " + records.size());
        int totalRecords = 0;
        Map<String, Integer> result = new HashMap<>();
        for ( Row record : records ) {
            if ( record.getInt("age") > 35 ) {
                if ( result.containsKey(record.getString("colaggkey_x")) ) {
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

    public void executeView9() {
        long startBatchProcTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from("schematest", "emp");
        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);
        logger.info("Total initial records fetched in Batch Processing = " + records.size());
        int totalRecords = 0;
        Map<String, Integer> result = new HashMap<>();
        for ( Row record : records ) {
            if ( record.getInt("age") > 35 ) {
                if ( result.containsKey(record.getString("colaggkey_x")) ) {
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

    public void executeView10() {
        long startBatchProcTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from("schematest", "emp");
        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);
        logger.info("Total initial records fetched in Batch Processing = " + records.size());
        int totalRecords = 0;
        Map<String, Integer> result = new HashMap<>();
        for ( Row record : records ) {

            if ( result.containsKey(record.getString("colaggkey_x")) ) {
                result.put(record.getString("colaggkey_x"), result.get(record.getString("colaggkey_x")) + 1);
            } else {
                result.put(record.getString("colaggkey_x"), 1);
            }
        }
        long stopBatchProcTimer = System.currentTimeMillis();
        logger.info("### Batch time stats: " + (stopBatchProcTimer - startBatchProcTimer));
        logger.info("### Batch processing results :: " + result);
        logger.info("### (BP)View1 stats for " + operationsGenerator.getNumOfKeys() + " keys per node and " +
                operationsGenerator.getNumOfOperations() + " operations #### ");
        logger.info("Total records(after batch processing) = " + totalRecords);

    }

    public void executeView11() {
        long startBatchProcTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from("schematest", "emp");
        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);
        logger.info("Total initial records fetched in Batch Processing = " + records.size());
        int totalRecords = 0;
        Map<String, Integer> result = new HashMap<>();
        for ( Row record : records ) {

            if ( result.containsKey(record.getString("colaggkey_x")) ) {
                result.put(record.getString("colaggkey_x"), result.get(record.getString("colaggkey_x")) +
                        record.getInt("age"));
            } else {
                result.put(record.getString("colaggkey_x"), record.getInt("age"));
            }
        }
        long stopBatchProcTimer = System.currentTimeMillis();
        logger.info("### Batch time stats: " + (stopBatchProcTimer - startBatchProcTimer));
        logger.info("### Batch processing results :: " + result);
        logger.info("### (BP)View1 stats for " + operationsGenerator.getNumOfKeys() + " keys per node and " +
                operationsGenerator.getNumOfOperations() + " operations #### ");
        logger.info("Total records(after batch processing) = " + totalRecords);
    }

    public void executeView12() {
        long startBatchProcTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from("schematest", "emp");
        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);
        logger.info("Total initial records fetched in Batch Processing = " + records.size());
        int totalRecords = 0;
        Map<String, Integer> result = new HashMap<>();
        for ( Row record : records ) {
            if ( record.getInt("age") > 35 ) {

                if ( result.containsKey(record.getString("colaggkey_x")) ) {
                    result.put(record.getString("colaggkey_x"), result.get(record.getString("colaggkey_x")) +
                            1);
                } else {
                    result.put(record.getString("colaggkey_x"), 1);
                }
            }
        }
        long stopBatchProcTimer = System.currentTimeMillis();
        logger.info("### Batch time stats: " + (stopBatchProcTimer - startBatchProcTimer));
        logger.info("### Batch processing results :: " + result);
        logger.info("### (BP)View1 stats for " + operationsGenerator.getNumOfKeys() + " keys per node and " +
                operationsGenerator.getNumOfOperations() + " operations #### ");
        logger.info("Total records(after batch processing) = " + totalRecords);
    }

    public void executeView13() {
        long startBatchProcTimer = System.currentTimeMillis();
        Statement viewFetchQuery = QueryBuilder.select().all().from("schematest", "emp");
        List<Row> records = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuery);
        logger.info("Total initial records fetched in Batch Processing = " + records.size());
        int totalRecords = 0;
        Map<String, Integer> result = new HashMap<>();
        for ( Row record : records ) {
            if ( record.getInt("age") > 30 ) {

                if ( result.containsKey(record.getString("colaggkey_x")) ) {
                    result.put(record.getString("colaggkey_x"), result.get(record.getString("colaggkey_x")) +
                            record.getInt("age"));
                } else {
                    result.put(record.getString("colaggkey_x"), record.getInt("age"));
                }
            }
        }
        List<String> shouldBeDeletedList = new ArrayList<>();
        for ( Map.Entry<String, Integer> resultEntry : result.entrySet() ) {
            if ( resultEntry.getValue() <= 300 ) {
                shouldBeDeletedList.add(resultEntry.getKey());
            }
        }

        for ( String key : shouldBeDeletedList ) {
            result.remove(key);
        }

        long stopBatchProcTimer = System.currentTimeMillis();
        logger.info("### Batch time stats: " + (stopBatchProcTimer - startBatchProcTimer));
        logger.info("### Batch processing results :: " + result);
        logger.info("### (BP)View1 stats for " + operationsGenerator.getNumOfKeys() + " keys per node and " +
                operationsGenerator.getNumOfOperations() + " operations #### ");
        logger.info("Total records(after batch processing) = " + totalRecords);
    }

    public void executeView14() {
        long startBatchProcTimer = System.currentTimeMillis();
        Statement viewFetchQueryEmp = QueryBuilder.select().all().from("schematest", "emp");
        Statement viewFetchQuerySal = QueryBuilder.select().all().from("schematest", "salary");
        List<Row> recordsEmp = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQueryEmp);
        List<Row> recordsSal = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuerySal);
        Map<Integer, List<Row>> result = new HashMap<>();
        logger.info("Total initial records fetched in Batch Processing = " + recordsEmp.size());
        int totalRecords = 0;
        Map<Integer, List<Row>> empMap = new HashMap<>();
        Map<Integer, List<Row>> salMap = new HashMap<>();
        for ( Row record : recordsEmp ) {
            if ( record.getInt("age") > 35 ) {

                if ( empMap.containsKey(record.getInt("joinkey")) ) {
                    List<Row> tempList = empMap.get(record.getInt("joinkey"));
                    tempList.add(record);
                    empMap.put(record.getInt("joinkey"), tempList);

                } else {
                    List<Row> tempList = new ArrayList<>();
                    tempList.add(record);
                    empMap.put(record.getInt("joinkey"), tempList);
                }
            }
        }

        for ( Row record : recordsSal ) {
            if ( record.getInt("salaryval") > 3500 ) {

                if ( salMap.containsKey(record.getInt("joinkey")) ) {
                    List<Row> tempList = salMap.get(record.getInt("joinkey"));
                    tempList.add(record);
                    salMap.put(record.getInt("joinkey"), tempList);

                } else {
                    List<Row> tempList = new ArrayList<>();
                    tempList.add(record);
                    salMap.put(record.getInt("joinkey"), tempList);
                }
            }
        }

        for ( Map.Entry<Integer, List<Row>> empEntry : empMap.entrySet() ) {
            if ( salMap.containsKey(empEntry.getKey()) ) {
                List<Row> temp = new ArrayList<>();
                temp.addAll(empEntry.getValue());
                temp.addAll(salMap.get(empEntry.getKey()));
                result.put(empEntry.getKey(), temp);
            }
        }

        long stopBatchProcTimer = System.currentTimeMillis();
        logger.info("### Batch time stats: " + (stopBatchProcTimer - startBatchProcTimer));
        logger.info("### Batch processing results :: " + result);
        logger.info("### (BP)View1 stats for " + operationsGenerator.getNumOfKeys() + " keys per node and " +
                operationsGenerator.getNumOfOperations() + " operations #### ");
        logger.info("Total records(after batch processing) = " + totalRecords);
    }

    public void executeView15() {
        long startBatchProcTimer = System.currentTimeMillis();
        Statement viewFetchQueryEmp = QueryBuilder.select().all().from("schematest", "emp");
        Statement viewFetchQuerySal = QueryBuilder.select().all().from("schematest", "salary");
        List<Row> recordsEmp = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQueryEmp);
        List<Row> recordsSal = CassandraClientUtilities.commandExecution(ipInUse, viewFetchQuerySal);
        Map<Integer, List<Row>> result = new HashMap<>();
        logger.info("Total initial records fetched in Batch Processing = " + recordsEmp.size());
        int totalRecords = 0;
        Map<Integer, List<Row>> empMap = new HashMap<>();
        Map<Integer, List<Row>> salMap = new HashMap<>();
        for ( Row record : recordsEmp ) {
            if ( record.getInt("age") > 30 ) {

                if ( empMap.containsKey(record.getInt("joinkey")) ) {
                    List<Row> tempList = empMap.get(record.getInt("joinkey"));
                    tempList.add(record);
                    empMap.put(record.getInt("joinkey"), tempList);

                } else {
                    List<Row> tempList = new ArrayList<>();
                    tempList.add(record);
                    empMap.put(record.getInt("joinkey"), tempList);
                }
            }
        }

        for ( Row record : recordsSal ) {
            if ( record.getInt("salaryval") > 3000 ) {

                if ( salMap.containsKey(record.getInt("joinkey")) ) {
                    List<Row> tempList = salMap.get(record.getInt("joinkey"));
                    tempList.add(record);
                    salMap.put(record.getInt("joinkey"), tempList);

                } else {
                    List<Row> tempList = new ArrayList<>();
                    tempList.add(record);
                    salMap.put(record.getInt("joinkey"), tempList);
                }
            }
        }

        for ( Map.Entry<Integer, List<Row>> empEntry : empMap.entrySet() ) {
            if ( salMap.containsKey(empEntry.getKey()) ) {
                List<Row> temp = new ArrayList<>();
                temp.addAll(empEntry.getValue());
                temp.addAll(salMap.get(empEntry.getKey()));
                result.put(empEntry.getKey(), temp);
            }
        }

        Map<String, Integer> resultAggView = new HashMap<>();
        List<String> listShouldBeDeleted = new ArrayList<>();
        for ( Map.Entry<Integer, List<Row>> resultEntry : result.entrySet() ) {
            for ( Row record : resultEntry.getValue() ) {
                try {
                    if ( resultAggView.containsKey(record.getString("colaggkey_x")) ) {
                        resultAggView.put(record.getString("colaggkey_x"), resultAggView.get(record.getString("colaggkey_x")) +
                                record.getInt("age"));
                    } else {
                        resultAggView.put(record.getString("colaggkey_x"), record.getInt("age"));
                    }
                } catch ( Exception e ) {
                    e.printStackTrace();
                }

            }
        }

        for ( Map.Entry<String, Integer> resultEntry : resultAggView.entrySet() ) {
            if ( resultEntry.getValue() <= 100 ) {
                listShouldBeDeleted.add(resultEntry.getKey());
            }
        }

        for ( String delEntry : listShouldBeDeleted ) {
            resultAggView.remove(delEntry);
        }

        long stopBatchProcTimer = System.currentTimeMillis();
        logger.info("### Batch time stats: " + (stopBatchProcTimer - startBatchProcTimer));
        logger.info("### Batch processing results :: " + resultAggView);
        logger.info("### (BP)View1 stats for " + operationsGenerator.getNumOfKeys() + " keys per node and " +
                operationsGenerator.getNumOfOperations() + " operations #### ");
//        logger.info("Total records(after batch processing) = " + totalRecords);
    }

    public void executeView16() {

    }

    public void executeView17() {

    }

}
