package de.tum.viewmaintenance.Evaluation;

import de.tum.viewmaintenance.OperationsManagement.OperationsGenerator;
import de.tum.viewmaintenance.action.ViewMaintenanceConfig;
import de.tum.viewmaintenance.action.ViewMaintenanceLogsReader;
import de.tum.viewmaintenance.client.Load;
import de.tum.viewmaintenance.client.LoadGenerationProcess;
import de.tum.viewmaintenance.view_table_structure.Views;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Created by shazra on 10/3/15.
 */
public class EvaluationClient {
    private static final Logger logger = Logger.getLogger("EVALUATION");

    public static void main(String[] args) {
        LoadGenerationProcess loadGenerationProcess = new LoadGenerationProcess();
        Load load = loadGenerationProcess.configFileReader();
        OperationsGenerator operationsGenerator = OperationsGenerator.getInstance();
        ViewMaintenanceConfig.readViewConfigFromFile();
        Views viewsObj = Views.getInstance();

        logger.debug("view tables are :: " + viewsObj.getTables());

        logger.info("### View1 stats for | " + operationsGenerator.getNumOfKeys() + " | keys per node ");
        logger.info("### Total number of operations |" + operationsGenerator.getNumOfOperations() +
                "|  operations #### ");
        BatchProcessing batchProcessing;
        ViewFetchClient viewFetchClient;

        switch ( viewsObj.getTables().get(0).getName() ) {
            case "vt1":

                batchProcessing = new BatchProcessing(operationsGenerator, viewsObj.getTables().get(0));
                batchProcessing.executeView1();
                viewFetchClient = new ViewFetchClient(operationsGenerator, viewsObj.getTables().get(0));
                viewFetchClient.executeView1();
                break;
            case "vt2":

                batchProcessing = new BatchProcessing(operationsGenerator, viewsObj.getTables().get(0));
                batchProcessing.executeView2();
                viewFetchClient = new ViewFetchClient(operationsGenerator, viewsObj.getTables().get(0));
                viewFetchClient.executeView2();

                break;
            case "vt3":
                batchProcessing = new BatchProcessing(operationsGenerator, viewsObj.getTables().get(0));
                batchProcessing.executeView3();
                viewFetchClient = new ViewFetchClient(operationsGenerator, viewsObj.getTables().get(0));
                viewFetchClient.executeView3();
                break;
//            case "vt4":
//                startViewTimer = System.currentTimeMillis();
//                ViewFetchClient.executeView4();
//                stopViewTimer = System.currentTimeMillis();
//
//                startBatchProcTimer = System.currentTimeMillis();
//                BatchProcessing.executeView4();
//                stopBatchProcTimer = System.currentTimeMillis();
//                break;
            case "vt5":
                batchProcessing = new BatchProcessing(operationsGenerator, viewsObj.getTables().get(0));
                batchProcessing.executeView5();
                viewFetchClient = new ViewFetchClient(operationsGenerator, viewsObj.getTables().get(0));
                viewFetchClient.executeView5();
                break;
//            case "vt6":
//                startViewTimer = System.currentTimeMillis();
//                ViewFetchClient.executeView6();
//                stopViewTimer = System.currentTimeMillis();
//
//                startBatchProcTimer = System.currentTimeMillis();
//                BatchProcessing.executeView6();
//                stopBatchProcTimer = System.currentTimeMillis();
//                break;
//            case "vt7":
//                startViewTimer = System.currentTimeMillis();
//                ViewFetchClient.executeView7();
//                stopViewTimer = System.currentTimeMillis();
//
//                startBatchProcTimer = System.currentTimeMillis();
//                BatchProcessing.executeView7();
//                stopBatchProcTimer = System.currentTimeMillis();
//                break;
//            case "vt8":
//                startViewTimer = System.currentTimeMillis();
//                ViewFetchClient.executeView8();
//                stopViewTimer = System.currentTimeMillis();
//
//                startBatchProcTimer = System.currentTimeMillis();
//                BatchProcessing.executeView8();
//                stopBatchProcTimer = System.currentTimeMillis();
//                break;
//            case "vt9":
//                startViewTimer = System.currentTimeMillis();
//                ViewFetchClient.executeView9();
//                stopViewTimer = System.currentTimeMillis();
//
//                startBatchProcTimer = System.currentTimeMillis();
//                BatchProcessing.executeView9();
//                stopBatchProcTimer = System.currentTimeMillis();
//                break;
//            case "vt10":
//                startViewTimer = System.currentTimeMillis();
//                ViewFetchClient.executeView10();
//                stopViewTimer = System.currentTimeMillis();
//
//                startBatchProcTimer = System.currentTimeMillis();
//                BatchProcessing.executeView10();
//                stopBatchProcTimer = System.currentTimeMillis();
//                break;
//            case "vt11":
//                startViewTimer = System.currentTimeMillis();
//                ViewFetchClient.executeView11();
//                stopViewTimer = System.currentTimeMillis();
//
//                startBatchProcTimer = System.currentTimeMillis();
//                BatchProcessing.executeView11();
//                stopBatchProcTimer = System.currentTimeMillis();
//                break;
//
//            case "vt12":
//                startViewTimer = System.currentTimeMillis();
//                ViewFetchClient.executeView12();
//                stopViewTimer = System.currentTimeMillis();
//
//                startBatchProcTimer = System.currentTimeMillis();
//                BatchProcessing.executeView12();
//                stopBatchProcTimer = System.currentTimeMillis();
//                break;
//            case "vt13":
//                startViewTimer = System.currentTimeMillis();
//                ViewFetchClient.executeView13();
//                stopViewTimer = System.currentTimeMillis();
//
//                startBatchProcTimer = System.currentTimeMillis();
//                BatchProcessing.executeView13();
//                stopBatchProcTimer = System.currentTimeMillis();
//                break;
//            case "vt14":
//                startViewTimer = System.currentTimeMillis();
//                ViewFetchClient.executeView14();
//                stopViewTimer = System.currentTimeMillis();
//
//                startBatchProcTimer = System.currentTimeMillis();
//                BatchProcessing.executeView14();
//                stopBatchProcTimer = System.currentTimeMillis();
//
//                break;
//
//            case "vt15":
//                startViewTimer = System.currentTimeMillis();
//                ViewFetchClient.executeView15();
//                stopViewTimer = System.currentTimeMillis();
//
//                startBatchProcTimer = System.currentTimeMillis();
//                BatchProcessing.executeView15();
//                stopBatchProcTimer = System.currentTimeMillis();
//                break;

        }

        logger.info("### Time stats for view " + viewsObj.getTables().get(0).getName());

//        logger.info("### Batch time stats: " + (stopBatchProcTimer - startBatchProcTimer));

//        logger.info("### View time stats: " + (stopViewTimer - startViewTimer));

        // Generating time graphs

        TimeAnalysisPlots timeAnalysisPlots = new TimeAnalysisPlots();

        try {
            timeAnalysisPlots.drawMemoryAnalysisHistogram();

        } catch ( IOException e ) {
            e.printStackTrace();
        }

        logger.info("######## Evaluation phase is over ########");

        System.exit(0);
    }
}
