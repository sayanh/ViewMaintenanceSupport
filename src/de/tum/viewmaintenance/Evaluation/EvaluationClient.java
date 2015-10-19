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
//
//        EvaluationClient evaluationClient = new EvaluationClient();
//        evaluationClient.executeEvaluation(operationsGenerator);

        TimeAnalysisPlots timeAnalysisPlots = new TimeAnalysisPlots();

        try {
            timeAnalysisPlots.drawMemoryAnalysisHistogram();

        } catch ( IOException e ) {
            e.printStackTrace();
        }

        System.exit(0);
    }

    public void executeEvaluation(OperationsGenerator operationsGenerator) {

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
            case "vt4":
                batchProcessing = new BatchProcessing(operationsGenerator, viewsObj.getTables().get(0));
                batchProcessing.executeView4();
                viewFetchClient = new ViewFetchClient(operationsGenerator, viewsObj.getTables().get(0));
                viewFetchClient.executeView4();
                break;
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
            case "vt8":
                batchProcessing = new BatchProcessing(operationsGenerator, viewsObj.getTables().get(0));
                batchProcessing.executeView8();
                viewFetchClient = new ViewFetchClient(operationsGenerator, viewsObj.getTables().get(0));
                viewFetchClient.executeView8();
                break;
            case "vt9":
                batchProcessing = new BatchProcessing(operationsGenerator, viewsObj.getTables().get(0));
                batchProcessing.executeView9();
                viewFetchClient = new ViewFetchClient(operationsGenerator, viewsObj.getTables().get(0));
                viewFetchClient.executeView9();
                break;
            case "vt10":
                batchProcessing = new BatchProcessing(operationsGenerator, viewsObj.getTables().get(0));
                batchProcessing.executeView10();
                viewFetchClient = new ViewFetchClient(operationsGenerator, viewsObj.getTables().get(0));
                viewFetchClient.executeView10();
                break;
            case "vt11":
                batchProcessing = new BatchProcessing(operationsGenerator, viewsObj.getTables().get(0));
                batchProcessing.executeView11();
                viewFetchClient = new ViewFetchClient(operationsGenerator, viewsObj.getTables().get(0));
                viewFetchClient.executeView11();
                break;

            case "vt12":
                batchProcessing = new BatchProcessing(operationsGenerator, viewsObj.getTables().get(0));
                batchProcessing.executeView12();
                viewFetchClient = new ViewFetchClient(operationsGenerator, viewsObj.getTables().get(0));
                viewFetchClient.executeView12();
                break;
            case "vt13":
                batchProcessing = new BatchProcessing(operationsGenerator, viewsObj.getTables().get(0));
                batchProcessing.executeView13();
                viewFetchClient = new ViewFetchClient(operationsGenerator, viewsObj.getTables().get(0));
                viewFetchClient.executeView13();
                break;
            case "vt14":
                batchProcessing = new BatchProcessing(operationsGenerator, viewsObj.getTables().get(0));
                batchProcessing.executeView14();
                viewFetchClient = new ViewFetchClient(operationsGenerator, viewsObj.getTables().get(0));
                viewFetchClient.executeView14();
                break;
            case "vt15":
                batchProcessing = new BatchProcessing(operationsGenerator, viewsObj.getTables().get(0));
                batchProcessing.executeView15();
                viewFetchClient = new ViewFetchClient(operationsGenerator, viewsObj.getTables().get(0));
                viewFetchClient.executeView15();
                break;
            case "vt16":
                batchProcessing = new BatchProcessing(operationsGenerator, viewsObj.getTables().get(0));
                batchProcessing.executeView16();
                viewFetchClient = new ViewFetchClient(operationsGenerator, viewsObj.getTables().get(0));
                viewFetchClient.executeView16();
                break;
            case "vt17":
                batchProcessing = new BatchProcessing(operationsGenerator, viewsObj.getTables().get(0));
                batchProcessing.executeView17();
                viewFetchClient = new ViewFetchClient(operationsGenerator, viewsObj.getTables().get(0));
                viewFetchClient.executeView17();
                break;
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

    }
}
