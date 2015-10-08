package de.tum.viewmaintenance.Evaluation;

import com.jcraft.jsch.Session;
import de.tum.viewmaintenance.client.SshUtilities;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.*;
import org.jfree.chart.labels.ItemLabelAnchor;
import org.jfree.chart.labels.ItemLabelPosition;
import org.jfree.chart.labels.StandardCategoryToolTipGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.chart.renderer.category.LineAndShapeRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.TextAnchor;

import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by shazra on 10/6/15.
 */
public class ViewManagerOperationsVsTimeConsumedPlots {
    public final static int PORT = 22;
    public static final String OUTPUT_FILENAME = "/home/anarchy/timeVsOperations_.jpg";

    public static void main(String[] args) {
        ViewManagerOperationsVsTimeConsumedPlots plot = new ViewManagerOperationsVsTimeConsumedPlots();
        plot.drawDualAxisHistogramOperationsVsTime("anarchy", "password", "vm1");
    }

    private CategoryDataset createNumOfOperationsDataset(List<String> logsList) {


        // row keys...
        final String series1 = "Number of Operations";


        // create the dataset...
        final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        int operationsExecuted = 0;
        int numOfOperations = 1;
        for ( String logStr : logsList ) {
            if ( logStr.contains("Number of operations executed") ) {
                System.out.println(logStr);
                operationsExecuted = Integer.parseInt(logStr.split("\\|")[2].trim());

                dataset.addValue(operationsExecuted, series1, numOfOperations + " stage");
                numOfOperations++;
            }
        }

        return dataset;
    }

    private CategoryDataset createTimeDataset(List<String> logsList) {


        // row keys...
        final String series1 = "Time consumed";

        // column keys...

        int numOfOperations = 1;

        // create the dataset...
        final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        int timeTakenForViewMaintenance = 0;
        for (String logStr: logsList) {
            if (logStr.contains("Time taken to maintain views")) {
                System.out.println(logStr);
                timeTakenForViewMaintenance = Integer.parseInt(logStr.split("\\|")[2].trim())
                        / 1000;

                dataset.addValue(timeTakenForViewMaintenance, series1, numOfOperations + " stage");
                numOfOperations ++;
            }
        }
        return dataset;


    }

    public void drawDualAxisHistogramOperationsVsTime(String username, String password, String hostname) {

        List<String> logsToAnalyse = getTimeLogs(username, password, hostname, PORT);
        final CategoryDataset timeDataset = createTimeDataset(logsToAnalyse);

        // create the chart...
        final JFreeChart chart = ChartFactory.createBarChart(
                "Time consumed vs number of operations",        // chart title
                "Stages",               // domain axis label
                "Time consumed in secs",                  // range axis label
                timeDataset,                 // data
                PlotOrientation.VERTICAL,
                true,                     // include legend
                true,                     // tooltips?
                false                     // URL generator?  Not required...
        );

        chart.setBackgroundPaint(Color.white);
//        chart.getLegend().setAnchor(Legend.SOUTH);

        // get a reference to the plot for further customisation...
        final CategoryPlot plot = chart.getCategoryPlot();
        plot.setBackgroundPaint(new Color(0xEE, 0xEE, 0xFF));
        plot.setDomainAxisLocation(AxisLocation.BOTTOM_OR_RIGHT);

        final BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setDrawBarOutline(false);
        renderer.setItemMargin(0.10);

        // set up gradient paints for series...
        final GradientPaint gp0 = new GradientPaint(
                0.0f, 0.0f, Color.DARK_GRAY,
                0.0f, 0.0f, Color.DARK_GRAY
        );

        renderer.setSeriesFillPaint(0, gp0);
        plot.setRenderer(renderer);

        final CategoryDataset numOfOperationsDataset = createNumOfOperationsDataset
                (logsToAnalyse);
        plot.setDataset(1, numOfOperationsDataset);
        plot.mapDatasetToRangeAxis(1, 1);

        final CategoryAxis domainAxis = plot.getDomainAxis();
        domainAxis.setCategoryLabelPositions(CategoryLabelPositions.DOWN_45);
        final ValueAxis axis2 = new NumberAxis("Num of operations");
        plot.setRangeAxis(1, axis2);

        final LineAndShapeRenderer renderer2 = new LineAndShapeRenderer();
        renderer2.setToolTipGenerator(new StandardCategoryToolTipGenerator());
        renderer2.setSeriesLinesVisible(1, true);
        renderer2.setBaseShapesVisible(true);
        plot.setRenderer(1, renderer2);
        plot.setDatasetRenderingOrder(DatasetRenderingOrder.REVERSE);

        // OPTIONAL CUSTOMISATION COMPLETED.

        try {
            ChartUtilities.saveChartAsJPEG(new File(OUTPUT_FILENAME), chart, 800, 600);
        } catch ( IOException e ) {
            e.printStackTrace();
        }

    }


    public void drawMemoryAnalysisHistogram(String username, String password, String hostname) {
        List<String> logsToAnalyse = getTimeLogs(username, password, hostname, PORT);
        int timeTakenForViewMaintenance = 0;
        int operationsExecuted = 0;
        final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        final String series1 = "Time Taken";
        final String series2 = "";
        int numOfOperations = 0;
        for ( String logStr : logsToAnalyse ) {
            if ( logStr.contains("Time taken to maintain views") ) {
                System.out.println(logStr);
                timeTakenForViewMaintenance = Integer.parseInt(logStr.split("\\|")[2].trim())
                        / 1000;

                dataset.addValue(timeTakenForViewMaintenance, series1, numOfOperations + "");

            } else if ( logStr.contains("Number of operations executed") ) {
                System.out.println(logStr);
                operationsExecuted = Integer.parseInt(logStr.split("\\|")[2].trim());

                dataset.addValue(operationsExecuted, series1, numOfOperations + "");
                numOfOperations++;
            }
        }

        createHistogram(dataset);
    }


    public void drawMemoryAnalysisLineGraph(String username, String password, String hostname) {
        List<String> logsToAnalyse = getTimeLogs(username, password, hostname, PORT);
        final XYSeries series1 = new XYSeries("Time taken");
        int timeTakenForViewMaintenance = 0;
        int operationsExecuted = 0;

        for ( String logStr : logsToAnalyse ) {

            if ( logStr.contains("Time taken to maintain views") ) {
                System.out.println(logStr);
                timeTakenForViewMaintenance = Integer.parseInt(logStr.split("\\|")[2].trim())
                        / 1000;

            } else if ( logStr.contains("Number of operations executed") ) {
                System.out.println(logStr);
                operationsExecuted = Integer.parseInt(logStr.split("\\|")[2].trim());
                series1.add(operationsExecuted, timeTakenForViewMaintenance);
            }
        }

        final XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(series1);

        final JFreeChart chart = createLineChart(dataset);

        try {
            ChartUtilities.saveChartAsJPEG(new File(OUTPUT_FILENAME), chart, 1000, 800);
        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }


    public List<String> getTimeLogs(String username, String password, String host, int port) {
        Session session = SshUtilities.getSession(username, password, host, port);

        List<String> result = SshUtilities.runCommand(session, "grep Analysis ~/cassandraviewmaintenance/logs/system.log");

        SshUtilities.sessionDisconnect(session);

        return result;
    }

    private JFreeChart createLineChart(final XYDataset dataset) {

        // create the chart...
        final JFreeChart chart = ChartFactory.createXYLineChart(
                "Time taken Vs Number of Operations",      // chart title
                "Operations executed",                      // x axis label
                "Time taken by VM",                      // y axis label
                dataset,                  // data
                PlotOrientation.VERTICAL,
                true,                     // include legend
                true,                     // tooltips
                false                     // urls
        );

        // NOW DO SOME OPTIONAL CUSTOMISATION OF THE CHART...
        chart.setBackgroundPaint(Color.white);

//        final StandardLegend legend = (StandardLegend) chart.getLegend();
        //      legend.setDisplaySeriesShapes(true);

        // get a reference to the plot for further customisation...
        final XYPlot plot = chart.getXYPlot();
        plot.setBackgroundPaint(Color.lightGray);
        //    plot.setAxisOffset(new Spacer(Spacer.ABSOLUTE, 5.0, 5.0, 5.0, 5.0));
        plot.setDomainGridlinePaint(Color.white);
        plot.setRangeGridlinePaint(Color.white);

        final XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
        renderer.setSeriesLinesVisible(0, true);
        renderer.setSeriesShapesVisible(1, true);
        plot.setRenderer(renderer);

        // change the auto tick unit selection to integer units only...
        final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());
        // OPTIONAL CUSTOMISATION COMPLETED.

        return chart;

    }


    private JFreeChart createHistogram(final DefaultCategoryDataset dataset) {
        final JFreeChart chart = ChartFactory.createBarChart(
                "Time taken Vs Number of Operations",      // chart title
                "Operations executed",                      // x axis label
                "Time taken by VM",                      // y axis label
                dataset,                  // data
                PlotOrientation.VERTICAL, // orientation
                true,                    // include legend
                true,                     // tooltips?
                false                     // URLs?
        );


        chart.setBackgroundPaint(Color.white);

        final CategoryPlot plot = chart.getCategoryPlot();
        plot.setBackgroundPaint(Color.lightGray);
        plot.setDomainGridlinePaint(Color.white);
        plot.setRangeGridlinePaint(Color.white);


        final NumberAxis rangeAxis = (NumberAxis) plot.getRangeAxis();
        rangeAxis.setStandardTickUnits(NumberAxis.createIntegerTickUnits());

        final BarRenderer renderer = (BarRenderer) plot.getRenderer();
        renderer.setDrawBarOutline(false);
        renderer.setItemMargin(0.10);

        // set up gradient paints for series...
        final GradientPaint gp0 = new GradientPaint(
                0.0f, 0.0f, Color.blue,
                0.0f, 0.0f, Color.blue
        );
        final GradientPaint gp1 = new GradientPaint(
                0.0f, 0.0f, Color.green,
                0.0f, 0.0f, Color.green
        );
//        final GradientPaint gp2 = new GradientPaint(
//                0.0f, 0.0f, Color.red,
//                0.0f, 0.0f, Color.lightGray
//        );
        renderer.setSeriesPaint(0, gp0);
        renderer.setSeriesPaint(1, gp1);

        final ItemLabelPosition p = new ItemLabelPosition(
                ItemLabelAnchor.INSIDE12, TextAnchor.CENTER_RIGHT,
                TextAnchor.CENTER_RIGHT, -Math.PI / 2.0
        );
        renderer.setPositiveItemLabelPosition(p);

        final ItemLabelPosition p2 = new ItemLabelPosition(
                ItemLabelAnchor.OUTSIDE12, TextAnchor.CENTER_LEFT,
                TextAnchor.CENTER_LEFT, -Math.PI / 2.0
        );

        renderer.setPositiveItemLabelPositionFallback(p2);
        final CategoryAxis domainAxis = plot.getDomainAxis();
        domainAxis.setCategoryLabelPositions(CategoryLabelPositions.UP_45);

        try {
            ChartUtilities.saveChartAsJPEG(new File(OUTPUT_FILENAME), chart, 500, 300);
        } catch ( IOException e ) {
            e.printStackTrace();
        }

        return chart;
    }
}
