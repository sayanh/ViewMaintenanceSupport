package de.tum.viewmaintenance.Evaluation;

import de.tum.viewmaintenance.OperationsManagement.OperationsUtils;
import de.tum.viewmaintenance.client.LoadGenerationProcess;
import de.tum.viewmaintenance.view_table_structure.Views;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.CategoryAxis;
import org.jfree.chart.axis.CategoryLabelPositions;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.labels.ItemLabelAnchor;
import org.jfree.chart.labels.ItemLabelPosition;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BarRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.ui.TextAnchor;

import java.awt.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by shazra on 10/6/15.
 */
public class TimeAnalysisPlots {
    public static final String TIME_DATA_FILE = "/home/anarchy/work/sources/ViewMaintenanceCassandra/evaluationLog.out";
    public static String OUTPUT_FILENAME = "/home/anarchy/timePlots.jpg";
    public static void main(String[] args) {
        TimeAnalysisPlots timeAnalysisPlots = new TimeAnalysisPlots();
        try {
            timeAnalysisPlots.drawMemoryAnalysisHistogram();
        } catch ( IOException e ) {
            e.printStackTrace();
        }
    }

    public void drawMemoryAnalysisHistogram() throws IOException {

        // Reading the lines from the file

        BufferedReader bufferedReader = new BufferedReader(new FileReader(TIME_DATA_FILE));
        String tempLine = "";
        List<String> linesList = new ArrayList<>();
        while ( (tempLine = bufferedReader.readLine()) != null ) {
            linesList.add(tempLine);
        }

        // row keys...
        final String series1 = "Batch Processing";
        final String series2 = "Views Mechanism";
        String numOfOperations = "";

        final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        for ( String line : linesList ) {

            if (line.contains("Total number of operations")) {
                numOfOperations = line.split("\\|")[1].trim();
            } else if ( line.contains("Batch time stats") ) {
                String[] lineArr = line.split(":");
                int timeMemory = Integer.parseInt(lineArr[lineArr.length - 1].trim());
                dataset.addValue(timeMemory, series1, numOfOperations);

            } else if ( line.contains("View time stats") ) {
                String[] lineArr = line.split(":");
                int timeMemory = Integer.parseInt(lineArr[lineArr.length - 1].trim());
                dataset.addValue(timeMemory, series2, numOfOperations);
            }


        }

        final JFreeChart chart = ChartFactory.createBarChart(
                "Response time for " + getPlotName() + " view vs batch processing",       // chart title
                "No. of operations",               // domain axis label
                "Time in ms",          // range axis label
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

        ChartUtilities.saveChartAsJPEG(new File(OUTPUT_FILENAME), chart, 500, 300);
    }

    private String getPlotName() {
//        LoadGenerationProcess loadGenerationProcess = new LoadGenerationProcess();
//        loadGenerationProcess.readViewConfig();
        Views viewsObj = Views.getInstance();

        return OperationsUtils.getOperationNameForPlots(viewsObj.getTables().get(0).getName());

    }
}
