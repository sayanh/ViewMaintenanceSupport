package de.tum.viewmaintenance.Evaluation;

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
 * Created by shazra on 10/5/15.
 */
public class MemoryAnalysis {
    public static final String MEMORY_DATA_FILE = "/home/anarchy/work/sources/cassandra/memoryLogs.out";
    public static String OUTPUT_FILENAME = "/home/anarchy/memory_.jpg";

    public static void main(String[] args) throws IOException {

        BufferedReader bufferedReader = new BufferedReader(new FileReader(MEMORY_DATA_FILE));
        String tempLine = "";
        List<String> lines = new ArrayList<>();
        while ( (tempLine = bufferedReader.readLine()) != null ) {
            lines.add(tempLine);
        }

        MemoryAnalysis memoryAnalysis = new MemoryAnalysis();
        memoryAnalysis.drawMemoryAnalysisHistogram(lines, "random_plot_heading");

    }

    public void drawMemoryAnalysisHistogram(List<String> linesList, String plotHeading) throws IOException {

        // row keys...
        final String series1 = "Memory Used Before";
        final String series2 = "Memory Used After";

        final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        int timeLapse = 1;
        float memoryUsageBefore = 0.0f;
        for ( String line : linesList ) {

            if ( line.contains("Memory usage before view maintenance") ) {
                String[] lineArr = line.split("\\|");
                memoryUsageBefore = Float.parseFloat(lineArr[lineArr.length - 1].trim());

            } else if ( line.contains("Memory usage after view maintenance") ) {
                String[] lineArr = line.split("\\|");
                float timeMemory = Float.parseFloat(lineArr[lineArr.length - 1].trim());
                dataset.addValue(memoryUsageBefore, series1, timeLapse + " run");
                dataset.addValue(timeMemory, series2, timeLapse + " run");
                timeLapse += 1;
            }


        }

        final JFreeChart chart = ChartFactory.createBarChart(
                "Different stages",               // domain axis label
                "Memory usage while View Maintenance for " + plotHeading + " view",       // chart title
                "Memory Usage in %age",          // range axis label
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
//        renderer.setSeriesPaint(2, gp2);

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

        ChartUtilities.saveChartAsJPEG(new File(OUTPUT_FILENAME), chart, 800, 600);
    }
}
