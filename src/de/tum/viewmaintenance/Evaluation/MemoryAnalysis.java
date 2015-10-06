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
import org.jfree.data.time.Day;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.ui.TextAnchor;

import java.awt.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.List;

/**
 * Created by shazra on 10/5/15.
 */
public class MemoryAnalysis {
    public static final String MEMORY_DATA_FILE = "/home/anarchy/work/sources/cassandra/memoryLogs.out";
    public static String OUTPUT_FILENAME = "/home/anarchy/memoryHistogram.jpg";

    public static void main(String[] args) throws IOException {

        BufferedReader bufferedReader = new BufferedReader(new FileReader(MEMORY_DATA_FILE));
        String tempLine = "";
        List<String> lines = new ArrayList<>();
        while ( (tempLine = bufferedReader.readLine()) != null ) {
            lines.add(tempLine);
        }

        MemoryAnalysis memoryAnalysis = new MemoryAnalysis();
        memoryAnalysis.drawMemoryAnalysisHistogram(lines);

    }

//    public void drawMemoryBarChart(List<String> linesList) throws IOException {
//
//        DefaultCategoryDataset bardataset = new DefaultCategoryDataset();
//        int timeLapse = 1;
//        for ( String line : linesList ) {
//            if ( line.contains("Memory usage after view maintenance") ) {
//                String[] lineArr = line.split("\\|");
//                float timeMemory = Float.parseFloat(lineArr[lineArr.length - 1].trim());
//                bardataset.setValue(timeMemory, "memory", "" + timeLapse + " run VM");
//                timeLapse += 1;
//            }
//        }
//
//        JFreeChart barchart = ChartFactory.createBarChart(
//                "Memory usage while view maintenance",      //Title
//                "Time",             // X-axis Label
//                "Memory usage %",               // Y-axis Label
//                bardataset,             // Dataset
//                PlotOrientation.VERTICAL,      //Plot orientation
//                false,                // Show legend
//                true,                // Use tooltips
//                false                // Generate URLs
//        );
//        String filename = "/home/anarchy/memory.jpg";
//        ChartUtilities.saveChartAsJPEG(new File(filename), barchart, 500, 300);
//    }


    public void drawMemoryAnalysisHistogram(List<String> linesList) throws IOException {

        // row keys...
        final String series1 = "Memory Used Before";
        final String series2 = "Memory Used After";

        final DefaultCategoryDataset dataset = new DefaultCategoryDataset();
        int timeLapse = 1;
        for ( String line : linesList ) {

            if ( line.contains("Memory usage before view maintenance") ) {
                String[] lineArr = line.split("\\|");
                float timeMemory = Float.parseFloat(lineArr[lineArr.length - 1].trim());
                dataset.addValue(timeMemory, series1, timeLapse + " run");

            } else if ( line.contains("Memory usage after view maintenance") ) {
                String[] lineArr = line.split("\\|");
                float timeMemory = Float.parseFloat(lineArr[lineArr.length - 1].trim());
                dataset.addValue(timeMemory, series2, timeLapse + " run");
                timeLapse += 1;
            }


        }

        final JFreeChart chart = ChartFactory.createBarChart(
                "Memory usage while View Maintenance",       // chart title
                "Different stages",               // domain axis label
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

        ChartUtilities.saveChartAsJPEG(new File(OUTPUT_FILENAME), chart, 500, 300);
    }
}
