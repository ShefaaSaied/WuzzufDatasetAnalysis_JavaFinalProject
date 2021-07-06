import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.ml.feature.StringIndexer;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler;
import org.knowm.xchart.*;
import scala.Tuple2;
import tech.tablesaw.plotly.Plot;
import tech.tablesaw.plotly.components.Figure;
import tech.tablesaw.plotly.traces.PieTrace;
import tech.tablesaw.plotly.components.Layout;
import tech.tablesaw.api.Table;
public class WuzzufFinal {
    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[1]");

    // 1. Read data set and convert it to dataframe or Spark RDD and display some from it.
        // Create Spark Session to create connection to Spark
        SparkSession sparkSession = SparkSession.builder().appName("Wuzzuf dataset analysis").master("local[6]").getOrCreate();
        // Get DataFrameReader using SparkSession
        DataFrameReader dataFrameReader = sparkSession.read();
        Dataset<Row> jobs = dataFrameReader.option("header", "true").csv("in/Wuzzuf_Jobs.csv");

        jobs = jobs.select("Title", "Company", "Type", "level", "YearsExp", "Country", "Skills");
        //Display summary of jobs
        System.out.println("Dataset:");
        jobs.show();

    // 2. Display structure and summary of the data.
        System.out.println("Dataset Schema:");
        jobs.printSchema();
        jobs.summary();

        System.out.println("Data size before cleaning:");
        System.out.println(jobs.count());

    // 3. Clean the data (null, duplications)
        Dataset<Row> jobsNoNullDF = jobs.na().drop();
        Dataset<Row> jobs_Clean = jobsNoNullDF.dropDuplicates();

        System.out.println("Data size after cleaning:");
        System.out.println(jobs_Clean.count());

    // 4. Count the jobs for each company and display that in order
        System.out.println("Most Demanding Companies for Jobs:");
        Dataset<Row> company_count = jobs_Clean.groupBy("Company").count().orderBy(desc("Count"));
        company_count.show(20, false);

    // 5. Show step 4 in a pie chart  (Using TableSaw)
        Table table = Table.read().csv("in/Wuzzuf_Jobs.csv");
        Table comp_table = table.countBy(table.categoricalColumn("Company"));
        PieTrace trace = PieTrace.builder(comp_table.categoricalColumn("Category"), comp_table.numberColumn("Count")).build();
        Layout layout = Layout.builder()
                .title("Most Demanding Companies for Jobs")
                .height(800)
                .width(1600).build();
        System.out.println("Displaying PieChart in Browser ...........");
        Plot.show(new Figure(layout, trace));

    // 6. Find out What are it the most popular job titles?
        System.out.println("Most popular job titles:");
        Dataset<Row> jobs_count = jobs_Clean.groupBy("Title").count().orderBy(desc("Count"));
        jobs_count.show(20, false);

    // 7. Show step 6 in bar chart (Using Xchart)
        System.out.println("Displaying popular titles BarChart ...........");
        List<String> titles = jobs_count.select("Title").as(Encoders.STRING()).collectAsList();
        List<String> titlesCount = jobs_count.select("Count").as(Encoders.STRING()).collectAsList();
        List<Integer> titlesCount_int = titlesCount.stream().map(s -> Integer.parseInt(s)).collect(Collectors.toList());

        CategoryChart chart =
                new CategoryChartBuilder()
                        .width(1400).height(800).title("Top 20 popular job titles").xAxisTitle("Job title").yAxisTitle("Popularity").build();

        chart.addSeries("Title Counts", titles.subList(0, 20), titlesCount_int.subList(0, 20));

        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setPlotGridLinesVisible(true);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setLegendVisible(false);
        chart.getStyler().setStacked(true);
        chart.getStyler().setXAxisLabelRotation(45);

        new SwingWrapper<>(chart).displayChart();

    // 8. Find out the most popular areas?
        System.out.println("Most popular areas:");
        Dataset<Row> areas_count = jobs_Clean.groupBy("Country").count().orderBy(desc("Count"));
        areas_count.show(20, false);

    // 9. Show step 8 in bar chart (Using Xchart)
        System.out.println("Displaying popular areas BarChart ...........");
        List<String> areas = areas_count.select("Country").as(Encoders.STRING()).collectAsList();
        List<String> areasCount = areas_count.select("Count").as(Encoders.STRING()).collectAsList();
        List<Integer> areasCount_int = areasCount.stream().map(s -> Integer.parseInt(s)).collect(Collectors.toList());

        CategoryChart chart2 =
                new CategoryChartBuilder()
                        .width(1400).height(800).title("Top 10 popular areas").xAxisTitle("Area").yAxisTitle("Popularity").build();

        chart2.addSeries("Areas Counts", areas.subList(0, 10), areasCount_int.subList(0, 10));

        chart2.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart2.getStyler().setPlotGridLinesVisible(true);
        chart2.getStyler().setHasAnnotations(true);
        chart2.getStyler().setLegendVisible(false);
        chart2.getStyler().setStacked(true);
        chart2.getStyler().setXAxisLabelRotation(45);

        new SwingWrapper<>(chart2).displayChart();

    // 10. Print skills one by one and how many each repeated and order the output to find out the most important skills required?
        Dataset<String> skills_col = jobs_Clean.select("Skills").as(Encoders.STRING());

        JavaRDD<String> skillsRdd = skills_col.toJavaRDD();
        JavaRDD<String> eachSkill = skillsRdd.flatMap(line -> Arrays.asList(line.split(",")).iterator());

        JavaPairRDD<String, Integer> skillPairRdd = eachSkill.mapToPair(skill -> new Tuple2<>(skill, 1));
        JavaPairRDD<String, Integer> countSkills = skillPairRdd.reduceByKey((x, y) -> x + y);
        JavaPairRDD<Integer, String> skill_count_pairs = countSkills.mapToPair(item -> new Tuple2<>(item._2(), item._1())).sortByKey(false);

        System.out.println("Most important skills required:");
        for (Tuple2<Integer, String> skillCount : skill_count_pairs.collect()) {
            System.out.println(skillCount._2() + " --> " + skillCount._1());
        }

    // 11. Factorize the YearsExp feature and convert it to numbers in new col (Bonus)
        System.out.println("YearsExp column after factorization:");
        StringIndexer indexer = new StringIndexer().setInputCol("YearsExp").setOutputCol("Factorized YearsExp");
        indexer.fit(jobs_Clean).transform(jobs_Clean).show(20);

    // 12. Apply K-means for job title and companies (Bonus)
        /*Dataset<Row> wuzzuf = sparkSession.read().format("libsvm").load("in/Wuzzuf_Jobs.csv");
        StringIndexer indexer2 = new StringIndexer().setInputCol("Title").setOutputCol("features");
        Dataset<Row> indexed_title = indexer2.fit(jobs_Clean).transform(jobs_Clean);

        VectorAssembler vectorAssembler = new VectorAssembler();
        String inputColumns[] = {"features"};
        vectorAssembler.setInputCols(inputColumns);
        vectorAssembler.setOutputCol("clusters");

        Dataset<Row> dataset = vectorAssembler.transform(indexed_title);

        KMeans kmeans = new KMeans().setK(2).setSeed(1L);
        //// KMeans for  job titles
        KMeansModel model = kmeans.fit(dataset);
        // Make predictions
        Dataset<Row> predictions = model.transform(dataset);

        // Evaluate clustering by computing Silhouette score
        ClusteringEvaluator evaluator = new ClusteringEvaluator();
        double silhouette = evaluator.evaluate(predictions);
        System.out.println("Silhouette with squared euclidean distance = " + silhouette);

        // Shows the result.
        Vector[] centeroids = (Vector[]) model.clusterCenters();
        System.out.println("Cluster Centeroids: ");
        for (Vector centeroid : centeroids) {
            System.out.println(centeroid);
        }*/
    }
}