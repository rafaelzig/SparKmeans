package cn.edu.tsinghua.rafaelzig;

import cn.edu.tsinghua.rafaelzig.datastructure.Point;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class KMeans
{
	private static final String NL = System.getProperty("line.separator");
	private static final String FS              = System.getProperty("file.separator");
	private static final String HADOOP_HOME_DIR = "D:" + FS + "Documents" + FS + "Workspace" + FS + "SparKmeans";
	private static final String OUTPUT_PATH = "output" + FS;
	private static final SimpleDateFormat TIMESTAMP = new SimpleDateFormat("yyyyMMddHHmmssSSS");
	private static final String INPUT           = "input" + FS + "kddcup.testdata.unlabeled.txt";
//	private static final String INPUT           = "input" + FS + "kddcup.testdata.unlabeled_10_percent.txt";
//	private static final int    K               = 5;
	private static final int    K               = 2;

	public static void main(String[] args)
	{
		try (JavaSparkContext sc = new JavaSparkContext(getSparkConfig()))
		{
			long start = System.nanoTime();

			JavaRDD<Point> dataSetRDD = sc.textFile(INPUT).distinct().map(Point::new);
			List<Point> centroids = dataSetRDD.takeSample(false, K, System.currentTimeMillis());

			for (int i = 0; i < K; ++i)
			{
				centroids.get(i).setLabel(i);
			}

			int iterations = 0;
			List<Point> oldCentroids;
			do
			{
				// Save state and clear points assigned to cluster in last iteration
				oldCentroids = centroids.stream().map(Point::new).collect(Collectors.toList());

				// Assign each point to the closest centroid and move centroid to new position
				dataSetRDD = dataSetRDD.map(point -> new Point(point, centroids)).cache();
				moveCentroids(dataSetRDD, centroids);

				System.out.println(">>> End of iteration " + ++iterations + " <<<" + NL);
			}
			while (!centroids.equals(oldCentroids));

			long elapsed = (System.nanoTime() - start) / 1_000_000;
			System.out.println("Performed in " + elapsed + "ms (Average " + elapsed / iterations + "ms per iteration)");

			dataSetRDD.saveAsTextFile(OUTPUT_PATH + TIMESTAMP.format(new Date()));
			System.out.println("Output successfully written.");
		}
	}

	// Move centroids based on points in their cluster
	private static void moveCentroids(JavaRDD<Point> dataSetRDD, Iterable<Point> centroids)
	{
		for (Point centroid : centroids)
		{
			JavaRDD<Point> clusterRDD = dataSetRDD.filter(p -> p.getLabel() == centroid.getLabel());
			long clusterSize = clusterRDD.count();
			centroid.move(clusterRDD.reduce(Point::new), clusterSize);

			System.out.println("Centroid " + centroid.getLabel() + " -> " + clusterSize + " points");
		}
	}

	private static SparkConf getSparkConfig()
	{
		System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR);
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);

//		return new SparkConf().setAppName(KMeans.class.getName());
		return new SparkConf().setAppName(KMeans.class.getName())
		                      .setMaster("local[4]");
	}
}