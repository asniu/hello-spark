package xn.sparktest.simpleapp;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
  public static void main(String[] args) {
	  if(args == null || args.length == 0) {
		  throw new IllegalArgumentException("Missing log file argument!");
	  }
    String logFile = args[0]; // Should be some file on your system
    SparkConf conf = new SparkConf();
    JavaSparkContext sc = new JavaSparkContext(conf);
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter( s -> s.contains("a") ).count();

    long numBs = logData.filter( s -> s.contains("b") ).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    
    sc.stop();
  }
}