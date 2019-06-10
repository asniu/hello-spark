package xn.sparktest.simpleapp;

import java.util.function.Consumer;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.launcher.SparkLauncher;

import xn.sparktest.simpleapp.function.StringToTwoInteger;
import xn.sparktest.simpleapp.function.SimpleVoidFunction;
import xn.sparktest.simpleapp.model.TwoIntegers;

public class SimpleApp {
  public static void main(String[] args) {
	  if(args == null || args.length == 0) {
		  throw new IllegalArgumentException("Missing log file argument!");
	  }
    String logFile = args[0]; // Should be some file on your system
    SparkConf conf = new SparkConf();
    JavaSparkContext sc = new JavaSparkContext(conf);
//    System.out.println("memory" + sc.getConf().get("SparkLauncher.DRIVER_MEMORY"));
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter( s -> s.contains("a") ).count();

    long numBs = logData.filter( s -> s.contains("b") ).count();
    
    StringToTwoInteger strToTwoInteger = new StringToTwoInteger();
    SimpleVoidFunction function2 = new SimpleVoidFunction();
    logData.map(strToTwoInteger).foreach(function2);
    
    Consumer<? super String> action = new Consumer<Object>() {

		@Override
		public void accept(Object t) {
			// TODO Auto-generated method stub
			System.out.println("t: " + t.toString());
		}
    
    };
	logData.collect().forEach(action);

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    
//    sc.stop();
  }
}