import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

public class Launcher {
  public static void main(String[] args) throws Exception {
   SparkAppHandle handle = new SparkLauncher()
     .setAppResource("/my/simple-project-1.0.jar")
     .setMainClass("main.java.SimpleApp")
     .setMaster("local")
     .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
     .startApplication();
   // Use handle API to monitor / control application.
 }
}