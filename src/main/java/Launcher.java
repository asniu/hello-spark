import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.Listener;
import org.apache.spark.launcher.SparkLauncher;

public class Launcher {
  public static void main(String[] args) throws Exception {
	  if(args == null || args.length == 0) {
		  throw new IllegalArgumentException("Missing proeprty file argument!");
	  }
	  Properties properties = new Properties();
	  try(
		  InputStream inStream = new FileInputStream(args[0]);
		) {
		  properties.load(inStream);
	  }
	  
	  String sparkHome = properties.getProperty("sparkHome");
	  String master = properties.getProperty("master");
	  String appResource = properties.getProperty("appResource");
	  String mainClass = properties.getProperty("mainClass");
	  String appName = properties.getProperty("appName");
	  String logFilePath = properties.getProperty("logFilePath");
	  
	  CountDownLatch latch = new CountDownLatch(1);
	  SparkAppHandle handle = new SparkLauncher()
	  .setSparkHome(sparkHome)
	  .setAppResource(appResource)
      .setMainClass(mainClass)
      .setMaster(master)
      .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
      .setAppName(appName)
      .setVerbose(true)
      .addAppArgs(logFilePath)
      .startApplication(new Listener() {

		@Override
		public void infoChanged(SparkAppHandle arg0) {
			System.out.println(arg0.toString());
		}

		@Override
		public void stateChanged(SparkAppHandle arg0) {
			System.out.println(arg0.getState().name());
			
			if(arg0.getState().isFinal()) {
				latch.countDown();
			}
		}
    	  
      });
	  latch.await();
	  System.out.println("Application: " + handle.getAppId()
			  + " finished with status: " + handle.getState().name());
 }
}