import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

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
	  
	  SparkAppHandle handle = new SparkLauncher()
	  .setSparkHome(sparkHome)
	  .setAppResource(appResource)
      .setMainClass(mainClass)
      .setMaster(master)
      .setConf(SparkLauncher.DRIVER_MEMORY, "1g")
      .setAppName(appName)
      .setVerbose(true)
      .addAppArgs(logFilePath)
//      .redirectToLog("testlog")
      .startApplication(new Listener() {

		@Override
		public void infoChanged(SparkAppHandle arg0) {
			// TODO Auto-generated method stub
			System.out.println(arg0.toString());
		}

		@Override
		public void stateChanged(SparkAppHandle arg0) {
			// TODO Auto-generated method stub
			System.out.println(arg0.getState().name());
		}
    	  
      });
	  System.out.println(handle.getState().name());
	  while(handle.getState() == SparkAppHandle.State.UNKNOWN 
			  ||  handle.getState() == SparkAppHandle.State.RUNNING) {
		  System.out.println(handle.getState().name());
		  System.out.println(handle.getAppId());
		  Thread.sleep(5000); 
	  }
	  // Use handle API to monitor / control application.
	  System.out.println(handle.getState().name());
	  System.out.println(handle.getAppId());
 }
}