import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceSideJoinDriver {

	
	public static class UserFileMapper extends Mapper<LongWritable, Text, Text, Text> {
	        
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] userMobName = line.split(",");
	        String mobNum = userMobName[0];
	        String userName = userMobName[1];
	      //user text is used by reducer to identify data is coming from which mapper and which folder.
	        context.write(new Text(mobNum), new Text("user\t"+userName));
	    }
	 }	
	
	
	public static class DeliverDetailsMapper extends Mapper<LongWritable, Text, Text, Text> {
	        
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] deliveryStatusCodeMobNo = line.split(",");
	        String mobNum = deliveryStatusCodeMobNo[0];
	        String deliveryStatusCode = deliveryStatusCodeMobNo[1];
	        //deli text is used by reducer to identify data is coming from which mapper and which folder.
	        context.write(new Text(mobNum), new Text("deli\t"+deliveryStatusCode));
	    }
	 }
	
	
	 public static class SMSReducer extends Reducer<Text, Text, Text, Text> {

		 /*Map to store Delivery Codes and Messages
		 *Key being the status code and vale being the status message*/
		 private static HashMap<String,String> DeliveryCodesMap= new HashMap<String,String>();
		 
		 enum MYCOUNTER{
			 RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
		 }
		// Variables to aid the join process
		private String customerName, deliveryReport;
		private BufferedReader brReader;

		// To load the Delivery Codes and Messages into a hash map
		private void loadDeliveryStatusCodes(Path filePath, Context context)
				throws IOException {
			String strLineRead = "";
			try {
				brReader = new BufferedReader(new FileReader(
						filePath.toString()));

				// Read each line, split and load to HashMap
				while ((strLineRead = brReader.readLine()) != null) {
					String splitarray[] = strLineRead.split(",");
					DeliveryCodesMap.put(splitarray[0].trim(),splitarray[1].trim());
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
			} catch (IOException e) {
				context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
				e.printStackTrace();
			} finally {
				if (brReader != null) {
					brReader.close();
				}
			}
		}
		//Load the files from DistributedCache 
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Path[] cacheFilesLocal = DistributedCache
					.getLocalCacheFiles(context.getConfiguration());
			for (Path eachPath : cacheFilesLocal) {
				if (eachPath.getName().toString().trim()
						.equals("DeliveryStatusCodes")) {
					context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
					loadDeliveryStatusCodes(eachPath, context);
				}
			}
		}
		
		 
		 
		 public void reduce(Text key, Iterable<Text> values, Context context) 
	     throws IOException, InterruptedException {
			 String userName= null;
			 String deliveryStaus = null;
			 List<String> deliverStatusLst= new ArrayList<String>();
			 
            for(Text txt: values){
                String str = txt.toString();
                String[] splitVal = str.split("\t");
            	if(splitVal[0].trim().equals("user")){
            		userName=splitVal[1].trim(); 
            	} else if(splitVal[0].trim().equals("deli")){
            		deliveryStaus = DeliveryCodesMap.get(splitVal[1].trim());
            		deliverStatusLst.add(deliveryStaus);
            	}
            }
            
            if(null!=userName && userName.trim().length()>0 && deliverStatusLst!=null && deliverStatusLst.size()>0 ){
            	for(String str: deliverStatusLst){
            	    context.write(new Text(userName),new Text(str));
            	}
            }else if(userName==null){
            	for(String str: deliverStatusLst){
            	    context.write(new Text("userName"),new Text(str));
            	}
            }else if(!(deliverStatusLst!=null && deliverStatusLst.size()>0)){
               context.write(new Text(userName), new Text("deliveryStatus"));
            }
            
	       
	     }
	  }
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws URISyntaxException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
		if (args.length != 2) {
			System.out.printf("Two parameters are required- <input dir> <output dir>\n");
			System.exit(2);
			}
		  /*  Configuration conf = new Configuration();
		    Job job = new Job(conf);
		    job.setJobName("ReducerSideJoin");
			DistributedCache.addCacheFile(new URI("/user/hadoop/joinProject/data/DeliveryStatusCodes"),conf);
			job.setJarByClass(ReduceSideJoinDriver.class);
			//specifying the custom reducer class
			job.setReducerClass(SMSReducer.class); 
			//Specifying the input directories(@ runtime) and Mappers independently for inputs from multiple sources
			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UserFileMapper.class);
			MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, DeliverDetailsMapper.class);
             
			//Specifying the output directory @ runtime
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			 
			boolean success = job.waitForCompletion(true);
			System.out.println(success);*/
		
		
		

	}

}
