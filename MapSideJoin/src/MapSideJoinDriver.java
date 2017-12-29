import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapSideJoinDriver {

	
	public static class MapJoinDistributedCacheMapper extends Mapper<LongWritable, Text, Text, Text> {
		  
	    private static HashMap<String, String> DepartmentMap = new HashMap<String, String>();
	    private BufferedReader brReader;
	    private String strDeptName = "";
	    private Text txtMapOutputKey = new Text("");
	    private Text txtMapOutputValue = new Text("");
	  
	    enum MYCOUNTER {
	    RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
	    }
	  
	protected void setup(Context context) throws IOException,InterruptedException {
	  System.out.println("***********HELLO***********");
	 
	 URI[] cacheFiles = DistributedCache.getCacheFiles(context.getConfiguration());
	    System.out.println("***********HELLO***********"+cacheFiles[0].getPath());
	    
	    for (URI eachPath : cacheFiles) {
	    	Path temp = new Path(eachPath.getPath());
	    	FSDataInputStream fis =  FileSystem.get(context.getConfiguration()).open(temp);
	    	System.out.println("***********HELLO***********"+temp.getName());
	        if (temp.getName().toString().trim().equals("department.txt")) {
	        context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
	        //loadDepartmentsHashMap(temp, context);
	        loadDepartmentsHashMap(fis, context);
	        }
	    }
	  
	}
	
	private void loadDepartmentsHashMap(DataInputStream stream, Context context) throws IOException {
	    String strLineRead = "";
	    try {
            BufferedReader brReader = new BufferedReader(
                    new InputStreamReader(stream));
	        // Read each line, split and load to HashMap
	        while ((strLineRead = brReader.readLine()) != null) {
	            String deptFieldArray[] = strLineRead.split("\\t");
	            DepartmentMap.put(deptFieldArray[0].trim(),    deptFieldArray[1].trim());
	        }
	    } catch (FileNotFoundException e) {
	        e.printStackTrace();
	        context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
	    } catch (IOException e) {
	        context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
	        e.printStackTrace();
	    }finally {
	        if (brReader != null) {
	            brReader.close();    
	        }
	    }
	}
	  
	
	  
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	  
	    context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);
	      
	    if (value.toString().length() > 0) {
	    String arrEmpAttributes[] = value.toString().split("\\t");
	      
	    try {
	        strDeptName = DepartmentMap.get(arrEmpAttributes[3].toString());
	    } finally {
	        strDeptName = ((strDeptName.equals(null) || strDeptName    .equals("")) ? "NOT-FOUND" : strDeptName);
	    }
	      
	    txtMapOutputKey.set(arrEmpAttributes[0].toString());
	      
	    txtMapOutputValue.set(arrEmpAttributes[0].toString() + "\t"
	    + arrEmpAttributes[1].toString() + "\t"
	    + arrEmpAttributes[2].toString() + "\t"    
	    + arrEmpAttributes[3].toString() + "\t" + strDeptName);
	      
	    }
	    context.write(txtMapOutputKey, txtMapOutputValue);
	    strDeptName = "";
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
//		if (args.length != 2) {
//			System.out.printf("Two parameters are required- <input dir> <output dir>\n");
//			System.exit(2);
//			}
		
		    Configuration conf = new Configuration();
		    Job job = new Job(conf);
		    //hdfs://localhost:54310/user/mapjoin/department.txt
	      	// This is HDFS Path - No need to mention hdfs://localhost:54310/ 
		    DistributedCache.addCacheFile(new URI("/user/mapjoin/department.txt"),job.getConfiguration());
			 
	            
			job.setJobName("Map-side join with text lookup file in DCache");  
			job.setJarByClass(MapSideJoinDriver.class);
			FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:54310/user/mapjoin/users.txt"));
			
			Path outputPath= new Path("hdfs://localhost:54310/user/mapjoin/output/");
			FileOutputFormat.setOutputPath(job, outputPath);
			outputPath.getFileSystem(conf).delete(outputPath);
			
			job.setMapperClass(MapJoinDistributedCacheMapper.class);
			 
			job.setNumReduceTasks(0);
			 
			boolean success = job.waitForCompletion(true);
			System.out.println(success);

	}

}
