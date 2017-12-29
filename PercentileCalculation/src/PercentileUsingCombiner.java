import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * @author ubuntu
 *
 *Your percentile score = { (No, of people who got less than you/ equal to you) / (no. of people appearing in the exam) } x 100 
 *Thus, if in an exam, 10 people give the test and 9 people get either less than what you got or equal to what you got, your percentile score is: 
 *{9/10} x 100 = 90 percentile. 
 *
 */

public class PercentileUsingCombiner {
	 private final static IntWritable one = new IntWritable(1);
	
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Numbers> {
	      
	       private Text word = new Text();
	           
	       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	           String line = value.toString();
	           StringTokenizer tokenizer = new StringTokenizer(line);
	           while (tokenizer.hasMoreTokens()) {
	               int number =  Integer.parseInt(tokenizer.nextToken().toString().trim());
	               context.write(new IntWritable(number) ,new Numbers(new IntWritable(number),one) );
	           }
	       }
	    }
	
	public static class Combiner extends Reducer<IntWritable, Numbers, IntWritable, Numbers>{
		//Logger logger = LoggerFactory.getLogger(WordCountCombiner.class);

	    protected void reduce(IntWritable key, Iterable<Numbers> values,
	                          Context context)
	            throws IOException, InterruptedException {
	        //System.out.println("Entering WordCountCombiner.reduce() " );
	        int count = 0;
	        for(Numbers iw : values){
	        	count++;
	        }
	        //logger.debug(key + " -> " + sum);
	        context.write(one, new Numbers(key,new IntWritable(count)));
	        //logger.debug("Exiting WordCountCombiner.reduce()");
	    }
	}
	
	
	public static class Reduce extends Reducer<IntWritable, Numbers, IntWritable, DoubleWritable > {
 	   
	       public void reduce(IntWritable key, Iterable<Numbers> values, Context context) 
	         throws IOException, InterruptedException {
	           int val = 0;
	           int count =0;
	           int lastIndex = 0;
	           double percentile = 0;
	           
	           
	           java.util.Map<Integer,Integer> treeMap = new TreeMap<Integer,Integer>();
	                     
	           
	           List<Integer> doubleValues = new ArrayList<Integer>();
	           for (Numbers vals : values) {
	               count+=vals.getNumValCount().get();
	               doubleValues.add(vals.getNumValCount().get());
	               treeMap.put(vals.getNumVal().get(), vals.getNumValCount().get());
	           }
	           System.out.println("Hello"+treeMap +" count="+count +" - doubleValues="+doubleValues);
	           int size = treeMap.size();
	           
	           int treeKey=0, treeVal=0;
	           int treeKeyInt =0;
	           int treeValInt = 0;
	           for(int i=0;i<size;i++){
	        	   treeKey = (Integer) treeMap.keySet().toArray()[i];
	        	   treeVal = treeMap.get(treeKey);
	        	   treeKeyInt =0;
	        	   treeValInt =0;
	        	   System.out.println(treeKey+"*********"+treeVal);
	        	   // Find how many peoples are before
	        	    for(int index = 0;index< i;index++){
	        	    	System.out.println(doubleValues.get(index));
	        	    	treeValInt +=doubleValues.get(index);
	        	    }
	        	           	   
	        	   System.out.println(treeKey+"*********"+treeValInt);
	        	          	      
	        	   
	        	   percentile = (((treeValInt+1)*100)/count);
	        	   for(int j=0;j<treeVal;j++){
	        		   
	        	       context.write(new IntWritable(treeKey), new DoubleWritable(percentile));
	        	   }
               }
	           
	       }
	    }
	
	
	private static class Numbers implements WritableComparable<Numbers> {

	       

		IntWritable numVal;
        IntWritable numValCount;

        public Numbers(IntWritable numVal, IntWritable numValCount) {
            this.numVal = numVal;
            this.numValCount = numValCount;
        }
        
        public Numbers() {
            this.numVal = new IntWritable();
            this.numValCount = new IntWritable();

        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
         */
        public void write(DataOutput out) throws IOException {
            this.numVal.write(out);
            this.numValCount.write(out);

        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
         */
        public void readFields(DataInput in) throws IOException {

            this.numVal.readFields(in);
            this.numValCount.readFields(in);
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Comparable#compareTo(java.lang.Object)
         */
        public int compareTo(Numbers pop) {
            if (pop == null)
                return 0;
            int intcnt = numVal.compareTo(pop.numVal);
          //  System.out.println(numVal+"&&&&&&&&&&&"+pop.numVal);
            if (intcnt != 0) {
                return intcnt;
            } else {
            //	System.out.println(numValCount+"^^^^^^^^^^^"+pop.numValCount);
            	return numValCount.compareTo(pop.numValCount);
            }
        }
        
        

        /**
		 * @return the numVal
		 */
		public IntWritable getNumVal() {
			return numVal;
		}

		/**
		 * @param numVal the numVal to set
		 */
		public void setNumVal(IntWritable numVal) {
			this.numVal = numVal;
		}

		/**
		 * @return the numValCount
		 */
		public IntWritable getNumValCount() {
			return numValCount;
		}

		/**
		 * @param numValCount the numValCount to set
		 */
		public void setNumValCount(IntWritable numValCount) {
			this.numValCount = numValCount;
		}

		/*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#toString()
         */
        public String toString() {

            return numVal.toString() + ":" + numValCount.toString();
        }

    }
	
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		long startTime = System.currentTimeMillis();
    	Configuration conf = new Configuration();
            
        Job job = new Job(conf, "wordcount");
        
        job.setJarByClass(PercentileUsingCombiner.class);
        //This will be Map Output key,value
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Numbers.class);
            
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setCombinerClass(Combiner.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
            
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/percentile/percentile.txt"));
        Path outputPath= new Path("hdfs://localhost:54310/user/percentile/output/");
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
            
        //job.waitForCompletion(true);
        int returnValue = job.waitForCompletion(true) ? 0 : 1;
        long endTime = System.currentTimeMillis();
		long executionTime = endTime- startTime;
		System.out.println("executionTime :: "+executionTime);

	}

}
