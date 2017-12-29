
//Example Driver class for Word count program
import java.io.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WebLogReader {
	
	
    public static class WebLogWritable implements WritableComparable<WebLogWritable> {
      private Text siteURL, reqDate, timestamp, ipaddress;
      private IntWritable reqNo;

      
      /**
       * All Writable implementations must have a default constructor so that the MapReduce framework can instantiate them, then populate their fields by calling readFields() . 
       * Writable instances are mutable and often reused so we have provided write() method. We have also provided custom constructor to set the object fields.
       */
      
      //Default Constructor
      public WebLogWritable() {
          this.siteURL = new Text();
          this.reqDate = new Text();
          this.timestamp = new Text();
          this.ipaddress = new Text();
          this.reqNo = new IntWritable();
      }

      //Custom Constructor
      public WebLogWritable(IntWritable reqno, Text url, Text rdate, Text rtime, Text rip) {
          this.siteURL = url;
          this.reqDate = rdate;
          this.timestamp = rtime;
          this.ipaddress = rip;
          this.reqNo = reqno;
      }

      //Setter method to set the values of WebLogWritable object
      public void set(IntWritable reqno, Text url, Text rdate, Text rtime, Text rip) {
          this.siteURL = url;
          this.reqDate = rdate;
          this.timestamp = rtime;
          this.ipaddress = rip;
          this.reqNo = reqno;
      }

      //to get IP address from WebLog Record
      public Text getIp() {
          return ipaddress; 
      }
   
      @Override
      //overriding default readFields method. 
      //It de-serializes the byte stream data
      public void readFields(DataInput in) throws IOException {
          ipaddress.readFields(in);
          timestamp.readFields(in);
          reqDate.readFields(in);
          reqNo.readFields(in);
          siteURL.readFields(in);
      }

      @Override
      //It serializes object data into byte stream data
      public void write(DataOutput out) throws IOException {
          ipaddress.write(out);
          timestamp.write(out);
          reqDate.write(out);
          reqNo.write(out);
          siteURL.write(out);
      }
   
      /**
       * The compareTo() method returns a negative integer, 0,  or a positive integer, if our object is less than, equal to, or greater than the object being compared 
       * to respectively.
       */
      
      @Override
      public int compareTo(WebLogWritable o) {
          if (ipaddress.compareTo(o.ipaddress)==0)
          {
            return (timestamp.compareTo(o.timestamp));
          }
          else return (ipaddress.compareTo(o.ipaddress));
      }

      /**
       * In equals() method, we consider the objects equal if both the IP addresses and the time-stamps are the same. 
       * If the objects are not equal, we decide the sort order first based on the user IP address and then based on the time-stamp.
       */
      @Override
      public boolean equals(Object o) {
          if (o instanceof WebLogWritable) 
          {
            WebLogWritable other = (WebLogWritable) o;
            return ipaddress.equals(other.ipaddress) && timestamp.equals(other.timestamp);
          }
          return false;
      }

      /**
       * The hashCode() method is used by the HashPartitioner, the default partitioner in MapReduce, to choose a reduce partition. 
       * Usage of IP Address in our hashCode() method ensures that the intermediate WebLogWritable data will be partitioned based on the request host name/IP address.
       */
      @Override
      public int hashCode() {
          return ipaddress.hashCode();
      }
    }


    public static class WebLogMapper extends Mapper <LongWritable, Text, WebLogWritable, IntWritable> {
      private static final IntWritable one = new IntWritable(1);
  
      private WebLogWritable wLog = new WebLogWritable();

      private IntWritable reqno = new IntWritable();
      private Text url = new Text();
      private Text rdate = new Text();
      private Text rtime = new Text();
      private Text rip = new Text();

      public void map(LongWritable key, Text value, Context context) 
                            throws IOException, InterruptedException {
          String[] words = value.toString().split("\t") ;

          System.out.printf("Words[0] - %s, Words[1] - %s, Words[2] - %s, length - %d", 
                              words[0], words[1], words[2], words.length);

          reqno.set(Integer.parseInt(words[0]));
          url.set(words[1]);
          rdate.set(words[2]);
          rtime.set(words[3]);
          rip.set(words[4]);

          wLog.set(reqno, url, rdate, rtime, rip);

          context.write(wLog, one);
      }
    }
    
    public static class WebLogReducer extends Reducer <WebLogWritable, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Text ip = new Text();

        public void reduce(WebLogWritable key, Iterable<IntWritable> values, Context context) 
                                  throws IOException, InterruptedException {
            int sum = 0;
            ip = key.getIp(); 
    
            for (IntWritable val : values) 
            {
              sum++ ;
            }
            result.set(sum);
            context.write(ip, result);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job();
        job.setJobName("WebLog Reader");

        job.setJarByClass(WebLogReader.class);

        job.setMapperClass(WebLogMapper.class);
        job.setReducerClass(WebLogReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(WebLogWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/customiptype/Web_Log.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/customiptype/output/result"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}