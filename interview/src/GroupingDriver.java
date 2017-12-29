import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class GroupingDriver {
	
	public static class Student implements WritableComparable<Student>{

		private Text year;
		private Text classStandard;
		private Text section;
		//private Text rollNo;
		//private Text marks; 
		
		public Student() {
			this.year = new Text();
			this.classStandard = new Text();
			this.section = new Text();
		
		}

		public Student(Text year, Text classStandard, Text section
				 ) {
			this.year = year;
			this.classStandard = classStandard;
			this.section = section;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.year.readFields(in);
			this.classStandard.readFields(in);
			this.section.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			this.year.write(out);
			this.classStandard.write(out);
			this.section.write(out);
		}

		@Override
		public int compareTo(Student stud) {
			int status = 0;
			if(stud == null)
				 return 0;
			
			status = -1*this.year.compareTo(stud.year);
		    if(status!=0){
		       return status; 
		    } else {
		    	status = -1*this.classStandard.compareTo(stud.classStandard);
			    if(status!=0){
			    	return status;
			    } else {
			    	status = -1*this.section.compareTo(stud.section);
			    	return status;
			    }
		    }
		}
		
		public int hashCode() {
			int hashcode = this.year.hashCode();
			hashcode = hashcode + classStandard.hashCode();
			hashcode = hashcode + section.hashCode();
			return hashcode;
		}

		public String toString() {
			return this.year + ":" + this.classStandard+":"+ this.section;
		}

		public boolean equals(Object obj) {
			if(obj instanceof Student){
			   Student std = (Student) obj;
			   return this.year.equals(std.year) &&  this.classStandard.equals(std.classStandard) && this.section.equals(std.section);
			}
			return false;
		}
			
		

		/**
		 * @return the year
		 */
		public Text getYear() {
			return year;
		}

		/**
		 * @param year the year to set
		 */
		public void setYear(Text year) {
			this.year = year;
		}

		/**
		 * @return the classStandard
		 */
		public Text getClassStandard() {
			return classStandard;
		}

		/**
		 * @param classStandard the classStandard to set
		 */
		public void setClassStandard(Text classStandard) {
			this.classStandard = classStandard;
		}

		/**
		 * @return the section
		 */
		public Text getSection() {
			return section;
		}

		/**
		 * @param section the section to set
		 */
		public void setSection(Text section) {
			this.section = section;
		}
		
	}
	
	
	public static class GroupingMapper extends Mapper<LongWritable, Text, Student,Text> {
		public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException {
			
           if(null!=value && value.toString().trim().length()>0){  
			  String[] arr = value.toString().trim().split(",");
              Student stud = new Student();
              stud.setYear(new Text(arr[1].trim()));
              stud.setClassStandard(new Text(arr[2].trim()));
              stud.setSection(new Text(arr[3].trim()));
              System.out.println(stud+":"+arr[0].trim()+":"+arr[4].trim());
              context.write(stud,new Text(arr[0].trim()+":"+arr[4].trim()));
           }
		}
	}
	
	public static class Combiner extends Reducer<Student,Text, Student,Text> {
		public void reduce(Student key,  Iterable<Text> rollNoMarks,Context context) throws IOException, InterruptedException {
			int counter = 0;
			System.out.println("--------Combiner Start -------");
			Map<String, Integer> sortedMap = new HashMap<String,Integer>();
			
			String rollNo=null;
			int highestMark = 0;
			int count =0;
			
			for (Text val : rollNoMarks) {
				String arr[]= val.toString().split(":");
				if(count==0){
					highestMark= Integer.parseInt(arr[1]);
					rollNo= arr[0];
					count++;
					sortedMap.put(rollNo, highestMark);
				} else if(highestMark<Integer.parseInt(arr[1])){
					highestMark= Integer.parseInt(arr[1]);
					rollNo= arr[0];
					sortedMap.clear();
					sortedMap.put(rollNo, highestMark);
				} else if(highestMark==Integer.parseInt(arr[1])){
					highestMark= Integer.parseInt(arr[1]);
					rollNo= arr[0];
					sortedMap.put(rollNo, highestMark);
				}	
		    }
			for(Map.Entry<String, Integer> map: sortedMap.entrySet()){
			   context.write(key,new Text(map.getKey()+":"+map.getValue()));
			}
			System.out.println("--------Combiner End -------");
		}
	}
	
	
	public static class GroupingReducer extends Reducer<Student,Text,Student,Text> {
		public void reduce(Student key, Iterable<Text> rollNoMarks,Context context) throws IOException, InterruptedException {
			System.out.println("--------Reducer Start -------");
           
			Map<String, Integer> sortedMap = new HashMap<String,Integer>();
			String rollNo=null;
			int highestMark = 0;
			int count =0;
			
			for (Text val : rollNoMarks) {
				String arr[]= val.toString().split(":");
				if(count==0){
					highestMark= Integer.parseInt(arr[1]);
					rollNo= arr[0];
					count++;
					sortedMap.put(rollNo, highestMark);
				} else if(highestMark<Integer.parseInt(arr[1])){
					highestMark= Integer.parseInt(arr[1]);
					rollNo= arr[0];
					sortedMap.clear();
					sortedMap.put(rollNo, highestMark);
				} else if(highestMark==Integer.parseInt(arr[1])){
					highestMark= Integer.parseInt(arr[1]);
					rollNo= arr[0];
					sortedMap.put(rollNo, highestMark);
				}	
		    }
			for(Map.Entry<String, Integer> map: sortedMap.entrySet()){
			   context.write(key,new Text(map.getKey()+":"+map.getValue()));
			}
		}
	}
	
	
	
	

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        //Job job = Job.getInstance(conf, "GroupMR");
        Job job = new Job(conf, "GroupingDriver");
        job.setJarByClass(GroupingDriver.class);
        job.setMapperClass(GroupingMapper.class);
        job.setReducerClass(GroupingReducer.class);
        job.setCombinerClass(Combiner.class);
       // job.setReducerClass(GroupReducer.class);
        job.setOutputKeyClass(Student.class);
        job.setOutputValueClass(Text.class);
       // FileInputFormat.setMaxInputSplitSize(job, 10);
      //  FileInputFormat.setMinInputSplitSize(job, 100);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/grouping/student.txt"));
    	Path outputPath= new Path("hdfs://localhost:54310/user/grouping/output/");
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
        System.out.println(job.waitForCompletion(true) ? 0 : 1);

	}

}
