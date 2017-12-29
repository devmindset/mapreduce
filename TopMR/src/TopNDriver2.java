import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;






public class TopNDriver2 {
	
	public static int intReducerCounter = 0;

	private static class TopMapper extends
			Mapper<LongWritable, Text, NullWritable, Employee> {
		private TreeMap<Employee, Double> employeeMap = new TreeMap<Employee, Double>();

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (line != null && !line.equalsIgnoreCase("")) {
				String[] empl = line.split(",");//("\\t");
				System.out.println(Arrays.toString(empl));
				Employee employee = new Employee();
				Double salary = Double.parseDouble(empl[2]);
				employee.setEmpid(empl[0]);
				employee.setName(empl[1]);
				employee.setDeptId(empl[3]);
				employee.setSalary(salary);
				employeeMap.put(employee, salary);
				if (employeeMap.size() > 3) {
					
					System.out.println(employeeMap.firstKey() +"*******"+employeeMap.lastKey());
					//This method returns the last (highest) key currently in this map.
					//employeeMap.remove(employeeMap.lastKey());
					
					//This method returns the first (lowest) key currently in this map.
					employeeMap.remove(employeeMap.lastKey());
				}
			}
		}

		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, NullWritable, Employee>.Context context)
				throws IOException, InterruptedException {
			System.out.println("Mapper Cleanup===" + employeeMap);
			for (Employee emp : employeeMap.keySet()) {
				context.write(NullWritable.get(), emp);
			}

		}

	}
	
	
	public static class TopReducer extends
			Reducer<NullWritable, Employee, NullWritable, Employee> {
		private TreeMap<Employee, Double> empMap = new TreeMap<Employee, Double>();

		protected void reduce(NullWritable key, Iterator<Employee> values,
				Context context) throws IOException, InterruptedException {
			int cnt = 0;

			while (values.hasNext()) {
				this.empMap.put(values.next(), new Double(1));
				if (empMap.size() > 3) {
					empMap.remove(empMap.lastKey());
				}
			}

			System.out.println("===" + empMap);
			for (Employee emp : this.empMap.keySet()) {
				context.write(NullWritable.get(), emp);
			}

		}

	}

	
	private static class Employee implements WritableComparable<Employee> {
        private String empid;
		private String name;
		private Double salary;
        private String deptId;
		
        
		public String getEmpid() {
			return empid;
		}

		public void setEmpid(String empid) {
			this.empid = empid;
		}

		public String getDeptId() {
			return deptId;
		}

		public void setDeptId(String deptId) {
			this.deptId = deptId;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Double getSalary() {
			return salary;
		}

		public void setSalary(Double salary) {
			this.salary = salary;
		}

		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, empid);
			WritableUtils.writeString(out, name);
			out.writeDouble(salary);
			WritableUtils.writeString(out, deptId);
		}

		public void readFields(DataInput in) throws IOException {

			this.empid = WritableUtils.readString(in);
			this.name = WritableUtils.readString(in);
			this.salary = in.readDouble();
            this.deptId = WritableUtils.readString(in);			
		}

		public int compareTo(Employee o) {

			if (o == null)
				return 0;
			//Descending order
			int cnt = -1 * this.salary.compareTo(o.salary);
			return cnt == 0 ? this.name.compareTo(o.name) : cnt;
		}

		public boolean equals(Object obj) {
			Employee emp = (Employee) obj;
			return (this.name.equalsIgnoreCase(emp.name) && this.salary == emp.salary);

		}

		public int hashCode() {
			int hashcode = this.name.hashCode();
			hashcode = hashcode + salary.hashCode();
			return hashcode;
		}

		public String toString() {
			return name + ":" + salary;
		}
	}
	
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
			//FileUtils.deleteDirectory(new File("/Local/data/output"));

			Configuration conf = new Configuration();
			Job job = new Job(conf,"Top N Records");
			job.setJarByClass(TopNDriver2.class);
			job.setNumReduceTasks(1);
			job.setMapperClass(TopMapper.class);
			job.setReducerClass(TopReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Employee.class);
			FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/grouping/employee.txt"));
			Path outputPath = new Path("hdfs://localhost:54310/user/wordcount/output/topNRecords/");
	        FileOutputFormat.setOutputPath(job, outputPath);
	        outputPath.getFileSystem(conf).delete(outputPath);
			System.out.println(job.waitForCompletion(true) ? 0 : 1);

	}

}
