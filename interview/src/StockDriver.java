import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class StockDriver {

	public static final int _topN = 3;
	static int  cnt = 0;
	public static class Stock implements WritableComparable<Stock>{
		
		private Text symbol;
		private Text date;
		private Text active;
		
		public Stock() {
			this.symbol= new Text();
			this.date = new Text();
			this.active = new Text();
		}

		public Stock(Text symbol, Text date) {
			this.symbol = symbol;
			this.date = date;
			this.active = active;
		}

		

		/**
		 * @return the symbol
		 */
		public Text getSymbol() {
			return symbol;
		}

		/**
		 * @param symbol the symbol to set
		 */
		public void setSymbol(Text symbol) {
			this.symbol = symbol;
		}

		/**
		 * @return the date
		 */
		public Text getDate() {
			return date;
		}

		/**
		 * @param date the date to set
		 */
		public void setDate(Text date) {
			this.date = date;
		}

		/**
		 * @return the sold
		 */
		public Text getActive() {
			return active;
		}

		/**
		 * @param sold the sold to set
		 */
		public void setActive(Text active) {
			this.active = active;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.symbol.readFields(in);
			this.date.readFields(in);
			//this.active = in.readInt();
			this.active.readFields(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			this.symbol.write(out);
			this.date.write(out);
			//out.writeInt(active);
			this.active.write(out);
		}

		@Override
		public int compareTo(Stock stock) {
			int status = 0;
			if(stock==null)
				return 0;
			 status = -1*this.symbol.compareTo(stock.symbol);
			 
			 if(status!=0){
				 return status;
			 } else {
				 status = -1*this.date.compareTo(stock.date);
				// System.out.println("status :: "+status);
				 return status;
			}
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			int hashcode = this.symbol.hashCode();
			hashcode = hashcode + date.hashCode();
			return hashcode;
			
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if(obj instanceof Stock){
				Stock stock = (Stock)obj;
				boolean equalStatus= this.symbol.equals(stock.getSymbol()) &&  this.date.equals(stock.getDate());
				//System.out.println("equalStatus:::"+equalStatus);
				return equalStatus;
			}
			return false;
		}

		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "Stock [symbol=" + symbol + ", date=" + date+ ", active=" + active + "]";
		}
		
		
	}
	
	//Input is yyyy-MM-dd HH:mm:ss
	private static long dateStringToMilliseconds(String date){
	      try{
	    	  String[] formatDate= date.replaceAll("\\s+"," ").split(" ");
	    	  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	    	  Date dt = sdf.parse(formatDate[0].trim());
	    	  long val = dt.getTime();
	    	  //System.out.println(date+":::"+val);
	    	  return val;
	      }catch(ParseException e){
	    	  
	      }
	      return 0;
		}
	
	private static long dateTimeStringToMilliseconds(String date){
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      try{
    	  Date dt = sdf.parse(date.trim());
    	  long val = dt.getTime();
    	  return val;
      }catch(ParseException e){
    	  
      }
      return 0;
	}
	
	
	private static class TopMapper extends 	Mapper<LongWritable, Text,   Stock,Text> {
		Map<Stock, Integer> treeMap = new TreeMap<Stock,Integer>();
      protected void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		     String line = value.toString();
		     if (line != null && !line.equalsIgnoreCase("")) {
			     String[] stock = line.split(",");//("\\t");
			     long datediff = dateTimeStringToMilliseconds("2015-04-28 04:00:00") - dateTimeStringToMilliseconds(stock[1].trim());
			     if( datediff > 0 && datediff < 3600000){
			    	 System.out.println("line ::::: "+line);
				     Stock stockData = new Stock();
				     stockData.setSymbol(new Text(stock[0].trim()));
				     String data = String.valueOf(dateStringToMilliseconds(stock[1].trim()));
				     stockData.setDate(new Text(data));
				     if(treeMap.size()==0){
				    	 treeMap.put(stockData, Integer.parseInt(stock[2].trim())+Integer.parseInt(stock[3].trim()));
				     } else if(!treeMap.containsKey(stockData)){
				        treeMap.put(stockData, Integer.parseInt(stock[2].trim())+Integer.parseInt(stock[3].trim()));
				     } else if(treeMap.containsKey(stockData)){
				    	int val= treeMap.get(stockData);
				    	val+= Integer.parseInt(stock[2].trim())+Integer.parseInt(stock[3].trim());
				    	treeMap.put(stockData, val);
				     }
		         }
			}
	    }
      
      protected void cleanup(
				Mapper<LongWritable, Text, Stock,Text>.Context context)
				throws IOException, InterruptedException {
			    
			     for(Map.Entry<Stock, Integer> stk: treeMap.entrySet()){
			    	 Stock sk = stk.getKey();
			    	 sk.setActive(new Text(stk.getValue().toString()));
			    	 System.out.println("Mapper Cleanup Stock===" + sk);
					 context.write(sk,new Text(stk.getValue().toString()));
			     }
		}
    }
	
	public static class FirstPartitioner
	extends Partitioner<Stock, Text> {
	

	@Override
	public int getPartition(Stock key, Text arg1, int numPartitions) {
		// multiply by 127 to perform some mixing
		return Math.abs( key.getSymbol().hashCode() * 127) % numPartitions;
	}
	}
	
	public static class KeyComparator extends WritableComparator {
		protected KeyComparator() {
		super(Stock.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2) {
			System.out.println(":::::::ip1:::");
			
      		Stock ip1 = (Stock) w1;
		    Stock ip2 = (Stock) w2;
		    System.out.println("ip1:::"+ip1);
		    //Another approach
		    //int cmp =   ip1.getActive().compareTo(ip2.getActive());
		    //return -1*cmp;
		    Integer int1 = Integer.valueOf(ip1.getActive().toString());
		    Integer int2 = Integer.valueOf(ip2.getActive().toString());
		    int retval =  int1.compareTo(int2);
		    return -1*retval;
		   }
	}
	
	public static class TopReducer extends
	Reducer< Stock,Text, Stock, Text> {

protected void reduce(Stock key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {
	
	for (Text val : values) {
		if(_topN>cnt){
		   context.write(key,val);
		   cnt++;
		}
	}
}
}
	
/*	public static class TopReducer extends
			Reducer< Stock,Text, Stock, Text> {

		private TreeMap<Stock, Integer> stockMap = new TreeMap<Stock, Integer>();
		
		protected void reduce(Stock key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			for (Text val : values) {
				this.stockMap.put(key,Integer.valueOf(val.toString()));
				if (stockMap.size() > _topN) {
					stockMap.remove(stockMap.lastKey());
				}
			}

			for(Map.Entry<Stock, Integer> stk: stockMap.entrySet()){
		    	 Stock sk = stk.getKey();
		    	 sk.setActive(new Text(stk.getValue().toString()));
		    	 System.out.println("Mapper Cleanup Stock===" + sk);
				 context.write(sk,new Text(stk.getValue().toString()));
		     }

		}
	}*/
	
	
	
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf,"Top N Stocks");
		job.setJarByClass(StockDriver.class);
		job.setNumReduceTasks(1);
		job.setMapperClass(TopMapper.class);
		job.setReducerClass(TopReducer.class);
		job.setOutputKeyClass(Stock.class);
		job.setOutputValueClass(Text.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/grouping/stockdata.txt"));
		Path outputPath = new Path("hdfs://localhost:54310/user/grouping/amex/");
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
		System.out.println(job.waitForCompletion(true) ? 0 : 1);
	}

}
