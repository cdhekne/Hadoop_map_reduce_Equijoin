

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class equijoin{

	private static class myMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context){
			String line = value.toString();
			line = line.replaceAll("\\s+","");
			List<String> s = Arrays.asList(line.split(","));
			DoubleWritable d = new DoubleWritable(Double.parseDouble(s.get(1)));
			try {
				context.write(d, new Text(line));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	} 

	private static class myReducer extends Reducer<DoubleWritable, Text, NullWritable, Text> {

		@Override
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) {
			List<String> rList = new ArrayList<String>();
			List<String> sList = new ArrayList<String>();
			for (Text val : values) {
				List<String> s = Arrays.asList(val.toString().split(","));
				if(s.get(0).equals("R")==true)
					rList.add(val.toString());
				else
					sList.add(val.toString());
			}
			if(!((rList.size()==1 && sList.size()==0)||(rList.size()==0 && sList.size()==1))){
				String opString = "";
				int catCounter = rList.size()*sList.size();
				int counter =0;
				for(String i:rList){
					for(String j: sList){
						if(counter==catCounter-1)
							opString+=i+"," + j;
						else{
							opString+=i+"," + j+"\n";
							counter++;
						}
					}
				}
				try {
					context.write(null, new Text(opString));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}


	public static void main(String[] args){
		Configuration conf = new Configuration();
		try{
			@SuppressWarnings("deprecation")
			Job job = new Job(conf,"Assginment4_DDS");

			job.setJarByClass(equijoin.class);

			job.setMapperClass(myMap.class);
			job.setReducerClass(myReducer.class);

			job.setOutputKeyClass(DoubleWritable.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			boolean status = job.waitForCompletion(true);
			System.out.println(status);
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
}