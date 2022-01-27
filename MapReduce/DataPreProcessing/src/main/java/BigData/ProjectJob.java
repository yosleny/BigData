package BigData;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProjectJob {

	public static void main(String[] args) throws Exception {
		
		try {
			
			Configuration conf = new Configuration();

			Job j = Job.getInstance(conf,"Filtering");
			j.setJarByClass(ProjectJob.class);
			
			j.setPartitionerClass(KeyPartitioner.class);
			j.setGroupingComparatorClass(GroupComparator.class);
			
			j.setMapperClass(FilterNullMapper.class);
			j.setCombinerClass(FilterNullCombiner.class);
			j.setReducerClass(FilterNullReduce.class);
			
			
			j.setMapOutputKeyClass(CompositeKey.class);
			j.setMapOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(j, new Path(args[0]));
		    FileOutputFormat.setOutputPath(j, new Path(args[1]));
		    
		    
			j.waitForCompletion(true);
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

