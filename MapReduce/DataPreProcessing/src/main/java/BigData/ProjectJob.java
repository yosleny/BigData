package BigData;

import java.io.BufferedReader;
import java.io.IOException;
//import java.io.InputStream;
import java.io.InputStreamReader;
//import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProjectJob {
	
	public static double readHDFSFileContents(String uri, Configuration conf) throws Exception {
		
	    Path pt = new Path(uri + "/part-r-00000");
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

        String line;
        String[] values = new String[2];
        line = br.readLine();
        System.out.println(line);
        values = line.split("\t");
        while ((line = br.readLine()) != null) {
            System.out.println(line);
            line = br.readLine();
            values = line.split(",");
        }
        br.close();
	    
	    return Double.parseDouble(values[1]);
	    
	    
	  }

	public static void main(String[] args) throws Exception {
		
		try {
			
			Configuration conf = new Configuration();
			conf.setDouble("bmi", 10);
			Job j = Job.getInstance(conf,"Filtering");
			j.setJarByClass(ProjectJob.class);
			
			j.setMapperClass(FilterNullMapper.class);
			j.setCombinerClass(FilterNullCombiner.class);
			j.setReducerClass(FilterNullReduce.class);
			
			j.setMapOutputKeyClass(Text.class);
			j.setMapOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(j, new Path(args[0]));
		    FileOutputFormat.setOutputPath(j, new Path(args[1]));
		    
			j.waitForCompletion(true);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

