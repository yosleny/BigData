package BigData;

import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterNullCombiner extends  Reducer<Text,Text,Text,Text> {
	
    public void reduce(Text key, Iterable<Text> values,
            Context context
	) throws IOException, InterruptedException {
    	
    	double sum = 0;
		
	       
	        int l = 1;
	        double bmi = 0;
	        
	        for (Text val : values) {
	        	if(key.toString().equals("bmi")) {
		        	String[] fields = val.toString().split(",");
		        	
		        	if(fields.length == 1) { //is one of the bmi values we want to use to calculate the mean
		        		bmi = Double.parseDouble(fields[0]);
		        		l++;
		        	}
		        	else
		        		context.write(key, val);
		            
		            sum += bmi;
	        	}
	        	else
	        	  context.write(key, val);
	        }
	        sum = sum / l;
	        
		if(key.toString().equals("bmi") && sum > 0)
			context.write(new Text("bmi"), new Text(Double.toString(Precision.round(sum, 2))));
		
	}
}
