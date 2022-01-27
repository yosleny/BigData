package BigData;

import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FilterNullCombiner extends  Reducer<CompositeKey, Text, CompositeKey, Text> {
	
    public void reduce(CompositeKey key, Iterable<Text> values,
            Context context
	) throws IOException, InterruptedException {
    	
    		double sum = 0;
	        int l = 1;
	        double bmi = 0;
	        
	        for (Text val : values) {
	        	if(key.toString().equals("1:12")) {
	        		
		        	String[] fields = val.toString().split(",");
		        	
		        	if(fields.length == 12) { //is one of the bmi values we want to use to calculate the mean
		        		bmi = Double.parseDouble(fields[9]);
		        		l++;
		        	}
		            sum += bmi;
	        		context.write(new CompositeKey(new Text("0"), 12), val);
	        	}
	        	else
	        		context.write(key, val);
	        }
	        sum = sum / l;
	        
		if(key.toString().equals("1:12") && sum > 0)
			context.write(new CompositeKey(new Text("r"), 1), new Text(Double.toString(Precision.round(sum, 2))));
		
	}
}
