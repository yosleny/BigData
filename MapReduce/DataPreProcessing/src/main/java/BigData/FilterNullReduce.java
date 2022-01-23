package BigData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterNullReduce extends Reducer<Text, Text, Text, Text> {

	@Override
	 protected void reduce(Text key, Iterable<Text> values, Context context)
	            throws IOException, InterruptedException
	        {
		List<Text> cache = new ArrayList<Text>();
		
		double bmi = 0;
			for (Text val : values) {
	        	bmi = 0;
				String[] fields = val.toString().split(",");
				
				if(key.toString().equals("bmi") && fields.length == 1)
					bmi = Double.parseDouble(fields[0]);
				else if(key.toString().equals("bmi") && fields.length == 12)
				{
					if(bmi == 0)
						cache.add(new Text(String.join(",", fields)));
					else 
					{
						fields[9] = Double.toHexString(bmi);
						context.write(new Text(), new Text(String.join(",", fields)));
						
					}
					
				}
				else
					context.write(new Text(), val);
	        }
			
			for(Text val1: cache) {
				String[] fields = val1.toString().split(",");
					fields[9] = Double.toString(bmi);
				
				context.write(new Text(), new Text(String.join(",", fields)));
			}
    }
}
