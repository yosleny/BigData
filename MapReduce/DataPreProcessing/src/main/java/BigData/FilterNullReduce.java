package BigData;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterNullReduce extends Reducer<CompositeKey, Text, Text, Text> {

	@Override
	 protected void reduce(CompositeKey key, Iterable<Text> values, Context context)
	            throws IOException, InterruptedException
	        {
		
			double bmi = 0;
			for (Text val : values) {
				String[] fields = val.toString().split(",");
				
				if(key.toString().equals("r:1") && fields.length == 1)
				{
					bmi = Double.parseDouble(fields[0]);
				}
				else if(key.toString().equals("r:12"))
				{
						fields[9] = Double.toString(bmi);
						context.write(new Text(), new Text(String.join(",", fields)));
				}
				else
					context.write(new Text(),  val);
	        }
    }
}
