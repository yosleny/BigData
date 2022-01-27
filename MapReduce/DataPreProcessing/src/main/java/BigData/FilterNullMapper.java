package BigData;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FilterNullMapper extends Mapper<Object, Text, CompositeKey, Text>{

	@Override
	protected void map(Object key, Text value, Mapper<Object, Text, CompositeKey, Text>.Context context)
			throws IOException, InterruptedException {
			
			String[] fields = value.toString().split(",");
			if(fields.length == 12) {
									
				String gender = fields[1];
				fields[1] = gender.equals("Male") ? "1" : "0";
				
				String everMarried = fields[5];
				fields[5] = everMarried.equals("Yes") ? "1" : "0";
				
				String residenceType = fields[7];
				fields[7] = residenceType.equals("Urban") ? "1" : "0";
				
				if(!fields[9].equals("N/A"))
				{
					double bmi = Double.parseDouble(fields[9]);
					if(bmi > 50)
						fields[9] = Double.toString(50);
				}
				
				String smokingStatus = fields[10];
				fields[10] = smokingStatus.equals("smokes") || smokingStatus.equals("formerly smoked") ? "1" : "0";
				
				if(!fields[9].equals("N/A")  && (fields[3].equals("1") || fields[5].equals("1"))) {
			    	
					context.write(new CompositeKey(new Text("1"), 12), new Text(String.join(",", fields)));
			    }
				else if(fields[9].equals("N/A"))
					context.write(new CompositeKey(new Text("r"), 12), new Text(String.join(",", fields)));
				else
					context.write(new CompositeKey(new Text("0"), 12), new Text(String.join(",", fields)));

					
			}
		
				
	}
}
