package BigData;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;

public class KeyPartitioner extends Partitioner<CompositeKey, Text>{
  
	@Override
	public int getPartition(CompositeKey key, Text value, int numPartitions) {
		
		return Math.abs(key.getKey1().hashCode() * 127) % numPartitions;
		
	}
}
