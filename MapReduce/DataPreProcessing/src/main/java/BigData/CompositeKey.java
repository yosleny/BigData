package BigData;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey> {

	private Text key1 = new Text();
    private IntWritable key2 = new IntWritable(0);
    
    public CompositeKey(Text key1, int key2) {
        this.key1.set(key1);
        this.key2.set(key2);
    }
    
    public CompositeKey() {}
    
	public void write(DataOutput out) throws IOException {
		this.key1.write(out);
        this.key2.write(out);
		
	}

	public void readFields(DataInput in) throws IOException {
		this.key1.readFields(in);
        this.key2 .readFields(in);
		
	}

	public int compareTo(CompositeKey o) {
		if (o == null)
            return 0;
        
        int intcnt = key1.compareTo(o.key1);
        return intcnt == 0 ? key2.compareTo(o.key2) : intcnt;
	}
	
	@Override
    public String toString() {
        return key1.toString() + ":" + key2.toString();
    }
	
	public Text getKey1() { return this.key1; }
	
	public IntWritable getKey2() { return this.key2; }
	
}
