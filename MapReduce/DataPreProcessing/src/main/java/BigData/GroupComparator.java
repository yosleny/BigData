package BigData;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {
	
    public GroupComparator() {
        super(CompositeKey.class, true);
    }
    
    @Override
    public int compare(WritableComparable tp1, WritableComparable tp2) {
    	CompositeKey pair = (CompositeKey) tp1;
    	CompositeKey pair2 = (CompositeKey) tp2;
    	
        return pair.getKey1().compareTo(pair2.getKey1());
    }
}