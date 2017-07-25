package mrDfin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class KeyWritable implements WritableComparable<KeyWritable>{

	public int item;
	public int groupNum;
	
	public KeyWritable() {
		
	}
	
	public KeyWritable(int item, int groupNum) {
		this.item = item;
		this.groupNum = groupNum;
	}
	
	public KeyWritable(KeyWritable kw) {
		this.item = kw.item;
		this.groupNum = kw.groupNum;
	}
	
	@Override
	public int hashCode() {
		return item;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KeyWritable other = (KeyWritable) obj;
		if (item != other.item)
			return false;
		return true;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(item);
		out.writeInt(groupNum);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		item = in.readInt();
		groupNum = in.readInt();
		
	}

	@Override
	public int compareTo(KeyWritable o) {
		return this.item - o.item;
	}

}
