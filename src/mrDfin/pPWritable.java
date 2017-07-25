package mrDfin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class pPWritable implements Writable{
	private long firstItem;
	private ArrayList<Long> restItems;
	
	public pPWritable(){
		this.firstItem = 0;
		this.restItems = new ArrayList<Long>();
		
	}

	public pPWritable(ArrayList<Long> array){
		this.firstItem = array.get(0);
		this.restItems = new ArrayList<Long>();
		this.restItems.addAll(array.subList(1, array.size()));
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(firstItem);
		int size = restItems.size();
		out.writeInt(size);
		for(Long item : restItems){
			out.writeLong(item);
		}
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		firstItem = in.readLong();
		int size = in.readInt();
		restItems = new ArrayList<Long>();
		for(int i = 0; i < size; i++){
			restItems.add(in.readLong());
		}
		
	}
	
	public String toString(){
		StringBuilder str = new StringBuilder();
		str.append(firstItem + ":");
		int size = this.restItems.size();
		for(int i=0; i<size; i++){
			str.append(this.restItems.get(i));
			if(i != size-1){
				str.append(",");
			}
		}
		return str.toString();
	}

}
