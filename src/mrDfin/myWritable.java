package mrDfin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class myWritable implements Writable{
	
	private ArrayList<Integer> array;
	
	public myWritable(){
		this.array = new ArrayList<Integer>();
	}

	public myWritable(ArrayList<Integer> array){
		this.array = array;
	}
	
	public ArrayList<Integer> getArray(){
		return this.array;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		int size = array.size();
		out.writeInt(size);
		for(Integer item : array)
			out.writeLong(item);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		array = new ArrayList<Integer>();
		for(int i = 0; i < size; i++)
			array.add(in.readInt());
		
	}
	
	public String toString(){
		StringBuilder str = new StringBuilder();
		int size = this.array.size();
		for(int i=0; i<size; i++){
			str.append("" + this.array.get(i));
			if(i != size-1){
				str.append(",");
			}
		}
		return str.toString();
	}

}
