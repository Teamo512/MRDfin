package mrDfin;

import java.util.ArrayList;

public class TreeNode {
	
	public long item;
	public int count;
	public int preOrder;
	public int postOrder;
	ArrayList<TreeNode> childrenList;
	
	public TreeNode(){
		this.item = -1;
		this.count = 0;
		this.preOrder = 0;
		this.postOrder = 0;
		this.childrenList = new ArrayList<TreeNode>();
	}
	
	public TreeNode(long item){
		this.item = item;
		this.count = 1;
		this.preOrder = 0;
		this.postOrder = 0;
		this.childrenList = new ArrayList<TreeNode>();
	}
	
	public boolean equals(Object o){
		if(o instanceof TreeNode){
			TreeNode tmp = (TreeNode)o;
			
			if(this.item == tmp.item)
				return true;
			else
				return false;
		}
		return false;
	}
	
	public int hashCode() {   
		return Long.hashCode(item);
	}

}
