package mrDfin;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducerbak1 extends Reducer<IntWritable, ValueWritable, IntWritable, IntWritable>{
	private int minSupport;
	public PPCTreeNode ppcRoot;
	
	public int numOfFItem;
	public int[] itemSup;
	
	protected void setup(Context context) throws IOException, InterruptedException{
		super.setup(context);		
		minSupport = context.getConfiguration().getInt("minSup", 0);
		ppcRoot = new PPCTreeNode();
		
	}
	
	public void reduce(IntWritable key, Iterable<ValueWritable> values, Context context){
		int[] transaction = null;
		ppcRoot.label = -1;
		for(ValueWritable value : values) {
			transaction = value.itemset;
			buildTree(transaction);
		}
		itemSup = new int[numOfFItem];
		travelTree(ppcRoot);
		
	}
	
	public void buildTree(int[] transaction){
		
		int curPos = 0;
		PPCTreeNode curRoot = (ppcRoot);
		PPCTreeNode rightSibling = null;
		int tLen = transaction.length;
		while (curPos != tLen) {
			PPCTreeNode child = curRoot.firstChild;
			while (child != null) {
				if (child.label == transaction[curPos]) {
					curPos++;
					child.count++;
					curRoot = child;
					break;
				}
				if (child.rightSibling == null) {
					rightSibling = child;
					child = null;
					break;
				}
				child = child.rightSibling;
			}
			if (child == null)
				break;
		}
		for (int j = curPos; j < tLen; j++) {
			PPCTreeNode ppcNode = new PPCTreeNode();
			ppcNode.label = transaction[j];
			if (rightSibling != null) {
				rightSibling.rightSibling = ppcNode;
				rightSibling = null;
			} else {
				curRoot.firstChild = ppcNode;
			}
			ppcNode.rightSibling = null;
			ppcNode.firstChild = null;
			ppcNode.father = curRoot;
			ppcNode.labelSibling = null;
			ppcNode.count = 1;
			curRoot = ppcNode;
			numOfFItem++;
		}
		
		
	}

	public void travelTree(PPCTreeNode ppcRoot) {
		PPCTreeNode root = ppcRoot.firstChild;
		int pre = 1;
		while(root != null) {
			root.foreIndex = pre++;
			itemSup[root.label] += root.count; 
			//pre++;
			PPCTreeNode temp = root.father;
			/*while (temp.label != -1) {
				temp = temp.father;
			}*/
			if (root.firstChild != null) {
				root = root.firstChild;
			} else {
				if (root.rightSibling != null) {
					root = root.rightSibling;
				} else {
					root = root.father;
					while (root != null) {
						if (root.rightSibling != null) {
							root = root.rightSibling;
							break;
						}
						root = root.father;
					}
				}
			}
		}
		root = ppcRoot.firstChild;
		int back = 1;
		while (root != null) {
			if (root.firstChild != null) {
				root = root.firstChild;
			} else {
				root.backIndex = back++;
				if (root.rightSibling != null) {
					root = root.rightSibling;
					//root.backIndex = back++;
				} else {
					root = root.father;
					while (root != null) {
						root.backIndex = back++;
						if (root.rightSibling != null) {
							root = root.rightSibling;
							break;
						}
						root = root.father;
					}
				}
			}
		}
	}

}
