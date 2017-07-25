package mrDfin;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<LongWritable, myWritable, LongWritable, LongWritable>{

	public PPCTreeNode ppcRoot;
	public NodeListTreeNode nlRoot;
	public PPCTreeNode[] headTable;
	public int[] headTableLen;
	public int[] itemsetCount;
	public int[] sameItems;
	public int nlNodeCount;
	public int numOfFItem;
	
	protected void setup(Context context) throws IOException, InterruptedException{
		super.setup(context);
		ppcRoot = new PPCTreeNode();
	}
	
	public void reduce(LongWritable key, Iterable<myWritable> values, Context context){
		Item[] transaction;
		ArrayList<Integer> itemset;
		for(myWritable value : values){
			itemset = value.getArray();
			transaction = new Item[itemset.size()];
			int tLen = 0;
			for(int i=0; i<itemset.size();i++){
				transaction[tLen] = new Item();
				transaction[tLen].item = itemset.get(i);
				transaction[tLen].count = 0-i;
				tLen++;
			}
			buildTree(transaction);
		}
		
		headTable = new PPCTreeNode[numOfFItem];

		headTableLen = new int[numOfFItem];

		PPCTreeNode[] tempHead = new PPCTreeNode[numOfFItem];

		itemsetCount = new int[(numOfFItem - 1) * numOfFItem / 2];

		PPCTreeNode root = ppcRoot.firstChild;
		int pre = 0;
		int last = 0;
		while (root != null) {
			root.foreIndex = pre;
			pre++;

			if (headTable[root.label] == null) {
				headTable[root.label] = root;
				tempHead[root.label] = root;
			} else {
				tempHead[root.label].labelSibling = root;
				tempHead[root.label] = root;
			}
			headTableLen[root.label]++;

			PPCTreeNode temp = root.father;
			while (temp.label != -1) {
				itemsetCount[root.label * (root.label - 1) / 2 + temp.label] += root.count;
				temp = temp.father;
			}
			if (root.firstChild != null) {
				root = root.firstChild;
			} else {
				// back visit
				root.backIndex = last;
				last++;
				if (root.rightSibling != null) {
					root = root.rightSibling;
				} else {
					root = root.father;
					while (root != null) {
						// back visit
						root.backIndex = last;
						last++;
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
	
	public void insert_tree(ArrayList<Long> path, TreeNode root){
		int i = 0;
		TreeNode insertNode = null;
		TreeNode curNode = new TreeNode(path.get(i));
		if(root.childrenList.contains(curNode)){
			insertNode = root.childrenList.get(root.childrenList.indexOf(curNode));
			insertNode.count++;
		}else{
			root.childrenList.add(curNode);
			insertNode = curNode;
		}
		path.remove(i);
		if(! path.isEmpty()){
			insert_tree(path, insertNode);
		}
	}

	public void buildTree(Item[] transaction){
		ppcRoot.label = -1;
		int curPos = 0;
		PPCTreeNode curRoot = (ppcRoot);
		PPCTreeNode rightSibling = null;
		int tLen = transaction.length;
		while (curPos != tLen) {
			PPCTreeNode child = curRoot.firstChild;
			while (child != null) {
				if (child.label == 0 - transaction[curPos].count) {
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
			ppcNode.label = 0 - transaction[j].count;
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
		}
		
		
	}

}
