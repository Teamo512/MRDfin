package mrDfin;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<IntWritable, ValueWritable, IntWritable, IntWritable>{

	private int minSupport;
	
	public PPCTreeNode ppcRoot;
	public NodeListTreeNode nlRoot;
	public PPCTreeNode[] headTable;
	public int[] headTableLen;
	public int[] itemsetCount;
	public int[] sameItems;
	public int PPCNodeCount;
	public int nlNodeCount;
	public int numOfFItem;
	
	public int[] nlistBegin;
	public int nlistCol;
	public int[] nlistLen;
	public int firstNlistBegin;
	public int[] SupportDict;
	
	public int[][] bf;
	public int bf_cursor;
	public int bf_size;
	public int bf_col;
	public int bf_currentSize;
	
	int outputCount = 0;
	
	// public FILE out;
	public int[] result; // the current itemset
	public int resultLen = 0; // the size of the current itemset
	public int resultCount = 0;
	public int nlLenSum = 0; // node list length of the current itemset
	
	protected void setup(Context context) throws IOException, InterruptedException{
		super.setup(context);		
		minSupport = context.getConfiguration().getInt("minSup", 0);
		ppcRoot = new PPCTreeNode();
		nlRoot = new NodeListTreeNode();
		nlNodeCount = 0;
	}
	
	public void reduce(IntWritable key, Iterable<ValueWritable> values, Context context){
		int[] transaction = null;
		ppcRoot.label = -1;
		PPCNodeCount = 0;
		
		bf_size = 1000000;
		bf = new int[100000][];
		bf_currentSize = bf_size * 10;
		bf[0] = new int[bf_currentSize];

		bf_cursor = 0;
		bf_col = 0;
		
		for(ValueWritable value : values){
			transaction = value.itemset;
			buildTree(transaction);
		}
		ppcRoot = null;
		
		PPCTreeNode root = ppcRoot.firstChild;
		int pre = 0;
		itemsetCount = new int[(numOfFItem - 1) * numOfFItem / 2];
		nlistBegin = new int[(numOfFItem - 1) * numOfFItem / 2];
		nlistLen = new int[(numOfFItem - 1) * numOfFItem / 2];
		SupportDict = new int[PPCNodeCount + 1];
		while (root != null) {
			root.foreIndex = pre;
			SupportDict[pre] = root.count;
			pre++;
			PPCTreeNode temp = root.father;
			while (temp.label != -1) {
				itemsetCount[root.label * (root.label - 1) / 2 + temp.label] += root.count;
				nlistLen[root.label * (root.label - 1) / 2 + temp.label]++;
				temp = temp.father;
			}
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
		
		int sum = 0;
		for (int i = 0; i < (numOfFItem - 1) * numOfFItem / 2; i++) {
			if (itemsetCount[i] >= minSupport) {
				nlistBegin[i] = sum;
				sum += nlistLen[i];
			}
		}
		if (bf_cursor + sum > bf_currentSize * 0.85) {
			bf_col++;
			bf_cursor = 0;
			bf_currentSize = sum + 1000;
			bf[bf_col] = new int[bf_currentSize];
		}
		nlistCol = bf_col;
		firstNlistBegin = bf_cursor;
		root = ppcRoot.firstChild;
		bf_cursor += sum;
		while (root != null) {
			PPCTreeNode temp = root.father;
			while (temp.label != -1) {
				if (itemsetCount[root.label * (root.label - 1) / 2 + temp.label] >= minSupport) {
					int cursor = nlistBegin[root.label * (root.label - 1) / 2
							+ temp.label]
							+ firstNlistBegin;
					bf[nlistCol][cursor] = root.foreIndex;
					nlistBegin[root.label * (root.label - 1) / 2 + temp.label] += 1;
				}
				temp = temp.father;
			}
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
		for (int i = 0; i < numOfFItem * (numOfFItem - 1) / 2; i++) {
			if (itemsetCount[i] >= minSupport) {
				nlistBegin[i] = nlistBegin[i] - nlistLen[i];
			}
		}
		
		initializeTree();
		sameItems = new int[numOfFItem];

		int from_cursor = bf_cursor;
		int from_col = bf_col;
		int from_size = bf_currentSize;

		// Recursively traverse the tree
		NodeListTreeNode curNode = nlRoot.firstChild;
		NodeListTreeNode next = null;
		while (curNode != null) {
			next = curNode.next;
			// call the recursive "traverse" method
			//sameCount = 0;
			try {
				traverse(curNode, nlRoot, 1, 0);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (int c = bf_col; c > from_col; c--) {
				bf[c] = null;
			}
			bf_col = from_col;
			bf_cursor = from_cursor;
			bf_currentSize = from_size;
			curNode = next;
		}
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
		}
		
		
	}

	public void initializeTree() {
		NodeListTreeNode lastChild = null;
		for (int t = numOfFItem - 1; t >= 0; t--) {
			NodeListTreeNode nlNode = new NodeListTreeNode();
			nlNode.label = t;
			nlNode.support = 0;
			nlNode.NLStartinBf = bf_cursor;
			nlNode.NLLength = 0;
			nlNode.NLCol = bf_col;
			nlNode.firstChild = null;
			nlNode.next = null;
			//nlNode.support = item[t].num;
			if (nlRoot.firstChild == null) {
				nlRoot.firstChild = nlNode;
				lastChild = nlNode;
			} else {
				lastChild.next = nlNode;
				lastChild = nlNode;
			}
		}
	}
	
	public void traverse(NodeListTreeNode curNode, NodeListTreeNode curRoot, int level, int sameCount) throws IOException {

		NodeListTreeNode sibling = curNode.next;
		NodeListTreeNode lastChild = null;
		while (sibling != null) {
			if ((level == 1 && itemsetCount[(curNode.label - 1) * curNode.label / 2 + sibling.label] >= minSupport)) {
				IntegerByRef sameCountTemp = new IntegerByRef();
				sameCountTemp.count = sameCount;
				lastChild = gen2ItemSet(curNode, sibling, lastChild, sameCountTemp);
				//lastChild = gen2ItemSet(curNode, sibling, lastChild);
				sameCount = sameCountTemp.count;
			}else if(level == 2){
				IntegerByRef sameCountTemp = new IntegerByRef();
				sameCountTemp.count = sameCount;
				lastChild = gen3ItemSet(curNode, sibling, lastChild, sameCountTemp);
				//lastChild = gen3ItemSet(curNode, sibling, lastChild);
				sameCount = sameCountTemp.count;
			} else if (level > 2) {
				IntegerByRef sameCountTemp = new IntegerByRef();
				sameCountTemp.count = sameCount;
				lastChild = getKItemset(curNode, sibling, lastChild, sameCountTemp);
				//lastChild = getKItemset(curNode, sibling, lastChild);
				sameCount = sameCountTemp.count;
			}
			sibling = sibling.next;
		}
		resultCount += Math.pow(2.0, sameCount);
		nlLenSum += Math.pow(2.0, sameCount) * curNode.NLLength;

		result[resultLen++] = curNode.label;

		// ============= Write itemset(s) to file ===========
		//writeItemsetsToFile(curNode, sameCount);

		// ======== end of write to file

		nlNodeCount++;

		int from_cursor = bf_cursor;
		int from_col = bf_col;
		int from_size = bf_currentSize;
		NodeListTreeNode child = curNode.firstChild;
		NodeListTreeNode next = null;
		while (child != null) {
			next = child.next;
			traverse(child, curNode, level + 1, sameCount);
			for (int c = bf_col; c > from_col; c--) {
				bf[c] = null;
			}
			bf_col = from_col;
			bf_cursor = from_cursor;
			bf_currentSize = from_size;
			child = next;
		}
		resultLen--;
	}
	
	NodeListTreeNode gen2ItemSet(NodeListTreeNode ni, NodeListTreeNode nj,
			NodeListTreeNode lastChild, IntegerByRef sameCount) {
		int i = ni.label;
		int j = nj.label;
		if (ni.support == itemsetCount[(i - 1) * i / 2 + j]) {
			sameItems[sameCount.count++] = nj.label;
			//sameItems[sameCount++] = nj.label;
		} else {
			NodeListTreeNode nlNode = new NodeListTreeNode();
			nlNode.label = j;
			nlNode.NLCol = nlistCol;
			nlNode.NLStartinBf = nlistBegin[(i - 1) * i / 2 + j];
			nlNode.NLLength = nlistLen[(i - 1) * i / 2 + j];
			nlNode.support = itemsetCount[(i - 1) * i / 2 + j];
			nlNode.firstChild = null;
			nlNode.next = null;
			if (ni.firstChild == null) {
				ni.firstChild = nlNode;
				lastChild = nlNode;
			} else {
				lastChild.next = nlNode;
				lastChild = nlNode;
			}
		}
		return lastChild;
	}

	NodeListTreeNode gen3ItemSet(NodeListTreeNode ni, NodeListTreeNode nj,
			NodeListTreeNode lastChild, IntegerByRef sameCount) {
		if(bf_cursor + ni.NLLength > bf_currentSize)
		{
			bf_col++;
			bf_cursor = 0;
			bf_currentSize = bf_size > ni.NLLength * 1000 ? bf_size : ni.NLLength * 1000;
			bf[bf_col] = new int[bf_currentSize];
		}
			
		NodeListTreeNode nlNode = new NodeListTreeNode();
		nlNode.support = 0;
		nlNode.NLStartinBf = bf_cursor;
		nlNode.NLCol = bf_col;
		nlNode.NLLength = 0;
		
		int cursor_i = ni.NLStartinBf;
		int cursor_j = nj.NLStartinBf;
		int col_i = ni.NLCol;
		int col_j = nj.NLCol;
		while(cursor_i < ni.NLStartinBf + ni.NLLength && cursor_j < nj.NLStartinBf + nj.NLLength)
		{
			if(bf[col_i][cursor_i] == bf[col_j][cursor_j])
			{
				nlNode.support += SupportDict[bf[col_i][cursor_i]];
				cursor_i += 1;
				cursor_j += 1;
			}
			else if(bf[col_i][cursor_i] < bf[col_j][cursor_j])
			{
				bf[bf_col][bf_cursor++] =  bf[col_i][cursor_i];
				nlNode.NLLength++;
				cursor_i += 1;
			}
			else
			{
				cursor_j += 1;
			}
		}
		while(cursor_i < ni.NLStartinBf + ni.NLLength)
		{
			bf[bf_col][bf_cursor++] =  bf[col_i][cursor_i];
			nlNode.NLLength++;
			cursor_i += 1;
		}
		if(nlNode.support >= minSupport)
		{
			if(ni.support == nlNode.support)
				sameItems[sameCount.count++] = nj.label;
				//sameItems[sameCount++] = nj.label;
			else
			{
				nlNode.label = nj.label;
				nlNode.firstChild = null;
				nlNode.next = null;
				if(ni.firstChild == null)
				{
					ni.firstChild = nlNode;
					lastChild = nlNode;
				}
				else
				{
					lastChild.next = nlNode;
					lastChild = nlNode;
				}
			}
			return lastChild;
		}
		else
		{
			bf_cursor = nlNode.NLStartinBf;
			//delete nlNode;
		}
		return lastChild;
	}
	
	NodeListTreeNode getKItemset(NodeListTreeNode ni, NodeListTreeNode nj,
			NodeListTreeNode lastChild, IntegerByRef sameCountRef) {

		if (bf_cursor + ni.NLLength > bf_currentSize) {
			bf_col++;
			bf_cursor = 0;
			bf_currentSize = bf_size > ni.NLLength * 1000 ? bf_size
					: ni.NLLength * 1000;
			bf[bf_col] = new int[bf_currentSize];
		}

		NodeListTreeNode nlNode = new NodeListTreeNode();
		nlNode.support = ni.support;
		nlNode.NLStartinBf = bf_cursor;
		nlNode.NLCol = bf_col;
		nlNode.NLLength = 0;

		int cursor_i = ni.NLStartinBf;
		int cursor_j = nj.NLStartinBf;
		int col_i = ni.NLCol;
		int col_j = nj.NLCol;

		while (cursor_i < ni.NLStartinBf + ni.NLLength
				&& cursor_j < nj.NLStartinBf + nj.NLLength) {
			if (bf[col_i][cursor_i] == bf[col_j][cursor_j]) {
				cursor_i += 1;
				cursor_j += 1;
			} else if (bf[col_i][cursor_i] < bf[col_j][cursor_j]) {
				cursor_i += 1;
			} else {
				bf[bf_col][bf_cursor++] = bf[col_j][cursor_j];
				nlNode.NLLength++;
				nlNode.support -= SupportDict[bf[col_j][cursor_j]];
				cursor_j += 1;
			}
		}
		while(cursor_j < nj.NLStartinBf + nj.NLLength){
			bf[bf_col][bf_cursor++] = bf[col_j][cursor_j];
			nlNode.NLLength++;
			nlNode.support -= SupportDict[bf[col_j][cursor_j]];
			cursor_j += 1;
		}
		if (nlNode.support >= minSupport) {
			if (ni.support == nlNode.support) {
				sameItems[sameCountRef.count++] = nj.label;
				//sameItems[count++] = nj.label;
				//sameItems[sameCount++] = nj.label;
			} else {
				nlNode.label = nj.label;
				nlNode.firstChild = null;
				nlNode.next = null;
				if (ni.firstChild == null) {
					ni.firstChild = nlNode;
					lastChild = nlNode;
				} else {
					lastChild.next = nlNode;
					lastChild = nlNode;
				}
			}
			return lastChild;
		} else {
			bf_cursor = nlNode.NLStartinBf;
		}
		return lastChild;
	}
	
	/*private void writeItemsetsToFile(NodeListTreeNode curNode, int sameCount) throws IOException {

		// create a stringuffer
		StringBuilder buffer = new StringBuilder();
		if(curNode.support >= minSupport) {
			outputCount++;
			// append items from the itemset to the StringBuilder
			for (int i = 0; i < resultLen; i++) {
				buffer.append(item[result[i]]);
				buffer.append(' ');
			}
			// append the support of the itemset
			buffer.append("#SUP: ");
			buffer.append(curNode.support);
			buffer.append("\n");
		}

		// === Write all combination that can be made using the node list of
		// this itemset
		if (sameCount > 0) {
			// generate all subsets of the node list except the empty set
			for (long i = 1, max = 1 << sameCount; i < max; i++) {
				for (int k = 0; k < resultLen; k++) {
					buffer.append(item[result[k]].index);
					buffer.append(' ');
				}

				// we create a new subset
				for (int j = 0; j < sameCount; j++) {
					// check if the j bit is set to 1
					int isSet = (int) i & (1 << j);
					if (isSet > 0) {
						// if yes, add it to the set
						buffer.append(item[sameItems[j]].index);
						buffer.append(' ');
						// newSet.add(item[sameItems[j]].index);
					}
				}
				buffer.append("#SUP: ");
				buffer.append(curNode.support);
				buffer.append("\n");
				outputCount++;
			}
		}
		// write the strinbuffer to file and create a new line
		// so that we are ready for writing the next itemset.
		writer.write(buffer.toString());
	}*/
	
	class IntegerByRef {
		int count;
	}
}
