package mrDfin;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondReducer extends Reducer<IntWritable, ValueWritable, Text, NullWritable>{

	private int minSupport;
	
	public PPCTreeNode ppcRoot;
	public NodeListTreeNode nlRoot;
	public int[] itemsetCount;
	public int[] sameItems;
	public int PPCNodeCount;
	public int numOfFItem;
	public int[] itemSup;
	
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
	
	public int sameCount;
	public boolean useFileCache;
	
	Set<Integer> itemOfGroup = null;
	
	// public FILE out;
	public int[] result; // the current itemset
	public int resultLen = 0; // the size of the current itemset
	
	protected void setup(Context context) throws IOException, InterruptedException{
		super.setup(context);		
		minSupport = context.getConfiguration().getInt("minSup", 0);	
		nlRoot = new NodeListTreeNode();	
		useFileCache = context.getConfiguration().getBoolean("Cache", true);
	}
	
	public void reduce(IntWritable key, Iterable<ValueWritable> values, Context context){
		int[] transaction = null;
		ppcRoot = new PPCTreeNode();		
		ppcRoot.label = -1;
		PPCNodeCount = 0;

		for(ValueWritable value : values){
			transaction = value.itemset;
			buildTree(transaction);
		}
		transaction = null;
		numOfFItem += 1;//因为计数从0开始，所以算个数时需要加1
		itemSup = new int[numOfFItem];
		
		resultLen = 0;
		result = new int[numOfFItem];
		
		bf_size = 1000000;
		bf = new int[100000][];
		bf_currentSize = bf_size * 10;
		//bf[0] = new int[bf_currentSize];

		bf_cursor = 0;
		bf_col = 0;
		
		nlRoot = new NodeListTreeNode();
		traveseGetNodeSet();
		
		ppcRoot = null;
		
		initializeTree();
		itemSup = null;
		sameItems = new int[numOfFItem];

		int from_cursor = bf_cursor;
		int from_col = bf_col;
		int from_size = bf_currentSize;

		if(useFileCache)
			itemOfGroup = getItemOfGroup(key.get(), context);
		else
			itemOfGroup = getItemOfGroup_bak(key.get(), context);
		// Recursively traverse the tree
		NodeListTreeNode curNode = nlRoot.firstChild;
		NodeListTreeNode next = null;
		while (curNode != null) {
			next = curNode.next;
			// call the recursive "traverse" method
			try {
				sameCount = 0;
				traverse(curNode, nlRoot, 1, context);
			} catch (Exception e) {
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
		SupportDict = null;
		sameItems = null;
	}
	
	//Build the tree
	public void buildTree(int[] transaction){
		
		int curPos = 0;
		PPCTreeNode curRoot = ppcRoot;
		PPCTreeNode rightSibling = null;
		int tLen = transaction.length;
		if(numOfFItem < transaction[tLen-1])
			numOfFItem = transaction[tLen-1];//统计其中最大的元素来作为元素的个数，因为所有元素都是按找递增顺序重新赋值的。
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
			PPCNodeCount++;
		}
		
		
	}

	//Travese the tree and get 2nd frequnet itemset NodeSet
	public void traveseGetNodeSet() {
		PPCTreeNode root = ppcRoot.firstChild;
		int pre = 0;
		itemsetCount = new int[(numOfFItem - 1) * numOfFItem / 2];
		nlistBegin = new int[(numOfFItem - 1) * numOfFItem / 2];
		nlistLen = new int[(numOfFItem - 1) * numOfFItem / 2];
		SupportDict = new int[PPCNodeCount + 1];
		while (root != null) {
			root.foreIndex = pre;
			SupportDict[pre] = root.count;
			itemSup[root.label] += root.count; 
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
			//bf[bf_col] = new int[bf_currentSize];
		}
		bf[bf_col] = new int[bf_currentSize];
		
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
	}
	
	//初始化枚举树，这是整个挖掘过程的搜索路径
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
			nlNode.support = itemSup[t];
			if (nlRoot.firstChild == null) {
				nlRoot.firstChild = nlNode;
				lastChild = nlNode;
			} else {
				lastChild.next = nlNode;
				lastChild = nlNode;
			}
		}
		
	}
	
	public void traverse(NodeListTreeNode curNode, NodeListTreeNode curRoot, int level, Context context) throws IOException, InterruptedException {

		if(level == 1 && !itemOfGroup.contains(curNode.label))
			return;
		
		NodeListTreeNode sibling = curNode.next;
		NodeListTreeNode lastChild = null;
		while (sibling != null) {
			if ((level == 1 && itemsetCount[(curNode.label - 1) * curNode.label / 2 + sibling.label] >= minSupport)) {
				lastChild = gen2ItemSet(curNode, sibling, lastChild);
			}else if(level == 2){
				lastChild = gen3ItemSet(curNode, sibling, lastChild);
			} else if (level > 2) {
				lastChild = getKItemset(curNode, sibling, lastChild);
			}
			sibling = sibling.next;
		}
		//resultCount += Math.pow(2.0, sameCount);
		//nlLenSum += Math.pow(2.0, sameCount) * curNode.NLLength;

		result[resultLen++] = curNode.label;

		// ============= Write itemset(s) to file ===========
		//writeItemsetsToFile(curNode, sameCount);
		writeResult(curNode, sameCount, context, level);
		// ======== end of write to file

		//nlNodeCount++;

		int from_cursor = bf_cursor;
		int from_col = bf_col;
		int from_size = bf_currentSize;
		NodeListTreeNode child = curNode.firstChild;
		NodeListTreeNode next = null;
		int tmp = sameCount;
		while (child != null) {
			next = child.next;
			sameCount = tmp;
			traverse(child, curNode, level + 1, context);
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
	
	public NodeListTreeNode gen2ItemSet(NodeListTreeNode ni, NodeListTreeNode nj, NodeListTreeNode lastChild) {
		int i = ni.label;
		int j = nj.label;
		if (ni.support == itemsetCount[(i - 1) * i / 2 + j]) {
			sameItems[sameCount++] = nj.label;
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

	public NodeListTreeNode gen3ItemSet(NodeListTreeNode ni, NodeListTreeNode nj, NodeListTreeNode lastChild) {
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
				sameItems[sameCount++] = nj.label;
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
		else{
			bf_cursor = nlNode.NLStartinBf;
			nlNode = null;
		}
		return lastChild;
	}
	
	public NodeListTreeNode getKItemset(NodeListTreeNode ni, NodeListTreeNode nj, NodeListTreeNode lastChild) {

		if (bf_cursor + nj.NLLength > bf_currentSize) {
			bf_col++;
			bf_cursor = 0;
			bf_currentSize = bf_size > ni.NLLength * 1000 ? bf_size : ni.NLLength * 1000;
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
				sameItems[sameCount++] = nj.label;
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
			nlNode = null;
		}
		return lastChild;
	}
	
	public void writeResult(NodeListTreeNode curNode, int sameCount, Context context, int level) throws IOException, InterruptedException {
		//StringBuilder sb = new StringBuilder();
		if (curNode.support >= minSupport && level > 1) {
			context.getCounter(MRDfinCounter.TatolFrequentNum).increment(1);
			
			/*for (int i = 0; i < resultLen; i++) {
				sb.append(result[i] + ' ');
			}
			// append the support of the itemset
			sb.append("#SUP: " + curNode.support + "\n");*/
		}
		
		if(sameCount > 0) {
			for (long i = 1, max = 1 << sameCount; i < max; i++) {
				
				context.getCounter(MRDfinCounter.TatolFrequentNum).increment(1);
				
				/*for (int k = 0; k < resultLen; k++) {
					sb.append(result[k] + ' ');
				}

				// we create a new subset
				for (int j = 0; j < sameCount; j++) {
					// check if the j bit is set to 1
					int isSet = (int) i & (1 << j);
					if (isSet > 0) {
						// if yes, add it to the set
						sb.append(sameItems[j] + ' ');
					}
				}
				sb.append("#SUP: " + curNode.support + "\n");*/				
			}
		}
		/*if(sb.length() > 0)
			context.write(new Text(sb.toString()), NullWritable.get());*/
		
	}
	
	/*private void writeItemsetsToFile(NodeListTreeNode curNode, int sameCount) throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter("result"));
		// create a stringuffer
		StringBuilder buffer = new StringBuilder();
		if(curNode.support >= minSupport) {
			outputCount++;
			// append items from the itemset to the StringBuilder
			for (int i = 0; i < resultLen; i++) {
				buffer.append(result[i]);
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
					//buffer.append(item[result[k]].index);
					buffer.append(result[k]);
					buffer.append(' ');
				}

				// we create a new subset
				for (int j = 0; j < sameCount; j++) {
					// check if the j bit is set to 1
					int isSet = (int) i & (1 << j);
					if (isSet > 0) {
						// if yes, add it to the set
						buffer.append(sameItems[j]);
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
	
	public HashSet<Integer> getItemOfGroup_bak(int groupNum, Context context) {
		HashSet<Integer> itemOfGroup = new HashSet<Integer>();
		String[] str = context.getConfiguration().get("ItemGroup").split(";")[groupNum].split(":")[1].split(" ");
		for(String s : str)
			itemOfGroup.add(Integer.parseInt(s));
		return itemOfGroup;
		
	}
	
	public HashSet<Integer> getItemOfGroup(int groupNum, Context context) {
		HashSet<Integer> itemOfGroup = new HashSet<Integer>();
		SequenceFile.Reader reader = null;
		try {
			URI[] paths = context.getCacheFiles();
			if(paths == null || paths.length <=0){
				System.out.println("No DistributedCache keywords File!");
				System.exit(1);
			}

			IntWritable key = new IntWritable();
			IntWritable value = new IntWritable();
			for(URI path : paths){	
				if(path.getPath().contains("groupNum")) {
					reader = new SequenceFile.Reader(context.getConfiguration(), Reader.file(new Path(path)));
					while (reader.next(key, value)) {
						if(value.get() == groupNum) {
							itemOfGroup.add(key.get());
						}
					}
				}
			}
		}catch(Exception e) {
			e.printStackTrace();
		}finally {
			IOUtils.closeStream(reader);
		}
		return itemOfGroup;
	}
}
