package mrDfin;

import java.util.ArrayList;
import java.util.List;

public class Tools {
	public static int minSupport = MRDfin_Driver.minSup;
	protected static ArrayList<Integer> intersection(ArrayList<Integer> list1,ArrayList<Integer> list2) {
		ArrayList<Integer> common = new ArrayList<Integer>(list1.size());
		int i, j;
		for (i = 0, j = 0; i < list1.size() && j < list2.size();) {
			if (list1.get(i).intValue() == list2.get(j).intValue()) {
				common.add(list1.get(i));
				i++;
				j++;
			} else if (list1.get(i) < list2.get(j)) {
				i++;
			} else {
				j++;
			}
		}
		return common;
	}

	
	protected static ArrayList<Integer> intersection_bak(int[] array1, int[] array2) {
		ArrayList<Integer> list = new ArrayList<Integer>(array1.length);
		int l1 = array1.length - minSupport;
		int l2 = array2.length - minSupport;
		int i=0, j=0;
		while(i<array1.length && j<array2.length && l1>=0 && l2>=0){
			if(array1[i] == array2[j]){
				list.add(array1[i]);
				i++;
				j++;
			}else if(array1[i] < array2[j])
				i++;
			else
				j++;
		}
		return list;
	}
	
	protected static int[] intersection(int[] array1, int[] array2){
		List<Integer> list = new ArrayList<Integer>(array1.length);
		int i=0,j=0;
		while(i<array1.length && j<array2.length){
			if(array1[i] == array2[j]){
				list.add(array1[i]);
				i++;
				j++;
			}else if(array1[i] < array2[j])
				i++;
			else
				j++;
		}
		return toIntArray(list);		
	}
	
	/*protected static ArrayList<Integer> difference(ArrayList<Integer> list1, ArrayList<Integer> list2) {
		ArrayList<Integer> diff = new ArrayList<Integer>(list1.size());
		int i, j;
		for (i = 0, j = 0; i < list1.size() && j < list2.size();) {
			if (list1.get(i).intValue() == list2.get(j).intValue()) {
				i++;
				j++; 
			} else if (list1.get(i) < list2.get(j)) {
				diff.add(list1.get(i));
				i++;
			} else {
				j++;
			}
		}

		for (; i < list1.size(); i++) {
			diff.add(list1.get(i));
		}
		return diff;
	}*/

	protected static int[] difference(int[] array1, int[] array2){
		List<Integer> list = new ArrayList<Integer>(array1.length);
		int i=0,j=0;
		while(i<array1.length && j<array2.length){
			if(array1[i] == array2[j]){
				i++;
				j++;
			}else if(array1[i] < array2[j]){
				list.add(array1[i]);
				i++;
			}else
				j++;
		}
		for(;i<array1.length; i++)
			list.add(array1[i]);
		return toIntArray(list);
	}
	
	
	/*protected static ArrayList<Integer> combineList(ArrayList<Integer> list1,ArrayList<Integer> list2) {
		ArrayList<Integer> combine = new ArrayList<Integer>(list1.size()+list2.size());
		int i, j;
		for (i = 0, j = 0; i < list1.size() && j < list2.size();) {
			int compare = list1.get(i) - list2.get(j);
			if (compare < 0) {
				combine.add(list1.get(i));
				i++;
			} else if (compare == 0) {
				combine.add(list1.get(i));
				i++;
				j++;
			} else {
				combine.add(list2.get(j));
				j++;
			}
		}
		while(i<list1.size()){
			combine.add(list1.get(i++));
		}
		while(j<list2.size()){
			combine.add(list2.get(j++));
		}
		return combine;
	}*/

	protected static int[] combineList(int[] array1, int[] array2){
		List<Integer> list = new ArrayList<Integer>(array1.length+array2.length);
		int i=0, j=0;
		while(i<array1.length && j<array2.length){
			int compare = array1[i] - array2[j];
			if(compare < 0){
				list.add(array1[i]);
				i++;
			}else if(compare == 0){
				list.add(array1[i]);
				i++;
				j++;
			}else{
				list.add(array2[j]);
				j++;
			}
		}
		while(i < array1.length)
			list.add(array1[i++]);
		while(j < array2.length)
			list.add(array2[j++]);
		return toIntArray(list);
	}
	
	/*protected static int subsetOfCandidate(ArrayList<Integer> itemset, ArrayList<Integer> candidate) {
		int candidateSize = candidate.size();
		for (int i = 0; i < candidateSize - 2; i++) {
			if (itemset.get(i).intValue() != candidate.get(i).intValue())
				return 0;
		}
		if (itemset.get(candidateSize - 2).intValue() == candidate.get(candidateSize - 2).intValue()) {
			return 1;
		} else if (itemset.get(candidateSize - 2).intValue() == candidate.get(candidateSize - 1).intValue()) {
			return 2;
		} else {
			return 0;
		}
	}*/
	
	protected static int subsetOfCandidate(int[] itemset, int[] candidate) {
		int candidateSize = candidate.length;
		for (int i = 0; i < candidateSize - 2; i++) {
			if (itemset[i] != candidate[i])
				return 0;
		}
		if (itemset[candidateSize-2] == candidate[candidateSize-2]) {
			return 1;
		} else if (itemset[candidateSize-2] == candidate[candidateSize-1]) {
			return 2;
		} else {
			return 0;
		}
	}
	
	/*public static boolean contains(ArrayList<Integer> candidate, ArrayList<Integer> fitemset) {
		int i, j;
		int newSize = candidate.size();
		for (i = 0, j = 0; i < newSize && j < newSize - 1;) {
			if (candidate.get(i).intValue() == fitemset.get(j).intValue()) {
				i++;
				j++;
			} else if (candidate.get(i) < fitemset.get(j))
				i++;
			else
				break;
		}
		if (j < newSize - 1)
			return false;
		else
			return true;
	}*/
	
	public static boolean contains(int[] candidate, int[] fitemset) {
		int i, j;
		int newSize = candidate.length;
		for (i = 0, j = 0; i < newSize && j < newSize - 1;) {
			if (candidate[i] == fitemset[j]) {
				i++;
				j++;
			} else if (candidate[i] < fitemset[j])
				i++;
			else
				break;
		}
		return j >= newSize - 1;
	}
	
	public static int[] toIntArray(List<Integer> list) {
	    int[] intArray = new int[list.size()];
	    int ix = 0;
	    for (Integer i : list) {
	      intArray[ix++] = i;
	    }
	    return intArray;
	  }

}
