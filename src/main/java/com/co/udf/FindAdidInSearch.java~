package com.ebay.ecg.bolt.bigdata.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class FindAdidInSearch extends EvalFunc<String> {

	public String exec(Tuple input) throws IOException {
		
		String returnval = "false";
		
		if (input == null || input.size() == 0)
			return null;

		String str = (String) input.get(0);  // this is the adid
		
		String searchStr = (String) input.get(1);
		
		// Get the adids from the search results and see if the adid 
		// matches one of them.
		String[] searchResults = searchStr.split("\\|");
		boolean firstOne = true;
		for (String oneSearchStr : searchResults) {
			if (firstOne) { // skip the first one because it's the search string
				firstOne = false;
				continue;
			}
			if (str.contains(oneSearchStr)) returnval = str+"/"+oneSearchStr;
		}
		return returnval;
	}
	
	public static void main(String[] args) {
		String returnval = "NONE";
		String searchStr = "EMPTY|10010|10012";
		String[] searchResults = searchStr.split("\\|");
		System.out.println("0 - "+searchResults[0] + "1 - "+searchResults[1]);
		String str = "hello";
		for (String oneSearchStr : searchResults) {
			System.out.println(oneSearchStr);
			if (str.contains(oneSearchStr)) returnval = str+"/"+oneSearchStr;
		}
		System.out.println("returnval = "+returnval);
//		System.out.println("100hellothere".contains("100nothere"));
	}
}
