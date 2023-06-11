package edu.hanyang.submit;

import java.io.DataInputStream;
import java.util.List;

public interface ExternalSort {
	/**
	 * Initializing a tokenizer
	 */
	void sort();
	
	/**
	 * Extrecting tokens from a given input string
	 * 
	 * @param strInput string
	 * @returnList of tokens
	 */
	
	void _externalMergeSort(String tmpDir,String outputFile,int step);
	
	/**
	 * Finalizing the tokenizer
	 *
	 */
	void  n_way_merge(List<DataInputStream> files,String outputFile);
}
