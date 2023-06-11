package edu.hanyang.submit;

import java.util.List;

public interface Tokenizer {
	/**
	 * Initializing a tokenizer
	 */
	void setup();
	
	/**
	 * Extrecting tokens from a given input string
	 * 
	 * @param strInput string
	 * @returnList of tokens
	 */
	
	List<String> split(String str);
	
	/**
	 * Finalizing the tokenizer
	 *
	 */
	void clean();
}
