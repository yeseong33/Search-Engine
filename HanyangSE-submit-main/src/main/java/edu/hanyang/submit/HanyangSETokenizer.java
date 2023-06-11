package edu.hanyang.submit;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.tartarus.snowball.ext.PorterStemmer;

import io.github.hyerica_bdml.indexer.Tokenizer;

public class HanyangSETokenizer implements Tokenizer{
    private final Analyzer analyzer=new SimpleAnalyzer();
    private final PorterStemmer stemmer=new PorterStemmer();
    
    
    public HanyangSETokenizer(){
    }

    /**
     * Tokenizes the input text and returns the list of tokens.
     */
    @Override
    public List<String> split(String text){
        List<String> tokens=new ArrayList<>();
        try (TokenStream stream=analyzer.tokenStream(null, new StringReader(text))){
            stream.reset();
            CharTermAttribute ta=stream.addAttribute(CharTermAttribute.class);
            while (stream.incrementToken()){
                stemmer.setCurrent(ta.toString());
                stemmer.stem();
                tokens.add(stemmer.getCurrent());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return tokens;
    }

    /**
     * Closes the analyzer instance.
     */
    @Override
    public void clean(){
        analyzer.close();
    }

    /**
     * Initializes the tokenizer instance.
     */
    @Override
    public void setup(){
        // no-op
    }
}