package edu.hanyang;

import io.github.hyerica_bdml.indexer.Tokenizer;
import edu.hanyang.services.ServiceProvider;
import edu.hanyang.services.TokenizeService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TokenizeServiceTest {

    static List<String[]> results;
    static List<String> testSentences;

    @BeforeClass
    public static void init() {
        results = new ArrayList<String[]>();

        String[] result1 = { "unless", "she", "i", "more", "earnest", "in", "her", "studi", "she", "will", "not", "be", "abl", "to", "keep", "up", "with",
                "the", "class" };
        String[] result2 = { "where", "will", "the", "next", "olymp", "be", "held" };
        String[] result3 = { "i", "read", "thi", "book", "the", "other", "dai", "and", "found", "it", "veri", "interest", "so", "i", "will", "give", "it", "to",
                "you" };
        String[] result4 = { "the", "test", "email", "mail", "javacodegeek", "com" };
        String[] result5 = { "won", "t", "you", "go", "to", "that", "island", "with", "me", "if", "it", "i", "fine", "tomorrow", "afternoon", "it", "take",
                "about", "an", "hour", "by", "boat" };

        results.add(result1);
        results.add(result2);
        results.add(result3);
        results.add(result4);
        results.add(result5);

        testSentences = new ArrayList<String>();
        testSentences.add("Unless she is more earnest in her studies, she will not be able to keep up with the class.");
        testSentences.add("Where will the next Olympics be held?");
        testSentences.add("I read this book the other day and found it very interesting, so I will give it to you.");
        testSentences.add("The test email - mail@javacodegeeks.com");
        testSentences.add("Won't you go to that island with me if it is fine tomorrow afternoon? It takes about an hour by boat.");

    }

    @Test
    public void TestTokenizer() {
        try {
            // external code binding
            Tokenizer obj = ServiceProvider.getTokenizeService().createNewTokenizer();
            assertNotNull(obj);

            obj.setup();
            for (int i = 0; i < testSentences.size(); i++) {
                List<String> arr = obj.split(testSentences.get(i));
                assertNotNull(arr);

                assertEquals(arr.size(), results.get(i).length);
                for (int cnt = 0; cnt < arr.size(); cnt++) {
                    assertTrue(results.get(i)[cnt].equals(arr.get(cnt)));
                }
            }
            obj.clean();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    @Test
    public void testTokenize() {
        TokenizeService tokenizer = ServiceProvider.getTokenizeService();
        tokenizer.tokenize(
                tokenizer.createNewTokenizer(),
                "data",
                "output/triples.bin",
                "output/termids.bin",
                "output/titles.bin"
        );
    }
}
