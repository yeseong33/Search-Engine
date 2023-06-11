package edu.hanyang;

import edu.hanyang.services.ExternalSortService;
import org.junit.Test;

public class ExternalSortTest {

    @Test
    public void sortingTest() {
        ExternalSortService service = new ExternalSortService();
        service.sort(service.createNewExternalSort(), "output/triples.bin", "output/sortedTriples.bin", "tmp", 4096, 1000);
    }
}
