package org.geirove.exmeso.kryo;

import java.io.IOException;
import java.util.Comparator;

import org.geirove.exmeso.AbstractExternalMergeSortTest;
import org.junit.Test;

public class ExternalMergeSortTest extends AbstractExternalMergeSortTest {
    
    private static final KryoSort<Integer> handler = new KryoSort<Integer>(Integer.class, new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o1.compareTo(o2);
        }
    });

    @Test
    @Override
    public void testLargeIntegerSort() throws IOException {
        performLargeIntegerSort(handler, false);
    }
    
    @Test
    public void testPrimeIntegerSort() throws IOException {
        performPrimeIntegerSort(handler, false);
    }

    @Test
    public void testMultiMergeIntegerSort() throws IOException {
        performMultiMergeIntegerSort(handler, false);
    }

}
