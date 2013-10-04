package org.geirove.exmeso.kryo;

import java.io.IOException;
import java.util.Comparator;

import org.geirove.exmeso.AbstractExternalMergeSortTest;
import org.junit.Test;

public class ExternalMergeSortTest extends AbstractExternalMergeSortTest {
    
    private static final Comparator<Integer> integerComparator = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            return o1.compareTo(o2);
        }
    };
    private static final KryoSerializer<Integer> integerSerializer = new KryoSerializer<Integer>(Integer.class);

    @Test
    @Override
    public void testLargeIntegerSort() throws IOException {
        performLargeIntegerSort(integerSerializer, integerComparator, false);
    }
    
    @Test
    public void testPrimeIntegerSort() throws IOException {
        performPrimeIntegerSort(integerSerializer, integerComparator, false);
    }

    @Test
    public void testMultiMergeIntegerSort() throws IOException {
        performMultiMergeIntegerSort(integerSerializer, integerComparator, false);
    }

}
