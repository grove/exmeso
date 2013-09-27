package org.geirove.exmeso.kryo;

import java.io.IOException;
import java.util.Comparator;

import org.geirove.exmeso.AbstractExternalMergeSortTest;
import org.junit.Test;

public class ExternalMergeSortTest extends AbstractExternalMergeSortTest {

    @Test
    @Override
    public void testLargeIntegerSort() throws IOException {
        KryoSort<Integer> handler = new KryoSort<Integer>(Integer.class, new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        });
        performPrimeIntegerSort(handler, false);
        performLargeIntegerSort(handler, false);
    }

}
