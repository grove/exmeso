package org.geirove.exmeso;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.geirove.exmeso.ExternalMergeSort.MergeIterator;
import org.geirove.exmeso.ExternalMergeSort.SortHandler;

public abstract class AbstractExternalMergeSortTest {

    private static class RandomIntIterator implements Iterator<Integer> {

        private Random rand = new Random();
        private int size;
        private int c;

        private RandomIntIterator(int size) {
            this.size = size;
        }

        @Override
        public boolean hasNext() {
            return c < size;
        }

        @Override
        public Integer next() {
            c++;
            return rand.nextInt();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public abstract void testLargeIntegerSort() throws IOException;

    protected void performLargeIntegerSort(SortHandler<Integer> handler) throws IOException {
        // ten million elements, 500k chunks, max 20 files
        ExternalMergeSort<Integer> sort = ExternalMergeSort.newSorter(handler)
                .withChunkSize(500000)
                .withMaxOpenFiles(20)
                .withDistinct(true)
                .withCleanup(false)
                .withBufferSize(65536)
                .build();
        assertSorted(handler, sort, new RandomIntIterator(10000000));
    }

    private void assertSorted(SortHandler<Integer> handler, ExternalMergeSort<Integer> sort, Iterator<Integer> input) throws IOException {
        long st = System.currentTimeMillis();
        int last = Integer.MIN_VALUE;
        MergeIterator<Integer> iter = sort.mergeSort(input);
        System.out.println("A: " + (System.currentTimeMillis() - st) + "ms");
        try {
            while (iter.hasNext()) {
                int i = iter.next();
                assertTrue(i + " not sorted after " + last, handler.compareValues(i, last) >= 0);
                last = i;
            }
        } finally {
            iter.close();
        }
        System.out.println("B: " + (System.currentTimeMillis() - st) + "ms");
    }

}
