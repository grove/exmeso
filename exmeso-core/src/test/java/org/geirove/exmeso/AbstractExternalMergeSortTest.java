package org.geirove.exmeso;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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

    protected void performLargeIntegerSort(SortHandler<Integer> handler, boolean distinct) throws IOException {
        // ten million elements, 500k chunks, max 19 files
        ExternalMergeSort<Integer> sort = ExternalMergeSort.newSorter(handler)
                .withChunkSize(500000)
                .withMaxOpenFiles(19)
                .withDistinct(distinct)
                .withCleanup(!ExternalMergeSort.debug)
                .withBufferSize(65536)
                .build();
        int size = 10000000;
        assertSorted(handler, sort, new RandomIntIterator(size), size, distinct);
    }

    protected void performPrimeIntegerSort(SortHandler<Integer> handler, boolean distinct) throws IOException {
        ExternalMergeSort<Integer> sort = ExternalMergeSort.newSorter(handler)
                .withChunkSize(21)
                .withMaxOpenFiles(7)
                .withDistinct(distinct)
                .withCleanup(!ExternalMergeSort.debug)
                .build();
        int size = 9123;
        assertSorted(handler, sort, new RandomIntIterator(size), size, distinct);
    }

    protected void performMultiMergeIntegerSort(SortHandler<Integer> handler, boolean distinct) throws IOException {
        ExternalMergeSort<Integer> sort = ExternalMergeSort.newSorter(handler)
                .withChunkSize(3)
                .withMaxOpenFiles(5)
                .withDistinct(distinct)
                .withCleanup(!ExternalMergeSort.debug)
                .build();
        int size = 37;
        assertSorted(handler, sort, new RandomIntIterator(size), size, distinct);
    }

    private void assertSorted(SortHandler<Integer> handler, ExternalMergeSort<Integer> sort, Iterator<Integer> input, int size, boolean distinct) throws IOException {
        long st = System.currentTimeMillis();
        int last = Integer.MIN_VALUE;
        MergeIterator<Integer> iter = sort.mergeSort(input);
        if (ExternalMergeSort.debug) {
            System.out.println("A: " + (System.currentTimeMillis() - st) + "ms");
        }
        try {
            int count = 0;
            while (iter.hasNext()) {
                count ++;
                int i = iter.next();
                assertTrue(i + " not sorted after " + last, handler.compareValues(i, last) >= 0);
                last = i;
            }
            if (!distinct && count != size) {
                fail("Incorrect size: " + count + " vs " + size);
            }
        } finally {
            iter.close();
        }
        if (ExternalMergeSort.debug) {
            System.out.println("B: " + (System.currentTimeMillis() - st) + "ms");
        }
    }

}
