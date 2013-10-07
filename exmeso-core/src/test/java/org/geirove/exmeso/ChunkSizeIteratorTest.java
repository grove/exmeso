package org.geirove.exmeso;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

public class ChunkSizeIteratorTest {

    private static final String A = "A";
    private static final String B = "B";
    private static final String C = "C";
    private static final String D = "D";
    private static final String E = "E";
    private static final String F = "F";

    @Test
    public void testMulti() {
        ChunkSizeIterator<String> csi = newIterator(3, A, B, C, D, E, F);
        assertMulti(csi);
        assertEqualIterator(csi, A, B, C, D, E, F);
    }

    @Test
    public void testSingle2() {
        ChunkSizeIterator<String> csi = newIterator(3, A, B);
        assertNotMulti(csi);
        assertEqualIterator(csi, A, B);
    }

    @Test
    public void testSingle3() {
        ChunkSizeIterator<String> csi = newIterator(3, A, B, C);
        assertNotMulti(csi);
        assertEqualIterator(csi, A, B, C);
    }
    
    private <T> void assertMulti(ChunkSizeIterator<T> csi) {
        assertTrue("Not multi-chunk", csi.isMultiChunk());
    }
    
    private <T> void assertNotMulti(ChunkSizeIterator<T> csi) {
        assertTrue("Multi-chunk", !csi.isMultiChunk());
    }
    
    private <T> void assertEqualIterator(ChunkSizeIterator<T> i1, T... values) {
        Iterator<T> i2 = Arrays.asList(values).iterator();
        while (i1.hasNext() && i2.hasNext()) {
            T o1 = i1.next();
            T o2 = i2.next();
            assertEquals(o1, o2);
        }
        if (i1.hasNext()) {
            fail("Iterator 1 has more element(s): " + i1.next());
        }
        if (i2.hasNext()) {
            fail("Iterator 2 has more element(s): " + i2.next());
        }
    }
    
    private <T> ChunkSizeIterator<T> newIterator(int chunkSize, T... values) {
        Iterator<T> iter = Arrays.asList(values).iterator();
        return new ChunkSizeIterator<T>(iter, chunkSize);
    }

}
