package org.geirove.exmeso;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * An iterator that checks to see if its nested iterator contains more or less 
 * than a given number of elements. Iterating over the iterator will return 
 * all elements of the nested iterator.
 * 
 * @author geir.gronmo
 *
 * @param <T>
 */
public class ChunkSizeIterator<T> implements Iterator<T> {

    private Iterator<T> head;
    private Iterator<T> tail;
    private int headSize;
    private boolean multi;
    private boolean hasNext;
    
    public ChunkSizeIterator(Iterator<T> nested, int chunkSize) {
        List<T> chunk = new ArrayList<T>(chunkSize);
        for (int i=0; i < chunkSize && nested.hasNext(); i++) {
            chunk.add(nested.next());
        }
        this.headSize = chunk.size();
        this.multi = (headSize == chunkSize && nested.hasNext());
        this.head = chunk.iterator();
        this.tail = nested;
        this.hasNext = head.hasNext(); 
    }

    /**
     * Returns true if the iterator contains more than <code>chunkSize</code> elements.
     */
    public boolean isMultipleChunks() {
        return multi;
    }

    /**
     * Returns the number of elements of the head, i.e. <code>chunkSize</code> or less elements
     */
    public int getHeadSize() {
        return headSize;
    }
    
    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public T next() {
        if (!hasNext) {
            throw new NoSuchElementException();
        }
        if (head != null) {
            T next = head.next();            
            if (head.hasNext()) {
                hasNext = true;
            } else {
                head = null;
                hasNext = tail.hasNext();
            }
            return next;
        } else {
            T next = tail.next();
            hasNext = tail.hasNext();
            return next;
        }
    }

    @Override
    public void remove() {
        if (head != null) {
            head.remove();
        } else {
            tail.remove();
        }
    }

}
