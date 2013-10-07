package org.geirove.exmeso;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class ChunkSizeIterator<T> implements Iterator<T> {

    private Iterator<T> head;
    private Iterator<T> tail;
    private boolean multi;
    private boolean hasNext;
    
    public ChunkSizeIterator(Iterator<T> nested, int chunkSize) {
        List<T> chunk = new ArrayList<T>(chunkSize);
        for (int i=0; i < chunkSize && nested.hasNext(); i++) {
            chunk.add(nested.next());
        }
        this.multi = (chunk.size() == chunkSize && nested.hasNext());
        this.head = chunk.iterator();
        this.tail = nested;
        this.hasNext = head.hasNext(); 
    }

    public boolean isMultiChunk() {
        return multi;
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
