package org.geirove.exmeso;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

public class MergeSortedIterator<T,I extends CloseableIterator<T>> implements CloseableIterator<T> {

    private final Queue<I> pq;
    private final Collection<I> iters;

    private final boolean distinct;

    private T next;

    public MergeSortedIterator(Collection<I> iters, boolean distinct) throws IOException {
        this.distinct = distinct;
        this.iters = iters;
        this.pq = new PriorityQueue<I>(iters); // NOTE: C must implement Comparable<C>
        readNext();
    }

    public MergeSortedIterator(Collection<I> iters, Comparator<I> comparator, boolean distinct) throws IOException {
        this.distinct = distinct;
        this.iters = iters;
        int initialSize = Math.max(1, iters.size());
        this.pq = new PriorityQueue<I>(initialSize, comparator);
        pq.addAll(iters);
        readNext();
    }

    private void readNext() {
        T next_;
        if (pq.isEmpty()) {
            next_ = null;
        } else {
            if (distinct) {
                do {
                    I iter = pq.poll();
                    next_ = iter.next();
                    if (iter.hasNext()) {
                        pq.add(iter);
                    }
                    if (!next_.equals(next)) {
                        break;
                    }
                } while (true);
            } else {
                I iter = pq.poll();
                next_ = iter.next();
                if (iter.hasNext()) {
                    pq.add(iter);
                }
            }
        }
        this.next = next_;
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public T next() {
        T result = next;
        readNext();
        return result;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        IOException ex = null;
        for (I iter : iters) {
            try {
                iter.close();
            } catch (IOException e) {
                ex = e; 
            }
        }
        if (ex != null) {
            throw ex;
        }
    }

}