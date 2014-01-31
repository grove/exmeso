package org.geirove.exmeso;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

public class MultiMergeIterator<T,C extends MergeIterator<T>> implements MergeIterator<T> {

    private final Queue<C> pq;
    private final Collection<C> cfs;

    private final boolean distinct;

    private T next;

    public MultiMergeIterator(Collection<C> cfs, boolean distinct) throws IOException {
        this.distinct = distinct;
        this.cfs = cfs;
        this.pq = new PriorityQueue<C>(cfs);
        readNext();
    }

    public MultiMergeIterator(Collection<C> cfs, Comparator<C> comparator, boolean distinct) throws IOException {
        this.distinct = distinct;
        this.cfs = cfs;
        int initialSize = Math.max(1, cfs.size());
        this.pq = new PriorityQueue<C>(initialSize, comparator);
        pq.addAll(cfs);
        readNext();
    }

    private void readNext() {
        T next_;
        if (pq.isEmpty()) {
            next_ = null;
        } else {
            if (distinct) {
                do {
                    C cf = pq.poll();
                    next_ = cf.next();
                    if (cf.hasNext()) {
                        pq.add(cf);
                    }
                    if (!next_.equals(next)) {
                        break;
                    }
                } while (true);
            } else {
                C cf = pq.poll();
                next_ = cf.next();
                if (cf.hasNext()) {
                    pq.add(cf);
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
        for (C cf : cfs) {
            try {
                cf.close();
            } catch (IOException e) {
                ex = e; 
            }
        }
        if (ex != null) {
            throw ex;
        }
    }

}