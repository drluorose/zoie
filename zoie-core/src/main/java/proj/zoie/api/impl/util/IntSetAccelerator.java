package proj.zoie.api.impl.util;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Collection;
import java.util.Iterator;

/**
 * This class, a IntSet decorator, is for accelerating look-up by having a filter
 * in front of the underlying IntSet. The filter is basically a Bloom filter.
 * The hash table size and the hash function are tuned for fast calculation of hash values.
 * The underlying IntSet should not be updated while this accelerator is in use.
 *
 * @author ymatsuda
 */
public class IntSetAccelerator implements IntSet {
    private final long[] _filter;
    private final int _mask;
    private final IntSet _set;
    private static final int MIXER = 2147482951; // a prime number

    public IntSetAccelerator(IntSet set) {
        _set = set;
        int mask = set.size() / 4;
        mask |= (mask >> 1);
        mask |= (mask >> 2);
        mask |= (mask >> 4);
        mask |= (mask >> 8);
        mask |= (mask >> 16);
        _mask = mask;
        _filter = new long[mask + 1];
        IntIterator iter = set.iterator();
        while (iter.hasNext()) {
            int h = iter.nextInt() * MIXER;

            long bits = _filter[h & _mask];
            bits |= ((1L << (h >>> 26)));
            bits |= ((1L << ((h >> 20) & 0x3F)));
            _filter[h & _mask] = bits;
        }
    }

    @Override
    public boolean contains(int val) {
        final int h = val * MIXER;
        final long bits = _filter[h & _mask];

        return (bits & (1L << (h >>> 26))) != 0 && (bits & (1L << ((h >> 20) & 0x3F))) != 0
                && _set.contains(val);
    }

    @Override
    public boolean contains(Object o) {
        return contains(((Integer) (o)).intValue());
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        final Iterator<?> i = c.iterator();
        int n = c.size();
        while (n-- != 0)
            if (!contains(i.next())) return false;

        return true;
    }

    @Override
    public boolean containsAll(IntCollection c) {
        final IntIterator i = c.iterator();
        int n = c.size();
        while (n-- != 0) {
            if (!contains(i.nextInt())) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean add(int key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean add(Integer o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends Integer> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(IntCollection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        return _set.equals(o);
    }

    @Override
    public int hashCode() {
        return _set.hashCode();
    }

    @Override
    public IntIterator intIterator() {
        return _set.iterator();
    }

    @Override
    public boolean isEmpty() {
        return _set.isEmpty();
    }

    @Override
    public IntIterator iterator() {
        return _set.iterator();
    }

    @Override
    public boolean rem(int key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(int key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(IntCollection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(IntCollection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return _set.size();
    }

    @Override
    public Object[] toArray() {
        return _set.toArray();
    }

    @Override
    public int[] toArray(int[] a) {
        return _set.toArray(a);
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return _set.toArray(a);
    }

    @Override
    public int[] toIntArray() {
        return _set.toIntArray();
    }

    @Override
    public int[] toIntArray(int[] a) {
        return _set.toIntArray(a);
    }
}
