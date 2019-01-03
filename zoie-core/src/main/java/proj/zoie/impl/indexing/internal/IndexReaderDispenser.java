package proj.zoie.impl.indexing.internal;

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

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;

import proj.zoie.api.DirectoryManager;
import proj.zoie.api.DocIDMapper;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.indexing.IndexReaderDecorator;

public class IndexReaderDispenser<R extends IndexReader> {
    private static final Logger log = Logger.getLogger(IndexReaderDispenser.class);

    private static final int INDEX_OPEN_NUM_RETRIES = 5;
    private volatile ZoieMultiReader<R> _currentReader;
    private volatile IndexSignature _currentSignature;
    private final IndexReaderDecorator<R> _decorator;
    private final DirectoryManager _dirMgr;
    private final DiskSearchIndex<R> _idx;

    public IndexReaderDispenser(DirectoryManager dirMgr, IndexReaderDecorator<R> decorator,
                                DiskSearchIndex<R> idx) {
        _idx = idx;
        _dirMgr = dirMgr;
        _decorator = decorator;
        _currentSignature = null;
        try {
            IndexSignature sig = new IndexSignature(_dirMgr.getVersion());
            if (sig != null) {
                getNewReader();
            }
        } catch (IOException e) {
            log.error(e);
        }
    }

    public String getCurrentVersion() {
        return _currentSignature != null ? _currentSignature.getVersion() : null;
    }

    /**
     * constructs a new IndexReader instance
     *
     * @param dirMgr    Where the index is.
     * @param decorator
     * @param signature
     * @return Constructed IndexReader instance.
     * @throws IOException
     */
    private ZoieMultiReader<R> newReader(DirectoryManager dirMgr, IndexReaderDecorator<R> decorator,
                                         IndexSignature signature) throws IOException {
        if (!dirMgr.exists()) {
            return null;
        }

        Directory dir = dirMgr.getDirectory();

        if (!DirectoryReader.indexExists(dir)) {
            return null;
        }

        int numTries = INDEX_OPEN_NUM_RETRIES;
        ZoieMultiReader<R> reader = null;

        // try max of 5 times, there might be a case where the segment file is being updated
        while (reader == null) {
            if (numTries == 0) {
                log.error("Problem refreshing disk index, all attempts failed.");
                throw new IOException("problem opening new index");
            }
            numTries--;

            try {
                if (log.isDebugEnabled()) {
                    log.debug("opening index reader at: " + dirMgr.getPath());
                }
                DirectoryReader srcReader = DirectoryReader.open(dir);

                try {
                    reader = new ZoieMultiReader<R>(srcReader, decorator);
                    _currentSignature = signature;
                } catch (IOException ioe) {
                    // close the source reader if ZoieMultiReader construction fails
                    if (srcReader != null) {
                        srcReader.close();
                    }
                    throw ioe;
                }
            } catch (IOException ioe) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    log.warn("thread interrupted.");
                    continue;
                }
            }
        }
        return reader;
    }

    /**
     * get a fresh new reader instance
     *
     * @return an IndexReader instance, can be null if index does not yet exit
     * @throws IOException
     */
    public ZoieMultiReader<R> getNewReader() throws IOException {
        int numTries = INDEX_OPEN_NUM_RETRIES;
        ZoieMultiReader<R> reader = null;

        // try it for a few times, there is a case where lucene is swapping the segment file,
        // or a case where the index directory file is updated, both are legitimate,
        // trying again does not block searchers,
        // the extra time it takes to get the reader, and to sync the index, memory index is collecting
        // docs

        while (reader == null) {
            if (numTries == 0) {
                break;
            }
            numTries--;
            try {
                IndexSignature sig = new IndexSignature(_dirMgr.getVersion());

                if (_currentReader == null) {
                    reader = newReader(_dirMgr, _decorator, sig);
                    break;
                } else {
                    reader = _currentReader.reopen();
                    _currentSignature = sig;
                }
            } catch (IOException ioe) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    log.warn("thread interrupted.");
                    continue;
                }
            }
        }

        // swap the internal readers
        if (_currentReader != reader) {
            if (reader != null) {
                DocIDMapper mapper = _idx._idxMgr._docIDMapperFactory.getDocIDMapper(reader);
                reader.setDocIDMapper(mapper);
            }
            // assume that this is the only place that _currentReader gets refreshed
            ZoieMultiReader<R> oldReader = _currentReader;
            _currentReader = reader;
            // we release our hold on the old reader so that it will be closed when
            // all the clients release their hold on it, the reader will be closed
            // automatically.
            log.info("swap disk reader and release old one from system");
            if (oldReader != null) {
                oldReader.decZoieRef();
            }
        }
        return reader;
    }

    public ZoieMultiReader<R> getIndexReader() {
        if (_currentReader != null) {
            return _currentReader;
        } else {
            return null;
        }
    }

    /**
     * Closes the factory.
     */
    public void close() {
        closeReader();
    }

    /**
     * Closes the index reader
     */
    public void closeReader() {
        if (_currentReader != null) {
            _currentReader.decZoieRef();
            int count = _currentReader.getInnerRefCount();
            log.info("final closeReader in dispenser and current refCount: " + count);
            if (count > 0) {
                log.warn("final closeReader call with reference count == " + count
                        + " greater than 0. Potentially, "
                        + "the IndexReaders are not properly return to ZoieSystem.");
            }
            _currentReader = null;
        }
    }
}
