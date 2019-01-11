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

import it.unimi.dsi.fastutil.longs.LongSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import proj.zoie.api.DocIDMapper;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.impl.ZoieMergePolicy;
import proj.zoie.api.impl.ZoieMergePolicy.MergePolicyParams;
import proj.zoie.api.impl.util.FileUtil;
import proj.zoie.api.impl.util.IndexUtil;
import proj.zoie.api.indexing.IndexReaderDecorator;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RAMSearchIndex<R extends IndexReader> extends BaseSearchIndex<R> {
    private volatile String _version;
    private final Directory _directory;
    private final File _backingdir;
    private final IndexReaderDecorator<R> _decorator;
    // a consistent pair of reader and deleted set
    private volatile ZoieMultiReader<R> _currentReader;
    private final MergePolicyParams _mergePolicyParams;

    public RAMSearchIndex(String version, IndexReaderDecorator<R> decorator,
                          SearchIndexManager<R> idxMgr, Directory ramIdxDir, File backingdir) {
        super(idxMgr, true);
        _directory = ramIdxDir;
        _backingdir = backingdir;
        _version = version;
        _decorator = decorator;
        _currentReader = null;
        _mergeScheduler = new SerialMergeScheduler();
        _mergePolicyParams = new MergePolicyParams();
        _mergePolicyParams.setNumLargeSegments(3);
        _mergePolicyParams.setMergeFactor(3);
        _mergePolicyParams.setMaxSmallSegments(4);
    }

    @Override
    public void close() {
        super.close();
        if (_currentReader != null) {
            _currentReader.decZoieRef();
        }
        if (_directory != null) {
            try {
                _directory.close();
                if (_backingdir != null) {
                    FileUtil.rmDir(_backingdir);
                }
            } catch (IOException e) {
                log.error("e", e);
            }
        }
    }

    @Override
    public String getVersion() {
        return _version;
    }

    @Override
    public void setVersion(String version) throws IOException {
        _version = version;
        synchronized (readerOpenLock) {
            readerOpenLock.notifyAll();
        }
    }

    @Override
    public int getNumdocs() {
        ZoieMultiReader<R> reader = null;
        synchronized (this) {
            reader = openIndexReader();
            if (reader == null) {
                return 0;
            }
            reader.incZoieRef();
        }
        int numDocs = reader.numDocs();
        reader.decZoieRef();
        return numDocs;
    }

    @Override
    public ZoieMultiReader<R> openIndexReader() {
        return _currentReader;
    }

    private ZoieMultiReader<R> openIndexReaderInternal() throws IOException {
        if (DirectoryReader.indexExists(_directory)) {
            DirectoryReader srcReader = null;
            ZoieMultiReader<R> finalReader = null;
            try {
                // for RAM indexes, just get a new index reader
                srcReader = DirectoryReader.open(_directory);
                finalReader = new ZoieMultiReader<R>(srcReader, _decorator);
                DocIDMapper mapper = _idxMgr._docIDMapperFactory.getDocIDMapper(finalReader);
                finalReader.setDocIDMapper(mapper);
                return finalReader;
            } catch (IOException ioe) {
                // if reader decoration fails, still need to close the source reader
                if (srcReader != null) {
                    srcReader.close();
                }
                throw ioe;
            }
        } else {
            return null; // null indicates no index exist, following the contract
        }
    }

    @Override
    public IndexWriter openIndexWriter(Analyzer analyzer, Similarity similarity) throws IOException {

        if (_indexWriter != null) {
            return _indexWriter;
        }

        ZoieMergePolicy mergePolicy = new ZoieMergePolicy();
        mergePolicy.setMergePolicyParams(_mergePolicyParams);
        mergePolicy.setUseCompoundFile(false);

        IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_43, analyzer);
        config.setOpenMode(OpenMode.CREATE_OR_APPEND);
        config.setMergeScheduler(_mergeScheduler);
        config.setMergePolicy(mergePolicy);
        config.setReaderPooling(false);
        if (similarity != null) {
            config.setSimilarity(similarity);
        }
        config.setRAMBufferSizeMB(3);

        IndexWriter idxWriter = new IndexWriter(_directory, config);
        _indexWriter = idxWriter;
        return idxWriter;
    }

    private final Object readerOpenLock = new Object();

    public ZoieMultiReader<R> openIndexReader(String minVersion, long timeout) throws IOException,
            TimeoutException {

        if (timeout < 0) {
            timeout = Long.MAX_VALUE;
        }
        if (_versionComparator.compare(minVersion, _version) <= 0) {
            return _currentReader;
        }
        long startTimer = System.currentTimeMillis();
        while (_versionComparator.compare(minVersion, _version) > 0) {
            synchronized (readerOpenLock) {
                try {
                    readerOpenLock.wait(100);
                } catch (InterruptedException e) {
                    // ignore
                }
            }

            long now = System.currentTimeMillis();
            if (now - startTimer >= timeout) throw new TimeoutException("timed-out, took: "
                    + (now - startTimer) + " ms");
        }

        return _currentReader;

    }

    @Override
    public void refresh() throws IOException {
        synchronized (this) {
            ZoieMultiReader<R> reader = null;
            if (_currentReader == null) {
                reader = openIndexReaderInternal();
            } else {
                reader = _currentReader.reopen();
                if (reader != _currentReader) {
                    DocIDMapper mapper = _idxMgr._docIDMapperFactory.getDocIDMapper(reader);
                    reader.setDocIDMapper(mapper);
                }
            }

            if (_currentReader != reader) {
                ZoieMultiReader<R> oldReader = _currentReader;
                _currentReader = reader;
                if (oldReader != null) {
                    oldReader.decZoieRef();
                }
            }
            LongSet delDocs = _delDocs;
            clearDeletes();
            markDeletes(delDocs); // re-mark deletes
            commitDeletes();
        }
    }

    public int getSegmentCount() throws IOException {
        return _directory == null ? -1 : IndexUtil.getNumSegments(_directory);
    }
}
