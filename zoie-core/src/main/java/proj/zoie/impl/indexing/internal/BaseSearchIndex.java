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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.similarities.Similarity;

import proj.zoie.api.UIDFilter;
import proj.zoie.api.ZoieHealth;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.api.indexing.ZoieIndexable.IndexingReq;

@Slf4j
public abstract class BaseSearchIndex<R extends IndexReader> {

    private int _eventsHandled = 0;
    protected MergeScheduler _mergeScheduler;
    protected IndexWriter _indexWriter = null;
    protected volatile LongOpenHashSet _delDocs = new LongOpenHashSet();
    protected final SearchIndexManager<R> _idxMgr;
    protected boolean _closeWriterAfterUpdate;
    protected final Comparator<String> _versionComparator;

    protected BaseSearchIndex(SearchIndexManager<R> idxMgr, boolean closeWriterAfterUpdate) {
        _idxMgr = idxMgr;
        _closeWriterAfterUpdate = closeWriterAfterUpdate;
        _versionComparator = idxMgr.getVersionComparator();
    }

    /**
     * gets index version, e.g. SCN
     *
     * @return index version
     */
    abstract String getVersion();

    /**
     * gets number of docs in the index, .e.g maxdoc - number of deleted docs
     */
    abstract public int getNumdocs();

    /**
     * Sets the index version
     *
     * @param version
     * @throws IOException
     */
    abstract public void setVersion(String version) throws IOException;

    /**
     * close and free all resources
     */
    public void close() {
        closeIndexWriter();
    }

    abstract public ZoieMultiReader<R> openIndexReader();

    abstract public void refresh() throws IOException;

    public void updateIndex(LongSet delDocs, List<IndexingReq> insertDocs, Analyzer defaultAnalyzer,
                            Similarity similarity) throws IOException {
        if (delDocs != null && delDocs.size() > 0) {
            deleteDocs(delDocs);
        }

        if (insertDocs == null || insertDocs.size() == 0) {
            return;
        }

        IndexWriter idxMod = null;
        try {
            idxMod = openIndexWriter(defaultAnalyzer, similarity);
            if (idxMod != null) {
                for (IndexingReq idxPair : insertDocs) {
                    Analyzer analyzer = idxPair.getAnalyzer();
                    Document doc = idxPair.getDocument();
                    if (analyzer == null) {
                        idxMod.addDocument(doc);
                    } else {
                        idxMod.addDocument(doc, analyzer);
                    }
                }
            }
        } finally {
            if (idxMod != null) {
                idxMod.commit();
                if (_closeWriterAfterUpdate) {
                    closeIndexWriter();
                }
            }
        }
    }

    public LongSet getDelDocs() {
        return _delDocs;
    }

    public synchronized void clearDeletes() {
        _delDocs = new LongOpenHashSet();
    }

    public void markDeletes(LongSet delDocs) throws IOException {
        if (delDocs != null && delDocs.size() > 0) {
            ZoieMultiReader<R> reader = null;
            synchronized (this) {
                reader = openIndexReader();
                if (reader == null) {
                    return;
                }
                reader.incZoieRef();
                reader.markDeletes(delDocs, _delDocs);
                reader.decZoieRef();
            }
        }
    }

    public void commitDeletes() throws IOException {
        ZoieMultiReader<R> reader = null;
        synchronized (this) {
            reader = openIndexReader();
            if (reader == null) {
                return;
            }
            reader.incZoieRef();
            reader.commitDeletes();
            reader.decZoieRef();
        }
    }

    private void deleteDocs(LongSet delDocs) throws IOException {
        if (delDocs == null || delDocs.size() == 0) {
            return;
        }

        ZoieMultiReader<R> reader = openIndexReader();
        if (reader == null) {
            return;
        }

        UIDFilter uidFilter = new UIDFilter(delDocs.toLongArray(), reader);
        IndexWriter writer = null;
        try {
            writer = openIndexWriter(null, null);
            writer.deleteDocuments(new ConstantScoreQuery(uidFilter));
            writer.commit();
        } finally {
            closeIndexWriter();
        }
    }

    public void loadFromIndex(BaseSearchIndex<R> index) throws IOException {
        LongSet delDocs = null;
        // delete docs in disk index first
        synchronized (this) {
            if (_delDocs != null && _delDocs.size() > 0) {
                delDocs = _delDocs;
                clearDeletes();
            }
        }
        deleteDocs(delDocs);

        // open readOnly ram index reader
        ZoieMultiReader<R> reader = index.openIndexReader();
        if (reader == null) {
            return;
        }
        // merge the readOnly ram index with the disk index
        IndexWriter writer = null;
        try {
            writer = openIndexWriter(null, null);
            writer.addIndexes(reader.directory());
            writer.maybeMerge();
        } finally {
            closeIndexWriter();
        }
    }

    abstract public IndexWriter openIndexWriter(Analyzer analyzer, Similarity similarity)
            throws IOException;

    public void closeIndexWriter() {
        if (_indexWriter != null) {
            try {
                _indexWriter.close();
            } catch (Exception e) {
                ZoieHealth.setFatal();
                log.error(e.getMessage(), e);
            }
            _indexWriter = null;
        }
    }

    public void incrementEventCount(int eventCount) {
        _eventsHandled += eventCount;
    }

    public int getEventsHandled() {
        return _eventsHandled;
    }
}
