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

import java.io.IOException;
import java.util.Comparator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.similarities.Similarity;

public class RAMLuceneIndexDataLoader<R extends IndexReader> extends LuceneIndexDataLoader<R> {

    public RAMLuceneIndexDataLoader(Analyzer analyzer, Similarity similarity,
                                    SearchIndexManager<R> idxMgr, Comparator<String> comparator) {
        super(analyzer, similarity, idxMgr, comparator);
    }

    @Override
    protected BaseSearchIndex<R> getSearchIndex() {
        return _idxMgr.getCurrentWritableMemoryIndex();
    }

    @Override
    protected void propagateDeletes(LongSet delDocs) throws IOException {
        if (delDocs == null || delDocs.size() == 0) {
            return;
        }

        RAMSearchIndex<R> readOnlyMemoryIdx = _idxMgr.getCurrentReadOnlyMemoryIndex();
        if (readOnlyMemoryIdx != null) {
            readOnlyMemoryIdx.markDeletes(delDocs);
        }

        DiskSearchIndex<R> diskIdx = _idxMgr.getDiskIndex();
        if (diskIdx != null) {
            diskIdx.markDeletes(delDocs);
        }
    }

    @Override
    protected void commitPropagatedDeletes() throws IOException {
        RAMSearchIndex<R> readOnlyMemoryIdx = _idxMgr.getCurrentReadOnlyMemoryIndex();
        if (readOnlyMemoryIdx != null) {
            readOnlyMemoryIdx.commitDeletes();
        }

        DiskSearchIndex<R> diskIdx = _idxMgr.getDiskIndex();
        if (diskIdx != null) {
            diskIdx.commitDeletes();
        }
    }
}
