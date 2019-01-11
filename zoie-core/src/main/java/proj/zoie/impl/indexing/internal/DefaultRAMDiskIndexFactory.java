package proj.zoie.impl.indexing.internal;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import proj.zoie.api.indexing.IndexReaderDecorator;

import java.io.File;
import java.io.IOException;

/**
 * @param <R>
 * @author "Xiaoyang Gu<xgu@linkedin.com>"
 */
@Slf4j
public class DefaultRAMDiskIndexFactory<R extends IndexReader> extends RAMIndexFactory<R> {
    private static int fold = 10000;

    @Override
    public synchronized RAMSearchIndex<R> newInstance(String version,
                                                      IndexReaderDecorator<R> decorator, SearchIndexManager<R> idxMgr) {
        Directory ramIdxDir;
        try {
            File backingdir = new File("/tmp/ram" + fold);// /Volumes/ramdisk/
            ramIdxDir = new SimpleFSDirectory(backingdir);
            fold++;
            return new RAMSearchIndex<R>(version, decorator, idxMgr, ramIdxDir, backingdir);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            log.error("e", e);
        }// new RAMDirectory();
        return null;
    }

}
