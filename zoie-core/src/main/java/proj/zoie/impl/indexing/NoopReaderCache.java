package proj.zoie.impl.indexing;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.index.IndexReader;
import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieMultiReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class NoopReaderCache<R extends IndexReader> extends AbstractReaderCache<R> {
    private volatile boolean alreadyShutdown = false;
    private final IndexReaderFactory<R> _readerfactory;

    public NoopReaderCache(IndexReaderFactory<R> readerfactory) {
        _readerfactory = readerfactory;
    }

    @Override
    public List<ZoieMultiReader<R>> getIndexReaders() {
        if (!alreadyShutdown) {
            try {
                return _readerfactory.getIndexReaders();
            } catch (IOException e) {
                log.error("getIndexReaders", e);
            }
        }
        return new ArrayList<ZoieMultiReader<R>>(0);
    }

    @Override
    public void returnIndexReaders(List<ZoieMultiReader<R>> readers) {
        _readerfactory.returnIndexReaders(readers);
    }

    @Override
    public void refreshCache(long timeout) throws ZoieException {
    }

    @Override
    public void shutdown() {
        alreadyShutdown = true;
    }

    @Override
    public void start() {
    }

    @Override
    public long getFreshness() {
        return 0;
    }

    @Override
    public void setFreshness(long freshness) {
    }

    public static ReaderCacheFactory FACTORY = new ReaderCacheFactory() {

        @Override
        public <R extends IndexReader> AbstractReaderCache<R> newInstance(
                IndexReaderFactory<R> readerfactory) {
            return new NoopReaderCache<R>(readerfactory);
        }
    };
}
