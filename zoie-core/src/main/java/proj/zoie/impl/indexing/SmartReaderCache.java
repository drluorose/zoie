package proj.zoie.impl.indexing;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;

import proj.zoie.api.IndexReaderFactory;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieMultiReader;

/**
 * @author dongjiejie
 */
public class SmartReaderCache<R extends IndexReader> extends AbstractReaderCache<R> {
    private static final Logger log = Logger.getLogger(DefaultReaderCache.class);
    private final Thread _maintenance;
    private volatile boolean alreadyShutdown = false;
    private volatile List<ZoieMultiReader<R>> cachedreaders = new ArrayList<ZoieMultiReader<R>>(0);
    private volatile long cachedreaderTimestamp = 0;
    private final Object cachemonitor = new Object();
    private long _freshness = 10000L;
    private final IndexReaderFactory<R> _readerfactory;
    private final HashMap<WeakReference<List<ZoieMultiReader<R>>>, List<ZoieMultiReader<R>>> readermap;
    private final ReferenceQueue<List<ZoieMultiReader<R>>> refq;

    public SmartReaderCache(IndexReaderFactory<R> readerfactory) {
        readermap = new HashMap<WeakReference<List<ZoieMultiReader<R>>>, List<ZoieMultiReader<R>>>();
        refq = new ReferenceQueue<List<ZoieMultiReader<R>>>();
        _readerfactory = readerfactory;
        _maintenance = newMaintenanceThread();
        _maintenance.setDaemon(true);
    }

    @Override
    public List<ZoieMultiReader<R>> getIndexReaders() {
        return cachedreaders;
    }

    @Override
    public void returnIndexReaders(List<ZoieMultiReader<R>> readers) {
    }

    @Override
    public void refreshCache(long timeout) throws ZoieException {
        long begintime = System.currentTimeMillis();
        while (cachedreaderTimestamp <= begintime) {
            synchronized (cachemonitor) {
                cachemonitor.notifyAll();
                long elapsed = System.currentTimeMillis() - begintime;
                if (elapsed > timeout) {
                    log.debug("refreshCached reader timeout in " + elapsed + "ms");
                    throw new ZoieException("refreshCached reader timeout in " + elapsed + "ms");
                }
                long timetowait = Math.min(timeout - elapsed, 200);
                try {
                    cachemonitor.wait(timetowait);
                } catch (InterruptedException e) {
                    log.warn("refreshCache", e);
                }
            }
        }
    }

    @Override
    public void shutdown() {
        _freshness = 30000L;
        alreadyShutdown = true;
    }

    @Override
    public void start() {
        _maintenance.start();
    }

    @Override
    public long getFreshness() {
        return _freshness;
    }

    @Override
    public void setFreshness(long freshness) {
        if (freshness < 0) {
            log.warn("freshness has to be at least 0 and cannot be " + freshness);
            freshness = 10000;
        }
        log.info("setting freshness to " + freshness + "ms");
        _freshness = freshness;
    }

    private Thread newMaintenanceThread() {
        return new Thread("SmartReaderCache-zoie-indexReader-maintenance") {
            @Override
            public void run() {
                while (true) {
                    try {
                        synchronized (cachemonitor) {
                            cachemonitor.wait(_freshness);
                        }
                    } catch (InterruptedException e) {
                        Thread.interrupted(); // clear interrupted state
                    }
                    List<ZoieMultiReader<R>> newreaders = null;
                    if (alreadyShutdown) {
                        newreaders = new ArrayList<ZoieMultiReader<R>>(0);
                        // clean up and quit
                    } else {
                        try {
                            newreaders = _readerfactory.getIndexReaders();
                        } catch (IOException e) {
                            log.info("SmartReaderCache-zoie-indexReader-maintenance", e);
                            newreaders = new ArrayList<ZoieMultiReader<R>>();
                        }
                    }
                    cachedreaders = new ArrayList<ZoieMultiReader<R>>(newreaders);
                    if (cachedreaders.size() > 0) {
                        WeakReference<List<ZoieMultiReader<R>>> w = new WeakReference<List<ZoieMultiReader<R>>>(
                                cachedreaders, refq);
                        readermap.put(w, newreaders); // when nobody uses cachedreaders, we will clean
                        // newreaders :)
                    }
                    cachedreaderTimestamp = System.currentTimeMillis();
                    synchronized (cachemonitor) {
                        cachemonitor.notifyAll();
                    }
                    // cleaning and reference counting on the ones no longer in use
                    Reference<? extends List<ZoieMultiReader<R>>> wclean = null;
                    while ((wclean = refq.poll()) != null) {
                        List<ZoieMultiReader<R>> readers = readermap.remove(wclean);
                        _readerfactory.returnIndexReaders(readers);
                    }
                    if (alreadyShutdown && readermap.size() == 0) {
                        log.info("SmartReaderCache-zoie-indexReader-maintenance done.");
                        break;
                    }
                }
            }
        };
    }

    public static ReaderCacheFactory FACTORY = new ReaderCacheFactory() {

        @Override
        public <R extends IndexReader> AbstractReaderCache<R> newInstance(IndexReaderFactory<R> readerfactory) {
            return new SmartReaderCache<R>(readerfactory);
        }
    };
}
