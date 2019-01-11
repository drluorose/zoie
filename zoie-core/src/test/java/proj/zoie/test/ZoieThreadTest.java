package proj.zoie.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.Version;
import org.junit.Test;
import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieMultiReader;
import proj.zoie.impl.indexing.MemoryStreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;
import proj.zoie.impl.indexing.ZoieSystem;
import proj.zoie.test.data.DataForTests;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertTrue;

@Slf4j
public class ZoieThreadTest extends ZoieTestCaseBase {
    public ZoieThreadTest() {
    }

    private static abstract class QueryRunnable implements Runnable {
        public volatile boolean stop = false;
        public volatile boolean mismatch = false;
        public volatile String message = null;
        public Exception exception = null;
    }

    @Test
    public void testThreadDelImpl() throws ZoieException {
        File idxDir = getIdxDir();
        final ZoieSystem<IndexReader, String> idxSystem = createZoie(idxDir, true, 100,
                ZoieConfig.DEFAULT_VERSION_COMPARATOR);
        for (String bname : idxSystem.getStandardMBeanNames()) {
            registerMBean(idxSystem.getStandardMBean(bname), bname);
        }
        idxSystem.start();
        int numThreads = 5;
        QueryRunnable[] queryRunnables = new QueryRunnable[numThreads];
        for (int i = 0; i < queryRunnables.length; i++) {
            queryRunnables[i] = new QueryRunnable() {
                @Override
                public void run() {
                    final String query = "zoie";
                    QueryParser parser = new QueryParser(Version.LUCENE_43, "contents",
                            idxSystem.getAnalyzer());
                    Query q;
                    try {
                        q = parser.parse(query);
                    } catch (Exception e) {
                        exception = e;
                        return;
                    }
                    int expected = DataForTests.testdata.length;
                    while (!stop) {
                        IndexSearcher searcher = null;
                        List<ZoieMultiReader<IndexReader>> readers = null;
                        MultiReader reader = null;
                        try {
                            readers = idxSystem.getIndexReaders();
                            reader = new MultiReader(readers.toArray(new IndexReader[readers.size()]), false);
                            searcher = new IndexSearcher(reader);
                            TopDocs hits = searcher.search(q, 10);
                            int count = hits.totalHits;
                            if (count != expected) {
                                mismatch = true;
                                message = "hit count: " + count + " / expected: " + expected;
                                stop = true;
                                StringBuffer sb = new StringBuffer();
                                sb.append(message + "\n");
                                sb.append("each\n");
                                sb.append(groupDump(readers, q));
                                sb.append("main\n");
                                sb.append(dump(reader, hits));
                                System.out.println(sb.toString());
                                log.info(sb.toString());
                            }
                            Thread.sleep(20);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            exception = ex;
                            stop = true;
                        } finally {
                            try {
                                if (searcher != null) {
                                    reader.close();
                                    reader = null;
                                    searcher = null;
                                }
                            } catch (IOException ioe) {
                                log.error(ioe.getMessage(), ioe);
                            } finally {
                                idxSystem.returnIndexReaders(readers);
                            }
                        }
                    }
                }

                private String groupDump(List<ZoieMultiReader<IndexReader>> readers, Query q)
                        throws IOException {
                    StringBuffer sb = new StringBuffer();
                    for (ZoieMultiReader<IndexReader> reader : readers) {
                        sb.append(reader).append("\n");
                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs hits = searcher.search(q, 20);
                        sb.append(dump(reader, hits));
                        searcher = null;
                    }
                    return sb.toString();
                }

                private String dump(IndexReader reader, TopDocs hits) throws CorruptIndexException,
                        IOException {
                    StringBuffer sb = new StringBuffer();
                    ScoreDoc[] sd = hits.scoreDocs;
                    long[] uids = new long[sd.length];
                    for (int i = 0; i < sd.length; i++) {
                        Document doc = reader.document(sd[i].doc);
                        uids[i] = Long.parseLong(doc.get("id"));
                    }
                    sb.append(Thread.currentThread() + Arrays.toString(uids)).append("\n");
                    int max = reader.maxDoc();
                    uids = new long[max];
                    for (int i = 0; i < max; i++) {
                        Document doc = reader.document(i);
                        uids[i] = Long.parseLong(doc.get("id"));
                    }
                    sb.append("uids: " + Arrays.toString(uids)).append("\n");
                    return sb.toString();
                }
            };
        }
        MemoryStreamDataProvider<String> memoryProvider = new MemoryStreamDataProvider<String>(
                ZoieConfig.DEFAULT_VERSION_COMPARATOR);
        memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
        memoryProvider.setDataConsumer(idxSystem);
        memoryProvider.start();
        ExecutorService threadPool = Executors.newCachedThreadPool();
        try {
            idxSystem.setBatchSize(10);
            final int count = DataForTests.testdata.length;
            List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(count);
            for (int i = 0; i < count; i++) {
                list.add(new DataEvent<String>(DataForTests.testdata[i], "" + i));
            }
            memoryProvider.addEvents(list);
            idxSystem.syncWithVersion(100000, "" + (count - 1));
            @SuppressWarnings("rawtypes")
            Future[] futures = new Future<?>[queryRunnables.length];
            for (int x = 0; x < queryRunnables.length; x++) {
                futures[x] = threadPool.submit(queryRunnables[x]);
            }
            for (int n = 1; n <= 3; n++) {
                for (int i = 0; i < count; i++) {
                    long version = n * count + i;
                    list = new ArrayList<DataEvent<String>>(1);
                    list.add(new DataEvent<String>(DataForTests.testdata[i], "" + version));
                    memoryProvider.addEvents(list);
                    idxSystem.syncWithVersion(100000, "" + version);
                }
                boolean stopNow = false;
                for (QueryRunnable queryThread : queryRunnables)
                    stopNow |= queryThread.stop;
                if (stopNow) break;
            }
            for (QueryRunnable queryThread : queryRunnables)
                queryThread.stop = true; // stop all query threads
            for (int x = 0; x < queryRunnables.length; x++) {
                futures[x].get();
                assertTrue("count mismatch[" + queryRunnables[x].message + "]", !queryRunnables[x].mismatch);
            }
        } catch (Exception e) {
            for (QueryRunnable queryThread : queryRunnables) {
                if (queryThread.exception == null) throw new ZoieException(e);
            }
        } finally {
            memoryProvider.stop();
            for (String bname : idxSystem.getStandardMBeanNames()) {
                unregisterMBean(bname);
            }
            idxSystem.shutdown();
            deleteDirectory(idxDir);
        }
        System.out.println(" done round");
        log.info(" done round");
        for (QueryRunnable queryThread : queryRunnables) {
            if (queryThread.exception != null) throw new ZoieException(queryThread.exception);
        }
    }

    @Test
    public void testDelBigSet() throws ZoieException {
        for (int i = 0; i < 2; i++) {
            System.out.println("testDelBigSet Round: " + i);
            log.info("\n\n\ntestDelBigSet Round: " + i);
            testDelBigSetImpl();
        }
    }

    private void testDelBigSetImpl() throws ZoieException {
        long starttime = System.currentTimeMillis();
        final long testduration = 3000L;
        final long endtime = starttime + testduration;
        final int membatchsize = 1;
        File idxDir = getIdxDir();
        final int datacount = 100;
        final String[] testdata = new String[datacount];
        Random r = new Random(0);
        for (int i = 0; i < datacount; i++) {
            testdata[i] = "zoie " + (i % 2 == 0 ? "even " : "odd ") + i;
        }
        final ZoieSystem<IndexReader, String> idxSystem = createZoie(idxDir, true, 2,
                ZoieConfig.DEFAULT_VERSION_COMPARATOR);
        for (String bname : idxSystem.getStandardMBeanNames()) {
            registerMBean(idxSystem.getStandardMBean(bname), bname);
        }
        idxSystem.getAdminMBean().setFreshness(20);
        idxSystem.start();
        int numThreads = 5;
        QueryThread[] queryThreads = new QueryThread[numThreads];
        for (int i = 0; i < queryThreads.length; i++) {
            queryThreads[i] = new QueryThread() {
                @Override
                public void run() {
                    final String query = "zoie";
                    QueryParser parser = new QueryParser(Version.LUCENE_43, "contents",
                            idxSystem.getAnalyzer());
                    Query q;
                    try {
                        q = parser.parse(query);
                    } catch (Exception e) {
                        exception = e;
                        return;
                    }

                    int expected = testdata.length;
                    while (!stop) {
                        IndexSearcher searcher = null;
                        List<ZoieMultiReader<IndexReader>> readers = null;
                        MultiReader reader = null;
                        try {
                            readers = idxSystem.getIndexReaders();
                            reader = new MultiReader(readers.toArray(new IndexReader[readers.size()]), false);
                            searcher = new IndexSearcher(reader);
                            TopDocs hits = searcher.search(q, 10);
                            int count = hits.totalHits;

                            if (count != expected) {
                                mismatch = true;
                                message = "hit count: " + count + " / expected: " + expected;
                                stop = true;
                                StringBuffer sb = new StringBuffer();
                                sb.append(message + "\n");
                                sb.append("each\n");
                                sb.append(groupDump(readers, q));
                                sb.append("main\n");
                                sb.append(dump(reader, hits));
                                System.out.println(sb.toString());
                                log.info(sb.toString());
                            }
                            Thread.sleep(2);
                        } catch (Exception ex) {
                            ex.printStackTrace();
                            exception = ex;
                            stop = true;
                        } finally {
                            try {
                                if (searcher != null) {
                                    reader.close();
                                    reader = null;
                                    searcher = null;
                                }
                            } catch (IOException ioe) {
                                log.error(ioe.getMessage(), ioe);
                            } finally {
                                idxSystem.returnIndexReaders(readers);
                            }
                        }
                    }
                }

                private String groupDump(List<ZoieMultiReader<IndexReader>> readers, Query q)
                        throws IOException {
                    StringBuffer sb = new StringBuffer();
                    for (ZoieMultiReader<IndexReader> reader : readers) {
                        sb.append(reader);
                        IndexSearcher searcher = new IndexSearcher(reader);
                        TopDocs hits = searcher.search(q, 20);
                        sb.append(dump(reader, hits)).append("\n");
                        searcher = null;
                    }
                    return sb.toString();
                }

                private String dump(IndexReader reader, TopDocs hits) throws CorruptIndexException,
                        IOException {
                    StringBuffer sb = new StringBuffer();
                    ScoreDoc[] sd = hits.scoreDocs;
                    long[] uids = new long[sd.length];
                    sb.append("\n");
                    for (int i = 0; i < sd.length; i++) {
                        Document doc = reader.document(sd[i].doc);
                        uids[i] = Long.parseLong(doc.get("id"));
                    }
                    Arrays.sort(uids);
                    sb.append(Thread.currentThread() + Arrays.toString(uids)).append("\n");
                    int max = reader.maxDoc();
                    uids = new long[max];
                    for (int i = 0; i < max; i++) {
                        Document doc = reader.document(i);
                        uids[i] = Long.parseLong(doc.get("id"));
                    }
                    Arrays.sort(uids);
                    sb.append("uids: " + Arrays.toString(uids)).append("\n");
                    return sb.toString();
                }
            };
            queryThreads[i].setDaemon(true);
        }

        MemoryStreamDataProvider<String> memoryProvider = new MemoryStreamDataProvider<String>(
                ZoieConfig.DEFAULT_VERSION_COMPARATOR);
        memoryProvider.setMaxEventsPerMinute(Long.MAX_VALUE);
        memoryProvider.setBatchSize(membatchsize);
        memoryProvider.setDataConsumer(idxSystem);
        memoryProvider.start();
        try {
            idxSystem.setBatchSize(10);

            final int count = testdata.length;
            List<DataEvent<String>> list = new ArrayList<DataEvent<String>>(count);
            for (int i = 0; i < count; i++) {
                list.add(new DataEvent<String>(testdata[i], "" + i));
            }
            memoryProvider.addEvents(list);

            idxSystem.syncWithVersion(1000000, "" + (count - 1));

            for (QueryThread queryThread : queryThreads)
                queryThread.start();

            for (int n = 1; n <= 3; n++) {
                for (int i = 0; i < count; i++) {
                    long version = n * count + i;
                    list = new ArrayList<DataEvent<String>>(1);
                    list.add(new DataEvent<String>(testdata[r.nextInt(testdata.length)], "" + version));
                    memoryProvider.addEvents(list);
                    idxSystem.syncWithVersion(100000, "" + version);
                    if (System.currentTimeMillis() > endtime) {
                        break;
                    }
                }

                boolean stopNow = false;
                for (QueryThread queryThread : queryThreads)
                    stopNow |= queryThread.stop;
                if (stopNow) break;
            }
            for (QueryThread queryThread : queryThreads)
                queryThread.stop = true; // stop all query threads
            for (QueryThread queryThread : queryThreads) {
                queryThread.join();
                assertTrue("count mismatch[" + queryThread.message + "]", !queryThread.mismatch);
            }
        } catch (Exception e) {
            for (QueryThread queryThread : queryThreads) {
                if (queryThread.exception == null) throw new ZoieException(e);
            }
        } finally {
            memoryProvider.stop();
            for (String bname : idxSystem.getStandardMBeanNames()) {
                unregisterMBean(bname);
            }
            idxSystem.shutdown();
            deleteDirectory(idxDir);
        }
        System.out.println(" done round");
        log.info(" done round");
        for (QueryThread queryThread : queryThreads) {
            if (queryThread.exception != null) throw new ZoieException(queryThread.exception);
        }
    }
}
