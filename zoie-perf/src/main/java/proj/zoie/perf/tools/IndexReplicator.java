package proj.zoie.perf.tools;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;

import proj.zoie.api.DefaultDirectoryManager;
import proj.zoie.api.DirectoryManager;

public class IndexReplicator {

    /**
     * @param args
     */
    public static void main(String[] args) throws IOException {
        File srcIndex = new File(args[0]);
        File targetIndex = new File(args[1]);
        int numReplicas = Integer.parseInt(args[2]);
        System.out.println("source index: " + srcIndex.getAbsolutePath());
        System.out.println("target index: " + targetIndex.getAbsolutePath());
        System.out.println("num replications: " + numReplicas);
        IndexReader reader = null;
        DirectoryManager srcDirMgr = new DefaultDirectoryManager(srcIndex);
        try {
            Directory dir = srcDirMgr.getDirectory();
            reader = DirectoryReader.open(dir);
            System.out.println("source index, numdocs: " + reader.numDocs());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                reader.close();
            }
            reader = null;
        }
        DirectoryManager targetDirMgr = new DefaultDirectoryManager(targetIndex);

        Directory targetDir = targetDirMgr.getDirectory(true);

        IndexWriter writer = null;
        try {
            IndexWriterConfig idxWriterConf = new IndexWriterConfig(Version.LUCENE_43,
                    new StandardAnalyzer(Version.LUCENE_43));
            writer = new IndexWriter(targetDir, idxWriterConf);
            for (int i = 0; i < numReplicas; ++i) {
                System.out.println("replicating " + (i + 1) + " time(s)");
                writer.addIndexes(new Directory[]{srcDirMgr.getDirectory()});
            }
            System.out.println("optimizing....");
            writer.forceMerge(1);
            System.out.println("done optimizing....");
        } finally {
            if (writer != null) {
                writer.close();
            }
        }

        try {
            reader = DirectoryReader.open(targetDir);
            System.out.println("target index, numdocs: " + reader.numDocs());
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }
}
