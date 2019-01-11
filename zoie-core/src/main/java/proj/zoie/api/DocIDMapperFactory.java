package proj.zoie.api;

import java.io.IOException;

public interface DocIDMapperFactory {
    /**
     * @param reader
     * @return
     * @throws IOException
     */
    DocIDMapper getDocIDMapper(ZoieSegmentReader<?> reader) throws IOException;

    /**
     * @param reader
     * @return
     * @throws IOException
     */
    DocIDMapper getDocIDMapper(ZoieMultiReader<?> reader) throws IOException;
}
