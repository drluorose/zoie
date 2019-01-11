package proj.zoie.api.indexing;

/**
 * Interface to translate from a data object to an indexable object.
 */
public interface ZoieIndexableInterpreter<V> {
    /**
     *
     * @param src
     * @return
     */
    ZoieIndexable convertAndInterpret(V src);
}
