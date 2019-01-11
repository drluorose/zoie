package proj.zoie.store;

import lombok.extern.slf4j.Slf4j;
import proj.zoie.api.LifeCycleCotrolledDataConsumer;
import proj.zoie.api.ZoieException;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;

@Slf4j
public class ZoieStoreConsumer<D> implements LifeCycleCotrolledDataConsumer<D> {

    private final ZoieStore _store;
    private final ZoieStoreSerializer<D> _serializer;

    public ZoieStoreConsumer(ZoieStore store, ZoieStoreSerializer<D> serializer) {
        _store = store;
        _serializer = serializer;
    }

    @Override
    public void consume(Collection<proj.zoie.api.DataConsumer.DataEvent<D>> data)
            throws ZoieException {
        for (DataEvent<D> datum : data) {
            String version = datum.getVersion();
            D obj = datum.getData();

            try {
                byte[] bytes = _serializer.toBytes(obj);
                long id = _serializer.getUid(obj);
                _store.put(id, bytes, version);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void flushEvents() throws ZoieException {
        try {
            _store.commit();
        } catch (Exception e) {
            throw new ZoieException(e.getMessage(), e);
        }
    }

    @Override
    public String getVersion() {
        return _store.getVersion();
    }

    @Override
    public Comparator<String> getVersionComparator() {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void start() {
        try {
            _store.open();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public void stop() {
        try {
            _store.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

}
