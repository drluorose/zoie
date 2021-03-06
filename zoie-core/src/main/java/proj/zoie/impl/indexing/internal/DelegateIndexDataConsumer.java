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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;


import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import proj.zoie.api.DataConsumer;
import proj.zoie.api.ZoieException;
import proj.zoie.api.ZoieHealth;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;

@Slf4j
public class DelegateIndexDataConsumer<D> implements DataConsumer<D> {
    private final DataConsumer<ZoieIndexable> _diskConsumer;
    private final DataConsumer<ZoieIndexable> _ramConsumer;
    private final ZoieIndexableInterpreter<D> _interpreter;

    public DelegateIndexDataConsumer(DataConsumer<ZoieIndexable> diskConsumer,
                                     DataConsumer<ZoieIndexable> ramConsumer, ZoieIndexableInterpreter<D> interpreter) {
        _diskConsumer = diskConsumer;
        _ramConsumer = ramConsumer;
        _interpreter = interpreter;
    }

    @Override
    public void consume(Collection<DataEvent<D>> data) throws ZoieException {
        if (data != null) {
            // PriorityQueue<DataEvent<ZoieIndexable>> indexableList = new
            // PriorityQueue<DataEvent<ZoieIndexable>>(data.size(), DataEvent.getComparator());
            ArrayList<DataEvent<ZoieIndexable>> indexableList = new ArrayList<DataEvent<ZoieIndexable>>(
                    data.size());
            Iterator<DataEvent<D>> iter = data.iterator();
            while (iter.hasNext()) {
                try {
                    DataEvent<D> event = iter.next();
                    ZoieIndexable indexable = ((ZoieIndexableInterpreter<D>) _interpreter)
                            .convertAndInterpret(event.getData());

                    DataEvent<ZoieIndexable> newEvent = new DataEvent<ZoieIndexable>(indexable,
                            event.getVersion(), event.isDelete());
                    indexableList.add(newEvent);
                } catch (Exception e) {
                    ZoieHealth.setFatal();
                    log.error(e.getMessage(), e);
                }
            }

            if (_diskConsumer != null) {
                synchronized (_diskConsumer) // this blocks the batch disk loader thread while indexing to
                // RAM
                {
                    if (_ramConsumer != null) {
                        ArrayList<DataEvent<ZoieIndexable>> ramList = new ArrayList<DataEvent<ZoieIndexable>>(
                                indexableList);
                        _ramConsumer.consume(ramList);
                    }
                    _diskConsumer.consume(indexableList);
                }
            } else {
                if (_ramConsumer != null) {
                    _ramConsumer.consume(indexableList);
                }
            }
        }
    }

    @Override
    public String getVersion() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Comparator<String> getVersionComparator() {
        throw new UnsupportedOperationException();
    }
}
