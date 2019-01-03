package proj.zoie.mbean;

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

import java.util.Date;

import proj.zoie.api.indexing.IndexingEventListener;
import proj.zoie.impl.indexing.IndexUpdatedEvent;
import proj.zoie.impl.indexing.ZoieSystem;

public class ZoieIndexingStatusAdmin implements ZoieIndexingStatusAdminMBean, IndexingEventListener {
    private final ZoieSystem<?, ?> _zoieSystem;
    private long _endTime;
    private long _startTime;
    private int _leftOver;
    private int _size;
    private long _totalTime;
    private int _totalSize;
    private String _diskVersion;

    public ZoieIndexingStatusAdmin(ZoieSystem<?, ?> zoieSystem) {
        _zoieSystem = zoieSystem;
        _zoieSystem.addIndexingEventListener(this);
        _startTime = 0L;
        _endTime = 0L;
        _leftOver = 0;
        _size = 0;
        _totalSize = 0;
        _totalTime = 0;
        _diskVersion = null;
    }

    @Override
    public long getAverageIndexingBatchDuration() {
        return _totalSize == 0 ? 0 : _totalTime / _totalSize;
    }

    @Override
    public long getLastIndexingBatchDuration() {
        return _endTime - _startTime;
    }

    @Override
    public int getLastIndexingBatchLeftOver() {
        return _leftOver;
    }

    @Override
    public int getLastIndexingBatchSize() {
        return _size;
    }

    @Override
    public Date getLastIndexingEndTime() {
        return new Date(_endTime);
    }

    @Override
    public void resetAverage() {
        _totalSize = 0;
        _totalTime = 0;
    }

    @Override
    public void handleIndexingEvent(IndexingEvent evt) {
        // only interested in IndexUpdateEvent
        if (evt instanceof IndexUpdatedEvent) {
            IndexUpdatedEvent updateEvt = (IndexUpdatedEvent) evt;
            _startTime = updateEvt.getStartIndexingTime();
            _endTime = updateEvt.getEndIndexingTime();
            _leftOver = updateEvt.getNumDocsLeftInQueue();
            _size = updateEvt.getNumDocsIndexed();
            _totalSize += _size;
            _totalTime += (_endTime - _startTime);
        }
    }

    @Override
    public void handleUpdatedDiskVersion(String version) {
        _diskVersion = version;
    }

    @Override
    public String getCurrentDiskVersion() {
        return _diskVersion;
    }
}
