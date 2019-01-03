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

import proj.zoie.impl.indexing.StreamDataProvider;

public class DataProviderAdmin implements DataProviderAdminMBean {
    private final StreamDataProvider<?> _dataProvider;

    public DataProviderAdmin(StreamDataProvider<?> dataProvider) {
        _dataProvider = dataProvider;
    }

    @Override
    public int getBatchSize() {
        return _dataProvider.getBatchSize();
    }

    @Override
    public long getEventCount() {
        return _dataProvider.getEventCount();
    }

    @Override
    public long getEventsPerMinute() {
        return _dataProvider.getEventsPerMinute();
    }

    @Override
    public long getMaxEventsPerMinute() {
        return _dataProvider.getMaxEventsPerMinute();
    }

    @Override
    public void setMaxEventsPerMinute(long maxEventsPerMinute) {
        _dataProvider.setMaxEventsPerMinute(maxEventsPerMinute);
    }

    @Override
    public String getStatus() {
        return _dataProvider.getStatus();
    }

    @Override
    public void pause() {
        _dataProvider.pause();
    }

    @Override
    public void resume() {
        _dataProvider.resume();
    }

    @Override
    public void setBatchSize(int batchSize) {
        _dataProvider.setBatchSize(batchSize);
    }

    @Override
    public void start() {
        _dataProvider.start();
    }

    @Override
    public void stop() {
        _dataProvider.stop();
    }

}
