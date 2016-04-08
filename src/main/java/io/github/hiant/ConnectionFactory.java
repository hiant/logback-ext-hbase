/*
 * Copyright 2016 heihuwudi@gmail.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.hiant;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * @author heihuwudi@gmail.com Created By: 2016.1.28.11:24
 */
public class ConnectionFactory extends BaseKeyedPooledObjectFactory<Configuration, Connection> {

    @Override
    public Connection create(Configuration key) throws Exception {
        return org.apache.hadoop.hbase.client.ConnectionFactory.createConnection(key);
    }

    @Override
    public PooledObject<Connection> wrap(Connection value) {
        return new DefaultPooledObject<Connection>(value);
    }

    @Override
    public void destroyObject(Configuration key, PooledObject<Connection> p) throws Exception {
        if (p != null) {
            try {
                p.getObject().close();
            } catch (IOException ignore) {
            }
        }
    }

    @Override
    public boolean validateObject(Configuration key, PooledObject<Connection> p) {
        return p != null && !p.getObject().isClosed();
    }
}
