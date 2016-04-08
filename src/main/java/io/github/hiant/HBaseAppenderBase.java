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

import ch.qos.logback.core.UnsynchronizedAppenderBase;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author heihuwudi@gmail.com Created By: 2016.1.22.10:49
 */
public abstract class HBaseAppenderBase<E> extends UnsynchronizedAppenderBase<E> {

    /**
     * Table schema
     */
    static final TableName idTable = TableName.valueOf("logging_id");
    static final TableName eventTable = TableName.valueOf("logging_event");
    static final TableName eventPropertyTable = TableName.valueOf("logging_event_property");
    static final TableName eventExceptionTable = TableName.valueOf("logging_event_exception");
    static final List<String> eventColumns =
            Lists.newArrayList("event_id", "timestmp", "formatted_message", "logger_name", "level_string", "thread_name", "reference_flag", "arg", "caller_filename", "caller_class", "caller_method", "caller_line");
    static final List<String> eventPropertyColumns =
            Lists.newArrayList("event_id", "mapped_key", "mapped_value");
    static final List<String> eventExceptionColumns =
            Lists.newArrayList("event_id", "i", "trace_line");
    /**
     * ColumnFamily index of the table "logging_event"
     */
    static final int TIMESTMP_INDEX = 1;
    static final int FORMATTED_MESSAGE_INDEX = 2;
    static final int LOGGER_NAME_INDEX = 3;
    static final int LEVEL_STRING_INDEX = 4;
    static final int THREAD_NAME_INDEX = 5;
    static final int REFERENCE_FLAG_INDEX = 6;
    static final int ARG_INDEX = 7;
    static final int CALLER_FILENAME_INDEX = 8;
    static final int CALLER_CLASS_INDEX = 9;
    static final int CALLER_METHOD_INDEX = 10;
    static final int CALLER_LINE_INDEX = 11;
    static final ConnectionPool POOL = ConnectionPool.getInstance();
    /**
     * TableName --> ColumnFamily
     */
    static Map<TableName, List<String>> tableSchema = Maps.newHashMap();

    static {
        tableSchema.put(eventTable, eventColumns);
        tableSchema.put(eventPropertyTable, eventPropertyColumns);
        tableSchema.put(eventExceptionTable, eventExceptionColumns);
        tableSchema.put(idTable, Lists.newArrayList("id"));
    }

    protected ConfigurationFactory configuration;

    public ConfigurationFactory getConfiguration() {
        return configuration;
    }

    public void setConfiguration(ConfigurationFactory configuration) {
        this.configuration = configuration;
    }

    @Override
    public void start() {
        if (configuration == null) {
            throw new IllegalStateException("HBaseAppender cannot function without a configuration");
        }
        Connection connection = null;
        try {
            connection = POOL.borrowObject(configuration.getConfiguration());
            createTable(connection);
        } catch (Exception e) {
            throw new IllegalStateException("HBaseAppender cannot function without a valid configuration", e);
        } finally {
            if (connection != null) {
                try {
                    POOL.returnObject(configuration.getConfiguration(), connection);
                } catch (IOException ignore) {
                }
            }
        }
        super.start();
    }

    @Override
    protected void append(E eventObject) {
        Connection connection = null;
        try {
            connection = POOL.borrowObject(configuration.getConfiguration());
            long eventId = subAppend(eventObject, connection);
            secondarySubAppend(eventObject, connection, eventId);
        } catch (Throwable throwable) {
            addError("problem appending event", throwable);
        } finally {
            if (connection != null) {
                try {
                    POOL.returnObject(configuration.getConfiguration(), connection);
                } catch (IOException ignore) {
                }
            }
        }
    }

    protected abstract long subAppend(E eventObject, Connection connection) throws Throwable;

    protected abstract void secondarySubAppend(E eventObject, Connection connection, long eventId) throws Throwable;

    @Override
    public void stop() {
        super.stop();
    }

    private void createTable(Connection connection) throws IOException {
        Admin admin = connection.getAdmin();
        for (Map.Entry<TableName, List<String>> entry : tableSchema.entrySet()) {
            TableName tableName = entry.getKey();
            if (!admin.tableExists(tableName)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                for (String column : entry.getValue()) {
                    tableDescriptor.addFamily(new HColumnDescriptor(column));
                }
                admin.createTable(tableDescriptor);
            }
        }
    }
}
