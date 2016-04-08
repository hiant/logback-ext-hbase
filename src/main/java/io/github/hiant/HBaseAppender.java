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

import ch.qos.logback.classic.db.DBHelper;
import ch.qos.logback.classic.spi.*;
import ch.qos.logback.core.CoreConstants;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author heihuwudi@gmail.com Created By: 2016.1.22.11:02
 */
public class HBaseAppender extends HBaseAppenderBase<ILoggingEvent> {
    static final byte[] ID = toBytes("id");
    static final byte[] BLANK = toBytes("");
    static final StackTraceElement EMPTY_CALLER_DATA = CallerData.naInstance();

    static byte[] toBytes(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof Number) {
            return Bytes.toBytes(o + "");
        }
        return Bytes.toBytes(o.toString());
    }

    @Override
    protected long subAppend(ILoggingEvent event, Connection connection) {
        long eventId = -1L;
        try (Table id = connection.getTable(idTable)) {
            eventId = id.incrementColumnValue(ID, ID, BLANK, 1);
            try (Table table = connection.getTable(eventTable)) {
                Put put = new Put(toBytes(eventId));
                bindLoggingEventWithPut(put, event);
                bindLoggingEventArgumentsWithPut(put, event.getArgumentArray());

                // This is expensive... should we do it every time?
                bindCallerDataWithPut(put, event.getCallerData());
                table.put(put);
            } catch (IOException e) {
                addWarn("Failed to insert loggingEvent", e);
            }
        } catch (IOException e) {
            addWarn("Failed to get loggingId", e);
        }
        return eventId;
    }

    @Override
    protected void secondarySubAppend(ILoggingEvent event, Connection connection, long eventId) throws Throwable {
        Map<String, String> mergedMap = mergePropertyMaps(event);
        insertProperties(mergedMap, connection, eventId);

        if (event.getThrowableProxy() != null) {
            insertThrowable(event.getThrowableProxy(), connection, eventId);
        }
    }

    Map<String, String> mergePropertyMaps(ILoggingEvent event) {
        Map<String, String> mergedMap = new HashMap<String, String>();
        // we add the context properties first, then the event properties, since
        // we consider that event-specific properties should have priority over
        // context-wide properties.
        Map<String, String> loggerContextMap = event.getLoggerContextVO().getPropertyMap();
        Map<String, String> mdcMap = event.getMDCPropertyMap();
        if (loggerContextMap != null) {
            mergedMap.putAll(loggerContextMap);
        }
        if (mdcMap != null) {
            mergedMap.putAll(mdcMap);
        }

        return mergedMap;
    }

    protected void insertProperties(Map<String, String> mergedMap, Connection connection, long eventId) throws IOException {
        if (mergedMap.size() > 0) {
            try (Table table = connection.getTable(eventPropertyTable)) {
                List<Put> list = Lists.newArrayList();
                for (Map.Entry<String, String> entry : mergedMap.entrySet()) {
                    Put put = new Put(toBytes(eventId));
                    put.addColumn(toBytes("mapped_key"), null, toBytes(entry.getKey()));
                    put.addColumn(toBytes("mapped_value"), null, toBytes(entry.getValue()));
                    list.add(put);
                }
                table.put(list);
            }
        }
    }

    protected void insertThrowable(IThrowableProxy tp, Connection connection, long eventId) throws IOException {
        try (Table table = connection.getTable(eventExceptionTable)) {
            List<Put> list = Lists.newArrayList();
            short baseIndex = 0;
            while (tp != null) {
                baseIndex = buildExceptionStatement(tp, baseIndex, list, eventId);
                tp = tp.getCause();
            }
            table.put(list);
        }
    }

    void bindLoggingEventWithPut(Put put, ILoggingEvent event) {
        put.addColumn(toBytes(eventColumns.get(TIMESTMP_INDEX)), BLANK, toBytes(event.getTimeStamp()));
        put.addColumn(toBytes(eventColumns.get(FORMATTED_MESSAGE_INDEX)), BLANK, toBytes(event.getFormattedMessage()));
        put.addColumn(toBytes(eventColumns.get(LOGGER_NAME_INDEX)), BLANK, toBytes(event.getLoggerName()));
        put.addColumn(toBytes(eventColumns.get(LEVEL_STRING_INDEX)), BLANK, toBytes(event.getLevel().toString()));
        put.addColumn(toBytes(eventColumns.get(THREAD_NAME_INDEX)), BLANK, toBytes(event.getThreadName()));
        put.addColumn(toBytes(eventColumns.get(REFERENCE_FLAG_INDEX)), BLANK, toBytes(DBHelper.computeReferenceMask(event)));
    }

    void bindLoggingEventArgumentsWithPut(Put put, Object[] argArray) {
        int arrayLen = argArray != null ? argArray.length : 0;
        for (int i = 0; i < arrayLen; i++) {
            put.addColumn(toBytes(eventColumns.get(ARG_INDEX)), toBytes(i), toBytes(argArray[i]));
        }
    }

    void bindCallerDataWithPut(Put put, StackTraceElement[] callerDataArray) {
        StackTraceElement caller = extractFirstCaller(callerDataArray);
        put.addColumn(toBytes(eventColumns.get(CALLER_FILENAME_INDEX)), BLANK, toBytes(caller.getFileName()));
        put.addColumn(toBytes(eventColumns.get(CALLER_CLASS_INDEX)), BLANK, toBytes(caller.getClassName()));
        put.addColumn(toBytes(eventColumns.get(CALLER_METHOD_INDEX)), BLANK, toBytes(caller.getMethodName()));
        put.addColumn(toBytes(eventColumns.get(CALLER_LINE_INDEX)), BLANK, toBytes(caller.getLineNumber()));
    }

    private StackTraceElement extractFirstCaller(StackTraceElement[] callerDataArray) {
        StackTraceElement caller = EMPTY_CALLER_DATA;
        if (hasAtLeastOneNonNullElement(callerDataArray))
            caller = callerDataArray[0];
        return caller;
    }

    private boolean hasAtLeastOneNonNullElement(StackTraceElement[] callerDataArray) {
        return callerDataArray != null && callerDataArray.length > 0 && callerDataArray[0] != null;
    }

    short buildExceptionStatement(IThrowableProxy tp, short baseIndex, List<Put> list, long eventId) {

        StringBuilder buf = new StringBuilder();
        ThrowableProxyUtil.subjoinFirstLine(buf, tp);
        updateExceptionStatement(list, buf.toString(), baseIndex++, eventId);

        int commonFrames = tp.getCommonFrames();
        StackTraceElementProxy[] stepArray = tp.getStackTraceElementProxyArray();
        for (int i = 0; i < stepArray.length - commonFrames; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append(CoreConstants.TAB);
            ThrowableProxyUtil.subjoinSTEP(sb, stepArray[i]);
            updateExceptionStatement(list, sb.toString(), baseIndex++, eventId);
        }

        if (commonFrames > 0) {
            StringBuilder sb = new StringBuilder(128);
            sb.append(CoreConstants.TAB).append("... ").append(commonFrames).append(" common frames omitted");
            updateExceptionStatement(list, sb.toString(), baseIndex++, eventId);
        }
        return baseIndex;
    }

    /**
     * Add an exception statement
     */
    void updateExceptionStatement(List<Put> list, String txt, short i, long eventId) {
        Put put = new Put(toBytes(eventId));
        put.addColumn(toBytes("i"), BLANK, toBytes(i));
        put.addColumn(toBytes("trace_line"), BLANK, toBytes(txt));
        list.add(put);
    }
}
