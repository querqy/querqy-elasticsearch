/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright 2021 Querqy for Elasticsearch Contributors
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
package querqy.elasticsearch.infologging;

import org.apache.logging.log4j.core.util.JsonUtils;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.util.StringBuilderFormattable;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class LogMessage implements Message, StringBuilderFormattable {

    private final Map<String, List<Object>> messages;
    private final String id;

    public LogMessage(final String id, final Map<String, List<Object>> messages) {
        this.id = id;
        this.messages = messages;
    }


    @Override
    public String getFormattedMessage() {
        final StringBuilder builder = new StringBuilder();
        formatTo(builder);
        return builder.toString();
    }

    @Override
    public String getFormat() {
        return "";
    }

    @Override
    public Object[] getParameters() {
        return null;
    }

    @Override
    public Throwable getThrowable() {
        return null;
    }

    @Override
    public void formatTo(final StringBuilder buffer) {
        buffer.append('{');
        if (id != null) {
            buffer.append("\"id\":\"");
            JsonUtils.quoteAsString(id, buffer);
            buffer.append("\",");
        }
        buffer.append("\"msg\":");
        formatMessagesTo(messages, buffer);
        buffer.append('}');
    }

    public abstract void formatMessagesTo(Map<String, List<Object>> messages, final StringBuilder buffer);

    public static class DetailLogMessage extends LogMessage {

        public DetailLogMessage(final String id, final Map<String, List<Object>> messages) {
            super(id, messages);
        }

        @Override
        public void formatMessagesTo(Map<String, List<Object>> messages, final StringBuilder buffer) {
            appendValue(messages, buffer);
        }
    }

    public static class RewriterIdLogMessage extends LogMessage {

        public RewriterIdLogMessage(final String id, final Map<String, List<Object>> messages) {
            super(id, messages);
        }

        @Override
        public void formatMessagesTo(Map<String, List<Object>> messages, final StringBuilder buffer) {
            appendValue(messages.keySet(), buffer);
        }
    }

    public static void appendValue(final Object value, final StringBuilder buffer) {
        if (value == null) {
            buffer.append("null");
        } else if (value instanceof Map) {
            final Map<String, ?> map = (Map<String, ?>) value;
            buffer.append('{');
            boolean first = true;
            for (final Map.Entry<String, ?> entry : map.entrySet()) {
                if (first) {
                    first = false;
                } else {
                    buffer.append(',');
                }
                buffer.append('"');
                JsonUtils.quoteAsString(entry.getKey(), buffer);
                buffer.append("\":");
                appendValue(entry.getValue(), buffer);
            }
            buffer.append('}');
        } else if (value instanceof Collection) {
            final Collection<?> coll = (Collection<?>) value;
            buffer.append('[');
            boolean first = true;
            for (final Object element: coll) {
                if (first) {
                    first = false;
                } else {
                    buffer.append(',');
                }
                appendValue(element, buffer);
            }
            buffer.append(']');
        } else if (value instanceof String) {
            buffer.append('"');
            JsonUtils.quoteAsString((String) value, buffer);
            buffer.append('"');
        } else if (value instanceof Number || value instanceof Boolean) {
            buffer.append(value.toString());
        } else {
            // whatever it is
            buffer.append('"');
            JsonUtils.quoteAsString(value.toString(), buffer);
            buffer.append('"');
        }
    }
}
