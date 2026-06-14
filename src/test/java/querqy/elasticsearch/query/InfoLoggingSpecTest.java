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
package querqy.elasticsearch.query;

import static org.junit.Assert.*;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Test;
import querqy.elasticsearch.infologging.LogPayloadType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class InfoLoggingSpecTest {

    @Test
    public void testThatRewriterIdIsDefaultPayloadType() {
        assertEquals(LogPayloadType.NONE, new InfoLoggingSpec().getPayloadType());
    }

    @Test
    public void testEqualsHashCode() {
        InfoLoggingSpec spec1 = new InfoLoggingSpec();
        assertFalse(spec1.equals(null));
        InfoLoggingSpec spec2 = new InfoLoggingSpec();

        assertEquals(spec1, spec2);
        assertEquals(spec1.hashCode(), spec2.hashCode());

        spec1.setId("idx");
        assertNotEquals(spec1, spec2);
        spec2.setId("idx");
        assertEquals(spec1, spec2);
        assertEquals(spec1.hashCode(), spec2.hashCode());
        spec2.setPayloadType("REWRITER_ID");
        assertNotEquals(spec1, spec2);
        spec1.setPayloadType("REWRITER_ID");
        assertEquals(spec1, spec2);
        assertEquals(spec1.hashCode(), spec2.hashCode());

        // logged property must not be considered
        spec1.setLogged(true);
        spec2.setLogged(false);

        assertEquals(spec1, spec2);
        assertEquals(spec1.hashCode(), spec2.hashCode());

    }


    @Test
    public void testWriteReadJson() throws IOException {

        final InfoLoggingSpec spec =  new InfoLoggingSpec();
        spec.setId("ID1");
        spec.setPayloadType("REWRITER_ID");
        spec.setLogged(true);

        ByteArrayOutputStream os = new ByteArrayOutputStream();

        final XContentBuilder xContentBuilder = new XContentBuilder(JsonXContent.jsonXContent, os);
        spec.toXContent(xContentBuilder, null);
        xContentBuilder.flush();
        xContentBuilder.close();

        final InfoLoggingSpec spec2 = fromJsonInnerObject(os.toByteArray());

        assertEquals(spec, spec2);
        assertEquals(spec.isLogged(), spec2.isLogged());
    }

    @Test
    public void testWriteReadStream() throws IOException {
        final InfoLoggingSpec spec =  new InfoLoggingSpec();
        spec.setId("ID2");
        spec.setPayloadType("DETAIL");
        spec.setLogged(true);

        final BytesStreamOutput out = new BytesStreamOutput();
        spec.writeTo(out);
        out.flush();
        out.close();

        final InfoLoggingSpec spec2 = new InfoLoggingSpec(out.bytes().streamInput());

        assertEquals(spec, spec2);
        assertEquals(spec.isLogged(), spec2.isLogged());

    }

    private InfoLoggingSpec fromJsonInnerObject(final byte[] bytes) throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, bytes);
        return InfoLoggingSpec.PARSER.parse(parser, null);
    }

}