/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright 2019 Querqy for Elasticsearch Contributors
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
package querqy.elasticsearch.rewriterstore;

import static org.junit.Assert.*;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.junit.Test;

import java.io.IOException;

public class DeleteRewriterRequestTest {

    @Test(expected = ElasticsearchParseException.class)
    public void testNullRewriterIsNotAccepted() {
        new DeleteRewriterRequest((String) null);
    }

    @Test
    public void testValidate() {
        final DeleteRewriterRequest validRequest = new DeleteRewriterRequest("r27");
        assertNull(validRequest.validate());
    }

    @Test
    public void testStreamSerialization() throws IOException {
        final DeleteRewriterRequest request1 = new DeleteRewriterRequest("r31");
        final BytesStreamOutput output = new BytesStreamOutput();
        request1.writeTo(output);
        output.flush();

        final DeleteRewriterRequest request2 = new DeleteRewriterRequest(output.bytes().streamInput());
        assertEquals(request1.getRewriterId(), request2.getRewriterId());
    }

}