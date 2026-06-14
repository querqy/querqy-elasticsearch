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

import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static querqy.elasticsearch.rewriterstore.RestPutRewriterAction.PARAM_REWRITER_ID;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class RestPutRewriterActionTest {

    @Test(expected = IllegalArgumentException.class)
    public void testThatNullRewriterIdIsRejected() {

        final NodeClient client = mock(NodeClient.class);
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(null)
                .withParams(Collections.emptyMap()).build();

        new RestPutRewriterAction().prepareRequest(restRequest, client);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testThatEmptyRewriterIdIsRejected() {

        final NodeClient client = mock(NodeClient.class);
        final Map<String, String> params = new HashMap<>();
        params.put(PARAM_REWRITER_ID, " ");
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(null)
                .withParams(params).build();

        new RestPutRewriterAction().prepareRequest(restRequest, client);

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testThatRequestIsParsed() throws Exception{
        final NodeClient client = mock(NodeClient.class);

        final Map<String, String> params = new HashMap<>();
        params.put(PARAM_REWRITER_ID, "rewriter1");

        final ByteBuffer buffer = ByteBuffer.wrap("{\"config\": {\"name\":42}}".getBytes());
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(null)
                .withParams(params)
                .withContent(BytesReference.fromByteBuffers(new ByteBuffer[] {buffer}), XContentType.JSON)
                .build();
        final RestPutRewriterAction.PutRewriterRequestBuilder requestBuilder = new RestPutRewriterAction()
                .createRequestBuilder(restRequest, client);

        final PutRewriterRequest putRewriterRequest = requestBuilder.request();
        assertNotNull(putRewriterRequest);

        assertEquals("rewriter1", putRewriterRequest.getRewriterId());

        final Map<String, Object> content = putRewriterRequest.getContent();
        assertNotNull(content);
        assertThat((Map<String, Object>) content.get("config"), hasEntry("name", 42));

    }

}