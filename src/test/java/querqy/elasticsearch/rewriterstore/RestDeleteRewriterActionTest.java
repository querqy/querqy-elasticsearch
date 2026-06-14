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
import static org.mockito.Mockito.mock;
import static querqy.elasticsearch.rewriterstore.RestDeleteRewriterAction.PARAM_REWRITER_ID;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class RestDeleteRewriterActionTest {

    @Test(expected = IllegalArgumentException.class)
    public void testThatNullRewriterIdIsRejected() {

        final NodeClient client = mock(NodeClient.class);
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(null)
                .withParams(Collections.emptyMap()).build();

        new RestDeleteRewriterAction().prepareRequest(restRequest, client);

    }

    @Test(expected = IllegalArgumentException.class)
    public void testThatEmptyRewriterIdIsRejected() {

        final NodeClient client = mock(NodeClient.class);
        final Map<String, String> params = new HashMap<>();
        params.put(PARAM_REWRITER_ID, " ");
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(null)
                .withParams(params).build();

        new RestDeleteRewriterAction().prepareRequest(restRequest, client);

    }

    @Test
    public void testThatRequestIsParsed() {

        final NodeClient client = mock(NodeClient.class);

        final Map<String, String> params = new HashMap<>();
        params.put(RestPutRewriterAction.PARAM_REWRITER_ID, "rewriter11");

        final FakeRestRequest restRequest = new FakeRestRequest.Builder(null)
                .withParams(params)
                .build();
        final RestDeleteRewriterAction.DeleteRewriterRequestBuilder requestBuilder
                = new RestDeleteRewriterAction().createRequestBuilder(restRequest, client);

        final DeleteRewriterRequest deleteRewriterRequest = requestBuilder.request();
        assertNotNull(deleteRewriterRequest);

        assertEquals("rewriter11", deleteRewriterRequest.getRewriterId());

    }


}