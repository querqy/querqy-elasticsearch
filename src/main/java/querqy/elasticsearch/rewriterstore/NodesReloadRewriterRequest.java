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

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;

public class NodesReloadRewriterRequest extends BaseNodesRequest {

    private final String rewriterId;

    public NodesReloadRewriterRequest(final String rewriterId, final String... nodesIds) {
        super(nodesIds);
        this.rewriterId = rewriterId;
    }

    public NodeRequest newNodeRequest() {
        return new NodeRequest(rewriterId);
    }

    public String getRewriterId() {
        return rewriterId;
    }


    public static class NodeRequest extends AbstractTransportRequest {

        String rewriterId;

        public NodeRequest(final StreamInput in) throws IOException {
            super(in);
            rewriterId = in.readString();
        }

        public NodeRequest(final String rewriterId) {
            super();
            this.rewriterId = rewriterId;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(rewriterId);
        }

        public String getRewriterId() {
            return rewriterId;
        }

    }
}
