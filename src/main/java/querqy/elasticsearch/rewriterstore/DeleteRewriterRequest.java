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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class DeleteRewriterRequest extends ActionRequest {

    private final String rewriterId;

    public DeleteRewriterRequest(final StreamInput in) throws IOException {
        super(in);
        rewriterId = in.readString();
    }

    public DeleteRewriterRequest(final String rewriterId) {
        super();
        if (rewriterId == null) {
            throw new ElasticsearchParseException("rewriterId must not be null");
        }
        this.rewriterId = rewriterId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(rewriterId);
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public String getRewriterId() {
        return rewriterId;
    }


}
