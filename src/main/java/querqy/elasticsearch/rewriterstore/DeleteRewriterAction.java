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

import org.elasticsearch.action.ActionType;

public class DeleteRewriterAction extends ActionType<DeleteRewriterResponse> {

    public static final String NAME = "cluster:admin/querqy/rewriter/delete";
    public static final DeleteRewriterAction INSTANCE = new DeleteRewriterAction(NAME);

    /**
     * @param name The name of the action, must be unique across actions.
     */
    protected DeleteRewriterAction(final String name) {
        super(name);
    }

}
