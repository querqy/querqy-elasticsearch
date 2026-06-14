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

import static querqy.elasticsearch.rewriterstore.Constants.QUERQY_INDEX_NAME;
import static org.elasticsearch.action.ActionListener.wrap;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportDeleteRewriterAction  extends HandledTransportAction<DeleteRewriterRequest, DeleteRewriterResponse> {

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public TransportDeleteRewriterAction(final TransportService transportService, final ActionFilters actionFilters,
                                      final ClusterService clusterService, final Client client) {
        super(DeleteRewriterAction.NAME, false, transportService, actionFilters, DeleteRewriterRequest::new,
                clusterService.threadPool().executor(ThreadPool.Names.MANAGEMENT));
        this.clusterService = clusterService;
        this.client = client;
    }
    @Override
    protected void doExecute(final Task task, final DeleteRewriterRequest request,
                             final ActionListener<DeleteRewriterResponse> listener) {

        final DeleteRequestBuilder deleteRequest = client.prepareDelete(QUERQY_INDEX_NAME, request.getRewriterId());

        deleteRequest.execute(new ActionListener<DeleteResponse>() {

            @Override
            public void onResponse(final DeleteResponse deleteResponse) {

                // TODO: exit if response status code is 404 (though is shouldn't harm to clear the rewriter from cache
                // regardless)

                client.execute(NodesClearRewriterCacheAction.INSTANCE,
                        new NodesClearRewriterCacheRequest(request.getRewriterId()),
                        wrap(
                                (clearResponse) -> listener.onResponse(new DeleteRewriterResponse(deleteResponse,
                                        clearResponse)),
                                listener::onFailure
                        ));
            }

            @Override
            public void onFailure(final Exception e) {
                listener.onFailure(e);
            }
        });


    }
}
