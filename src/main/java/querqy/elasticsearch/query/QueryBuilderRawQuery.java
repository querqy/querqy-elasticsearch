/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright 2020 Querqy for Elasticsearch Contributors
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

import org.elasticsearch.index.query.QueryBuilder;
import querqy.model.BooleanParent;
import querqy.model.RawQuery;

public class QueryBuilderRawQuery extends RawQuery {

    private final QueryBuilder queryBuilder;

    public QueryBuilderRawQuery(final BooleanParent parent, final QueryBuilder queryBuilder, final Occur occur,
                                final boolean isGenerated) {

        super(parent, occur, isGenerated);

        this.queryBuilder = queryBuilder;
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    @Override
    public RawQuery clone(final BooleanParent newParent) {
        return clone(newParent, this.generated);
    }

    @Override
    public RawQuery clone(final BooleanParent newParent, final boolean generated) {
        return new QueryBuilderRawQuery(newParent, queryBuilder, occur, generated);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((queryBuilder == null) ? 0 : queryBuilder.hashCode());
        result = prime * result
                + ((occur == null) ? 0 : occur.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QueryBuilderRawQuery other = (QueryBuilderRawQuery) obj;
        if (queryBuilder == null) {
            if (other.queryBuilder != null)
                return false;
        } else if (!queryBuilder.equals(other.queryBuilder))
            return false;

        return occur == other.occur;
    }

    @Override
    public String toString() {
        return "RawQuery [queryString=" + queryBuilder + "]";
    }

}
