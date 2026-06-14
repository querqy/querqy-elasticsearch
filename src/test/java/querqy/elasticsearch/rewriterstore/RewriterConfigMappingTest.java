/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * Copyright 2022 Querqy for Elasticsearch Contributors
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

import static org.hamcrest.Matchers.everyItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.lucene.util.BytesRef;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

public class RewriterConfigMappingTest {


    @Test
    public void testStringToSourceValue() {
        String s = "123456789ä";
        if (new BytesRef(s).length != 11) {
            throw new IllegalStateException("Test assumptions are wrong: unexpected encoding size: " +
                    new BytesRef(s).length);
        }
        assertEquals(s, RewriterConfigMapping.stringToSourceValue(s, 20));
        assertEquals(s, RewriterConfigMapping.stringToSourceValue(s, 11));

        final Object o1 = RewriterConfigMapping.stringToSourceValue(s, 10);
        assertTrue(o1.getClass().isArray());
        final String[] splits = (String[]) o1;
        assertEquals(2, splits.length);
        assertEquals(s, splits[0] + splits[1]);
        assertTrue(new BytesRef(splits[0]).length <= 10);
        assertTrue(new BytesRef(splits[1]).length <= 10);

        final Object o2 = RewriterConfigMapping.stringToSourceValue(s, 3);
        assertTrue(o2.getClass().isArray());
        final String[] arr = (String[]) o2;
        assertThat(Arrays.stream(arr).map(BytesRef::new).map(bytesRef -> bytesRef.length).collect(Collectors.toList()),
                everyItem(Matchers.lessThanOrEqualTo(3)));
        assertEquals(s, String.join("", arr));

    }

    @Test(expected = IllegalArgumentException.class)
    public void testStringToSourceValueWithIllegalLimit() {
        RewriterConfigMapping.stringToSourceValue("12345", 2);
    }

}
