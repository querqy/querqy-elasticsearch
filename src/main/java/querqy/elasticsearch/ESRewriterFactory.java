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
package querqy.elasticsearch;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.shard.IndexShard;
import querqy.elasticsearch.rewriterstore.LoadRewriterConfig;
import querqy.rewrite.RewriterFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class ESRewriterFactory {

    private static final Set<String> ALLOWED_CLASSES = loadAllowedClasses();

    private static Set<String> loadAllowedClasses() {
        final String spiFile = "META-INF/services/" + ESRewriterFactory.class.getName();
        final ClassLoader cl = ESRewriterFactory.class.getClassLoader();
        final Set<String> allowed = new HashSet<>();
        try {
            final Enumeration<URL> resources = cl.getResources(spiFile);
            while (resources.hasMoreElements()) {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(resources.nextElement().openStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        final int commentIdx = line.indexOf('#');
                        if (commentIdx >= 0) {
                            line = line.substring(0, commentIdx);
                        }
                        line = line.trim();
                        if (!line.isEmpty()) {
                            Class.forName(line, false, cl).asSubclass(ESRewriterFactory.class);
                            allowed.add(line);
                        }
                    }
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException("Failed to load ESRewriterFactory service providers", e);
        }
        return Collections.unmodifiableSet(allowed);
    }

    private static void checkClassAllowed(final String className) {
        if (!ALLOWED_CLASSES.contains(className)) {
            throw new IllegalArgumentException(
                    "Class is not a registered ESRewriterFactory service provider: " + className);
        }
    }

    protected final String rewriterId;

    protected ESRewriterFactory(final String rewriterId) {
        this.rewriterId = rewriterId;
    }

    public abstract void configure(final Map<String, Object> config) throws ElasticsearchException;

    public abstract List<String> validateConfiguration(final Map<String, Object> config);

    public abstract RewriterFactory createRewriterFactory(final IndexShard indexShard) throws ElasticsearchException;

    public String getRewriterId() {
        return rewriterId;
    }

    public static ESRewriterFactory loadConfiguredInstance(final LoadRewriterConfig instanceDescription) {

        final String classField = instanceDescription.getRewriterClassName();
        if (classField == null) {
            throw new IllegalArgumentException("Property not found: " + instanceDescription
                    .getConfigMapping().getRewriterClassNameProperty());
        }

        final String className = classField.trim();
        if (className.isEmpty()) {
            throw new IllegalArgumentException("Class name expected in property: " + instanceDescription
                    .getConfigMapping().getRewriterClassNameProperty());
        }

        checkClassAllowed(className);

        final ESRewriterFactory factory = builder().rewriterId(instanceDescription.getRewriterId())
                .className(className).loadFactory();

        factory.configure(instanceDescription.getConfig());
        return factory;


    }

    public static ESRewriterFactory loadInstance(final String rewriterId, final Map<String, Object> instanceDesc,
                                                 final String argName) {

        final String classField = (String) instanceDesc.get(argName);
        if (classField == null) {
            throw new IllegalArgumentException("Property not found: " + argName);
        }

        final String className = classField.trim();
        if (className.isEmpty()) {
            throw new IllegalArgumentException("Class name expected in property: " + argName);
        }

        checkClassAllowed(className);

        return builder()
                .rewriterId(rewriterId)
                .className(className)
                .loadFactory();

    }

    public static ESRewriterFactoryBuilder builder() {
        return new ESRewriterFactoryBuilder();
    }

    public static class ESRewriterFactoryBuilder {
        private String rewriterId;
        private String className;

        private ESRewriterFactoryBuilder() {}

        public ESRewriterFactoryBuilder rewriterId(final String rewriterId) {
            this.rewriterId = rewriterId;
            return this;
        }

        public ESRewriterFactoryBuilder className(final String className) {
            this.className = className;
            return this;
        }

        public ESRewriterFactory loadFactory() {
            Objects.requireNonNull(rewriterId);
            Objects.requireNonNull(className);
            try {
                Class<?> rewriterClass = Class.forName(className);
                if (!ESRewriterFactory.class.isAssignableFrom(rewriterClass)) {
                    throw new IllegalArgumentException("Class must implement " + ESRewriterFactory.class.getName());
                }
                return (ESRewriterFactory) rewriterClass.getDeclaredConstructor(String.class).newInstance(rewriterId);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

}
