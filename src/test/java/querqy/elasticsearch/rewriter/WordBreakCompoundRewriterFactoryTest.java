package querqy.elasticsearch.rewriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static querqy.elasticsearch.rewriter.WordBreakCompoundRewriterFactory.*;
import static querqy.elasticsearch.rewriter.WordBreakCompoundRewriterFactory.DEFAULT_MAX_COMBINE_LENGTH;
import static querqy.elasticsearch.rewriter.WordBreakCompoundRewriterFactory.DEFAULT_VERIFY_DECOMPOUND_COLLATION;
import static querqy.elasticsearch.rewriter.WordBreakCompoundRewriterFactory.MAX_CHANGES;

import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spell.WordBreakSpellChecker;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import querqy.lucene.contrib.rewrite.WordBreakCompoundRewriter;
import querqy.rewrite.RewriterFactory;
import querqy.trie.TrieMap;

import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class WordBreakCompoundRewriterFactoryTest {

    @Test(expected = IllegalArgumentException.class)
    public void testConfigureRequiresDictionaryField() throws Exception {
        final WordBreakCompoundRewriterFactory factory = new WordBreakCompoundRewriterFactory("r1");
        factory.configure(Collections.emptyMap());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConfigureRequiresNonEmptyDictionaryField() throws Exception {
        final WordBreakCompoundRewriterFactory factory = new WordBreakCompoundRewriterFactory("r1");
        factory.configure(Collections.singletonMap("dictionaryField", " "));
    }


    @Test
    public void testConfigureRequiresDictionaryFieldOnly() throws Exception {
        final WordBreakCompoundRewriterFactory factory = new WordBreakCompoundRewriterFactory("r1");
        factory.configure(Collections.singletonMap("dictionaryField", "f1"));
    }


    @Test
    public void testValidateRequiresDictionaryField() {

        final WordBreakCompoundRewriterFactory factory = new WordBreakCompoundRewriterFactory("r1");
        final List<String> errors1 = factory.validateConfiguration(Collections.emptyMap());
        assertEquals(1, errors1.size());
        assertTrue(errors1.get(0).contains("dictionaryField"));

        final List<String> errors2 = factory.validateConfiguration(Collections.singletonMap("dictionaryField", ""));
        assertEquals(1, errors2.size());
        assertTrue(errors2.get(0).contains("dictionaryField"));

    }


    @Test
    public void testValidateRequiresDictionaryFieldOnly() {
        final WordBreakCompoundRewriterFactory factory = new WordBreakCompoundRewriterFactory("r1");
        final List<String> errors = factory.validateConfiguration(Collections.singletonMap("dictionaryField", "f1"));
        assertTrue(errors == null || errors.isEmpty());
    }

    @Test
    public void testThatDefaultConfigurationIsApplied() {

        final WordBreakCompoundRewriterFactory factory = new WordBreakCompoundRewriterFactory("r1");
        factory.configure(Collections.singletonMap("dictionaryField", "f1"));
        final WordBreakSpellChecker spellChecker = factory.getSpellChecker();
        assertNotNull(spellChecker);
        assertEquals(MAX_CHANGES, spellChecker.getMaxChanges());
        assertEquals(DEFAULT_MAX_COMBINE_LENGTH, spellChecker.getMaxCombineWordLength());
        assertEquals(DEFAULT_MIN_SUGGESTION_FREQ, spellChecker.getMinSuggestionFrequency());
        assertEquals(DEFAULT_MIN_BREAK_LENGTH, spellChecker.getMinBreakWordLength());
        assertEquals(DEFAULT_LOWER_CASE_INPUT, factory.isLowerCaseInput());
        assertEquals(DEFAULT_ALWAYS_ADD_REVERSE_COMPOUNDS, factory.isAlwaysAddReverseCompounds());
        assertEquals(DEFAULT_VERIFY_DECOMPOUND_COLLATION, factory.isVerifyDecompoundCollation());

        assertEquals("f1", factory.getDictionaryField());

    }


    @Test
    public void testThatConfigurationIsApplied() {

        final Map<String, Object> config = new HashMap<>();
        config.put("minSuggestionFreq", 11);
        config.put("maxCombineLength", 22);
        config.put("minBreakLength", 1);
        config.put("dictionaryField", "f2");
        config.put("lowerCaseInput", !DEFAULT_LOWER_CASE_INPUT);
        config.put("alwaysAddReverseCompounds", !DEFAULT_ALWAYS_ADD_REVERSE_COMPOUNDS);
        config.put("reverseCompoundTriggerWords", Arrays.asList("für", "aus"));

        Map<String, Object> decompoundConf = new HashMap<>();
        config.put("decompound", decompoundConf);

        decompoundConf.put("verifyCollation", !DEFAULT_VERIFY_DECOMPOUND_COLLATION);
        decompoundConf.put("maxExpansions", 87);


        final WordBreakCompoundRewriterFactory factory = new WordBreakCompoundRewriterFactory("r1");
        factory.configure(config);
        final WordBreakSpellChecker spellChecker = factory.getSpellChecker();
        assertNotNull(spellChecker);

        assertEquals(22, spellChecker.getMaxCombineWordLength());
        assertEquals(11, spellChecker.getMinSuggestionFrequency());
        assertEquals(1, spellChecker.getMinBreakWordLength());
        assertEquals(87, factory.getMaxDecompoundExpansions());

        assertNotEquals(DEFAULT_LOWER_CASE_INPUT, factory.isLowerCaseInput());
        assertNotEquals(DEFAULT_ALWAYS_ADD_REVERSE_COMPOUNDS, factory.isAlwaysAddReverseCompounds());
        assertNotEquals(DEFAULT_VERIFY_DECOMPOUND_COLLATION, factory.isVerifyDecompoundCollation());

        assertEquals("f2", factory.getDictionaryField());

        final TrieMap<Boolean> words = factory.getReverseCompoundTriggerWords();
        assertNotNull(words);
        assertTrue(words.get("für").getStateForCompleteSequence().isFinal());
        assertTrue(words.get("aus").getStateForCompleteSequence().isFinal());

    }

    @Test
    public void testCreateRewriter() {

        final WordBreakCompoundRewriterFactory factory = new WordBreakCompoundRewriterFactory("r1");
        factory.configure(Collections.singletonMap("dictionaryField", "f1"));
        final IndexShard indexShard = mock(IndexShard.class);
        final IndexSearcher indexSearcher = mock(IndexSearcher.class);
        final Closeable closeable = mock(Closeable.class);

        final Engine.Searcher searcher = new Engine.Searcher("source", indexSearcher, closeable);

        final IndexReader reader = mock(IndexReader.class);

        when(indexShard.acquireSearcher("WordBreakCompoundRewriter")).thenReturn(searcher);
        when(indexSearcher.getIndexReader()).thenReturn(reader);

        final RewriterFactory rewriterFactory = factory.createRewriterFactory(indexShard);

        assertTrue(rewriterFactory.createRewriter(null, null) instanceof WordBreakCompoundRewriter);

    }


}
