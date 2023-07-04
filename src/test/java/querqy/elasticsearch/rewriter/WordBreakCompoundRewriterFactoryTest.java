package querqy.elasticsearch.rewriter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static querqy.elasticsearch.rewriter.WordBreakCompoundRewriterFactory.*;
import static querqy.elasticsearch.rewriter.WordBreakCompoundRewriterFactory.DEFAULT_MAX_COMBINE_LENGTH;
import static querqy.elasticsearch.rewriter.WordBreakCompoundRewriterFactory.DEFAULT_VERIFY_DECOMPOUND_COLLATION;
import static querqy.elasticsearch.rewriter.WordBreakCompoundRewriterFactory.MAX_CHANGES;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.spell.WordBreakSpellChecker;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import querqy.elasticsearch.DismaxSearchEngineRequestAdapter;
import querqy.lucene.contrib.rewrite.wordbreak.LuceneCompounder;
import querqy.lucene.contrib.rewrite.wordbreak.MorphologicalWordBreaker;
import querqy.lucene.contrib.rewrite.wordbreak.WordBreakCompoundRewriter;
import querqy.rewrite.RewriterFactory;
import querqy.trie.TrieMap;


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
    public void testValidateRefusesInvalidMorphology() {
        final WordBreakCompoundRewriterFactory factory = new WordBreakCompoundRewriterFactory("r1");
        final Map<String, Object> config = new HashMap<>();
        config.put("dictionaryField", "f1");
        config.put("morphology", "IDIOLECT");

        final List<String> errors = factory.validateConfiguration(config);
        assertThat(errors, Matchers.contains("Unknown morphology: IDIOLECT"));
    }

    @Test
    public void testThatDefaultConfigurationIsApplied() throws Exception {

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

        assertNotNull(factory.getCompounder());

        final MorphologicalWordBreaker wordBreaker = factory.getWordBreaker();
        assertNotNull(wordBreaker);

        final IndexReader indexReader = mock(IndexReader.class);
        final IndexReaderContext topReaderContext = mock(IndexReaderContext.class);

        when(indexReader.getContext()).thenReturn(topReaderContext);
        when(topReaderContext.reader()).thenReturn(indexReader);
        // This is horrible, but there seems to be no way to mock top level IndexReaderContext
        final Field field = IndexReaderContext.class.getDeclaredField("isTopLevel");
        field.setAccessible(true);
        field.setBoolean(topReaderContext, true);
        field.setAccessible(false);

        when(indexReader.docFreq(new Term("f1", "def"))).thenReturn(20);

        wordBreaker.breakWord("abcdef", indexReader, 2, true);
        verify(indexReader, times(1)).docFreq(eq(new Term("f1", "def")));
        verify(indexReader, times(1)).docFreq(eq(new Term("f1", "abc")));

        // min break length is 3:
        verify(indexReader, times(0)).docFreq(eq(new Term("f1", "ab")));
        // this will not be called by DEFAULT morphology:
        verify(indexReader, times(0)).docFreq(eq(new Term("f1", "cdef")));
        verify(indexReader, times(0)).docFreq(eq(new Term("f1", "abce")));

    }


    @Test
    public void testThatConfigurationIsApplied() throws Exception  {

        final Map<String, Object> config = new HashMap<>();
        config.put("minSuggestionFreq", 11);
        config.put("maxCombineLength", 22);
        config.put("minBreakLength", 1);
        config.put("dictionaryField", "f2");
        config.put("lowerCaseInput", !DEFAULT_LOWER_CASE_INPUT);
        config.put("alwaysAddReverseCompounds", !DEFAULT_ALWAYS_ADD_REVERSE_COMPOUNDS);
        config.put("reverseCompoundTriggerWords", Arrays.asList("für", "aus"));
        config.put("protectedWords", Arrays.asList("blumen"));
        config.put("morphology", "GERMAN");

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

        final TrieMap<Boolean> protectedWords = factory.getProtectedWords();
        assertNotNull(protectedWords);
        assertTrue(protectedWords.get("blumen").getStateForCompleteSequence().isFinal());

        final MorphologicalWordBreaker wordBreaker = factory.getWordBreaker();
        assertNotNull(wordBreaker);

        final IndexReader indexReader = mock(IndexReader.class);
        final IndexReaderContext topReaderContext = mock(IndexReaderContext.class);

        when(indexReader.getContext()).thenReturn(topReaderContext);
        when(topReaderContext.reader()).thenReturn(indexReader);
        // This is horrible, but there seems to be no way to mock top level IndexReaderContext
        final Field field = IndexReaderContext.class.getDeclaredField("isTopLevel");
        field.setAccessible(true);
        field.setBoolean(topReaderContext, true);
        field.setAccessible(false);

        when(indexReader.docFreq(new Term("f2", "de"))).thenReturn(20);

        wordBreaker.breakWord("abcde", indexReader, 2, true);
        verify(indexReader, times(1)).docFreq(eq(new Term("f2", "e")));
        verify(indexReader, times(1)).docFreq(eq(new Term("f2", "de")));
        verify(indexReader, times(1)).docFreq(eq(new Term("f2", "cde")));
        verify(indexReader, times(1)).docFreq(eq(new Term("f2", "bcde")));
        // this will be generated by GERMAN morphology:
        verify(indexReader, times(1)).docFreq(eq(new Term("f2", "abce")));

    }

    @Test
    public void testThatDecompoundMorphologyIsApplied() throws Exception  {

        final Map<String, Object> config = new HashMap<>();
        config.put("minSuggestionFreq", 11);
        config.put("maxCombineLength", 22);
        config.put("minBreakLength", 1);
        config.put("dictionaryField", "f2");
        config.put("lowerCaseInput", !DEFAULT_LOWER_CASE_INPUT);
        config.put("alwaysAddReverseCompounds", !DEFAULT_ALWAYS_ADD_REVERSE_COMPOUNDS);
        config.put("reverseCompoundTriggerWords", Arrays.asList("für", "aus"));
        config.put("protectedWords", Arrays.asList("blumen"));

        Map<String, Object> decompoundConf = new HashMap<>();
        config.put("decompound", decompoundConf);

        decompoundConf.put("verifyCollation", !DEFAULT_VERIFY_DECOMPOUND_COLLATION);
        decompoundConf.put("maxExpansions", 87);
        decompoundConf.put("morphology", "GERMAN");


        final WordBreakCompoundRewriterFactory factory = new WordBreakCompoundRewriterFactory("r1");
        factory.configure(config);

        final MorphologicalWordBreaker wordBreaker = factory.getWordBreaker();
        assertNotNull(wordBreaker);

        final IndexReader indexReader = mock(IndexReader.class);
        final IndexReaderContext topReaderContext = mock(IndexReaderContext.class);

        when(indexReader.getContext()).thenReturn(topReaderContext);
        when(topReaderContext.reader()).thenReturn(indexReader);
        // This is horrible, but there seems to be no way to mock top level IndexReaderContext
        final Field field = IndexReaderContext.class.getDeclaredField("isTopLevel");
        field.setAccessible(true);
        field.setBoolean(topReaderContext, true);
        field.setAccessible(false);

        when(indexReader.docFreq(new Term("f2", "de"))).thenReturn(20);

        wordBreaker.breakWord("abcde", indexReader, 2, true);
        verify(indexReader, times(1)).docFreq(eq(new Term("f2", "e")));
        verify(indexReader, times(1)).docFreq(eq(new Term("f2", "de")));
        verify(indexReader, times(1)).docFreq(eq(new Term("f2", "cde")));
        verify(indexReader, times(1)).docFreq(eq(new Term("f2", "bcde")));
        // this will be generated by GERMAN morphology:
        verify(indexReader, times(1)).docFreq(eq(new Term("f2", "abce")));

    }

    @Test
    public void testThatCompoundMorphologyIsApplied() throws Exception  {

        final Map<String, Object> config = new HashMap<>();
        config.put("minSuggestionFreq", 11);
        config.put("maxCombineLength", 22);
        config.put("minBreakLength", 1);
        config.put("dictionaryField", "f2");
        config.put("lowerCaseInput", !DEFAULT_LOWER_CASE_INPUT);
        config.put("alwaysAddReverseCompounds", !DEFAULT_ALWAYS_ADD_REVERSE_COMPOUNDS);
        config.put("reverseCompoundTriggerWords", Arrays.asList("für", "aus"));
        config.put("protectedWords", Arrays.asList("blumen"));

        final Map<String, Object> decompoundConf = new HashMap<>();
        config.put("decompound", decompoundConf);

        decompoundConf.put("verifyCollation", !DEFAULT_VERIFY_DECOMPOUND_COLLATION);
        decompoundConf.put("maxExpansions", 87);

        final Map<String, Object> compoundConf = new HashMap<>();
        config.put("compound", compoundConf);

        compoundConf.put("morphology", "GERMAN");

        final WordBreakCompoundRewriterFactory factory = new WordBreakCompoundRewriterFactory("r1");
        factory.configure(config);


        final LuceneCompounder compounder = factory.getCompounder();


        final MorphologicalWordBreaker wordBreaker = factory.getWordBreaker();
        assertNotNull(wordBreaker);

        final IndexReader indexReader = mock(IndexReader.class);
        final IndexReaderContext topReaderContext = mock(IndexReaderContext.class);

        // This is horrible, but there seems to be no way to mock top level IndexReaderContext
        final Field field = IndexReaderContext.class.getDeclaredField("isTopLevel");
        field.setAccessible(true);
        field.setBoolean(topReaderContext, true);
        field.setAccessible(false);

        compounder.combine(new querqy.model.Term[] {
                new querqy.model.Term(null, "ab"), new querqy.model.Term(null, "de")}, indexReader, false);
        // this will be generated by GERMAN morphology:
        verify(indexReader, times(1)).docFreq(eq(new Term("f2", "absde")));

    }

    @Test
    public void testCreateRewriter() throws Exception {

        final WordBreakCompoundRewriterFactory factory = new WordBreakCompoundRewriterFactory("r1");
        factory.configure(Collections.singletonMap("dictionaryField", "f1"));
        final IndexShard indexShard = mock(IndexShard.class);
        final IndexReader indexReader = mock(IndexReader.class);
        final IndexReaderContext topReaderContext = mock(IndexReaderContext.class);

        when(topReaderContext.reader()).thenReturn(indexReader);

        // This is horrible, but there seems to be no way to mock top level IndexReaderContext
        final Field field = IndexReaderContext.class.getDeclaredField("isTopLevel");
        field.setAccessible(true);
        field.setBoolean(topReaderContext, true);
        field.setAccessible(false);

        final SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        final IndexSearcher searcher = mock(IndexSearcher.class);
        when(searchExecutionContext.searcher()).thenReturn(searcher);

        when(searcher.getTopReaderContext()).thenReturn(topReaderContext);

        final DismaxSearchEngineRequestAdapter searchEngineRequestAdapter =
                mock(DismaxSearchEngineRequestAdapter.class);

        when(searchEngineRequestAdapter.getSearchExecutionContext()).thenReturn(searchExecutionContext);

        final RewriterFactory rewriterFactory = factory.createRewriterFactory(indexShard);

        assertTrue(rewriterFactory.createRewriter(null, searchEngineRequestAdapter) instanceof
                WordBreakCompoundRewriter);

    }


}
