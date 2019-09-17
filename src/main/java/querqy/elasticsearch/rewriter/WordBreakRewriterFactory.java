package querqy.elasticsearch.rewriter;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.spell.WordBreakSpellChecker;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import querqy.elasticsearch.ConfigUtils;
import querqy.elasticsearch.ESRewriterFactory;
import querqy.lucene.contrib.rewrite.WordBreakCompoundRewriter;
import querqy.model.ExpandedQuery;
import querqy.model.Term;
import querqy.rewrite.QueryRewriter;
import querqy.rewrite.RewriterFactory;
import querqy.rewrite.SearchEngineRequestAdapter;
import querqy.trie.TrieMap;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WordBreakRewriterFactory extends ESRewriterFactory {

    // this controls behaviour of the Lucene WordBreakSpellChecker:
    // for compounds: maximum distance of leftmost and rightmost term index
    //                e.g. max_changes = 1 for A B C D will check AB BC CD,
    //                     max_changes = 2 for A B C D will check AB ABC BC BCD CD
    // for decompounds: maximum splits performed
    //                  e.g. max_changes = 1 for ABCD will check A BCD, AB CD, ABC D,
    //                       max_changes = 2 for ABCD will check A BCD, A B CD, A BC D, AB CD, AB C D, ABC D
    // as we currently only send 2-grams to WBSP for compounding only max_changes = 1 is correctly supported
    private static final int MAX_CHANGES = 1;

    private static final int DEFAULT_MIN_SUGGESTION_FREQ = 1;
    private static final int DEFAULT_MAX_COMBINE_LENGTH = 30;
    private static final int DEFAULT_MIN_BREAK_LENGTH = 3;
    private static final int DEFAULT_MAX_DECOMPOUND_EXPANSIONS = 3;
    private static final boolean DEFAULT_LOWER_CASE_INPUT = false;
    private static final boolean DEFAULT_ALWAYS_ADD_REVERSE_COMPOUNDS = false;
    private static final boolean DEFAULT_VERIFY_DECOMPOUND_COLLATION = false;


    private int minSuggestionFreq = DEFAULT_MIN_SUGGESTION_FREQ;
    private int maxCombineLength  =  DEFAULT_MAX_COMBINE_LENGTH;
    private int minBreakLength = DEFAULT_MIN_BREAK_LENGTH;
    private String dictionaryField;

    private boolean lowerCaseInput = DEFAULT_LOWER_CASE_INPUT;
    private boolean alwaysAddReverseCompounds = DEFAULT_ALWAYS_ADD_REVERSE_COMPOUNDS;

    private WordBreakSpellChecker spellChecker;
    private TrieMap<Boolean> reverseCompoundTriggerWords;
    private int maxDecompoundExpansions = DEFAULT_MAX_DECOMPOUND_EXPANSIONS;
    private boolean verifyDecompoundCollation = DEFAULT_VERIFY_DECOMPOUND_COLLATION;


    public WordBreakRewriterFactory(final String rewriterId) {
        super(rewriterId);
    }

    @Override
    public void configure(final Map<String, Object> config) throws Exception {

        minSuggestionFreq = ConfigUtils.getArg(config, "minSuggestionFreq", DEFAULT_MIN_SUGGESTION_FREQ);
        maxCombineLength  = ConfigUtils.getArg(config, "maxCombineLength", DEFAULT_MAX_COMBINE_LENGTH);
        minBreakLength  = ConfigUtils.getArg(config, "minBreakLength", DEFAULT_MIN_BREAK_LENGTH);
        dictionaryField = ConfigUtils.getStringArg(config, "dictionaryField")
                .orElseThrow(() -> new IllegalArgumentException("Missing config:  dictionaryField"));
        lowerCaseInput = ConfigUtils.getArg(config, "lowerCaseInput", DEFAULT_LOWER_CASE_INPUT);
        alwaysAddReverseCompounds = ConfigUtils.getArg(config, "alwaysAddReverseCompounds",
                DEFAULT_ALWAYS_ADD_REVERSE_COMPOUNDS);

        spellChecker = new WordBreakSpellChecker();
        spellChecker.setMaxChanges(MAX_CHANGES);
        spellChecker.setMinSuggestionFrequency(minSuggestionFreq);
        spellChecker.setMaxCombineWordLength(maxCombineLength);
        spellChecker.setMinBreakWordLength(minBreakLength);
        spellChecker.setMaxEvaluations(100);

        reverseCompoundTriggerWords = new TrieMap<>();
        final Collection<String> reverseCompoundTriggerWordsConf =
                (Collection<String>) config.get("reverseCompoundTriggerWords");
        if (reverseCompoundTriggerWordsConf != null) {
            for (final String word : new HashSet<>(reverseCompoundTriggerWordsConf)) {
                reverseCompoundTriggerWords.put(word, Boolean.TRUE);
            }
        }

        Map<String, Object> decompoundConf = (Map<String, Object>) config.get("decompound");
        if (decompoundConf == null) {
            decompoundConf = Collections.emptyMap();
        }
        maxDecompoundExpansions = ConfigUtils.getArg(decompoundConf, "maxExpansions",
                DEFAULT_MAX_DECOMPOUND_EXPANSIONS);
        verifyDecompoundCollation =  ConfigUtils.getArg(decompoundConf, "verifyCollation",
                DEFAULT_VERIFY_DECOMPOUND_COLLATION);

    }

    @Override
    public List<String> validateConfiguration(final Map<String, Object> config) {
        return null;
    }

    @Override
    public RewriterFactory createRewriterFactory(final IndexShard indexShard) throws Exception {

        return new RewriterFactory(getRewriterId()) {
            @Override
            public QueryRewriter createRewriter(final ExpandedQuery input,
                                                final SearchEngineRequestAdapter searchEngineRequestAdapter) {


                return new WordBreakCompoundRewriter(spellChecker, getShardIndexReader(indexShard), dictionaryField,
                        lowerCaseInput, alwaysAddReverseCompounds, reverseCompoundTriggerWords, maxDecompoundExpansions,
                        verifyDecompoundCollation);


            }

            @Override
            public Set<Term> getGenerableTerms() {
                return QueryRewriter.EMPTY_GENERABLE_TERMS;
            }
        };
    }

    private IndexReader getShardIndexReader(final IndexShard indexShard) {

        Engine.Searcher searcher = null;

        try {
            searcher = indexShard.acquireSearcher("WordBreakCompoundRewriter");
            return searcher.reader();
        } finally {
            if (searcher != null) {
                searcher.close();
            }
        }
    }




}
