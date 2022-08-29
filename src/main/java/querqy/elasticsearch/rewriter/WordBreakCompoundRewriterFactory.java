package querqy.elasticsearch.rewriter;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.spell.WordBreakSpellChecker;
import org.elasticsearch.index.shard.IndexShard;
import querqy.elasticsearch.ConfigUtils;
import querqy.elasticsearch.DismaxSearchEngineRequestAdapter;
import querqy.elasticsearch.ESRewriterFactory;
import querqy.lucene.contrib.rewrite.wordbreak.LuceneCompounder;
import querqy.lucene.contrib.rewrite.wordbreak.MorphologicalCompounder;
import querqy.lucene.contrib.rewrite.wordbreak.MorphologicalWordBreaker;
import querqy.lucene.contrib.rewrite.wordbreak.Morphology;
import querqy.lucene.contrib.rewrite.wordbreak.MorphologyProvider;
import querqy.lucene.contrib.rewrite.wordbreak.SpellCheckerCompounder;
import querqy.lucene.contrib.rewrite.wordbreak.WordBreakCompoundRewriter;
import querqy.model.ExpandedQuery;
import querqy.model.Term;
import querqy.rewrite.QueryRewriter;
import querqy.rewrite.RewriterFactory;
import querqy.rewrite.SearchEngineRequestAdapter;
import querqy.trie.TrieMap;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class WordBreakCompoundRewriterFactory extends ESRewriterFactory {

    // this controls behaviour of the Lucene WordBreakSpellChecker:
    // for compounds: maximum distance of leftmost and rightmost term index
    //                e.g. max_changes = 1 for A B C D will check AB BC CD,
    //                     max_changes = 2 for A B C D will check AB ABC BC BCD CD
    // for decompounds: maximum splits performed
    //                  e.g. max_changes = 1 for ABCD will check A BCD, AB CD, ABC D,
    //                       max_changes = 2 for ABCD will check A BCD, A B CD, A BC D, AB CD, AB C D, ABC D
    // as we currently only send 2-grams to WBSP for compounding only max_changes = 1 is correctly supported
    static final int MAX_CHANGES = 1;

    static final int MAX_EVALUATIONS = 100;

    static final int DEFAULT_MIN_SUGGESTION_FREQ = 1;
    static final int DEFAULT_MAX_COMBINE_LENGTH = 30;
    static final int DEFAULT_MIN_BREAK_LENGTH = 3;
    static final int DEFAULT_MAX_DECOMPOUND_EXPANSIONS = 3;
    static final boolean DEFAULT_LOWER_CASE_INPUT = false;
    static final boolean DEFAULT_ALWAYS_ADD_REVERSE_COMPOUNDS = false;
    static final boolean DEFAULT_VERIFY_DECOMPOUND_COLLATION = false;

    static final String DEFAULT_MORPHOLOGY_NAME = "DEFAULT";

    private static final MorphologyProvider MORPHOLOGY_PROVIDER = new MorphologyProvider();

    private String dictionaryField;

    private boolean lowerCaseInput = DEFAULT_LOWER_CASE_INPUT;
    private boolean alwaysAddReverseCompounds = DEFAULT_ALWAYS_ADD_REVERSE_COMPOUNDS;

    private WordBreakSpellChecker spellChecker;
    private LuceneCompounder compounder;
    private MorphologicalWordBreaker wordBreaker;
    private TrieMap<Boolean> reverseCompoundTriggerWords;
    private TrieMap<Boolean> protectedWords;
    private int maxDecompoundExpansions = DEFAULT_MAX_DECOMPOUND_EXPANSIONS;
    private boolean verifyDecompoundCollation = DEFAULT_VERIFY_DECOMPOUND_COLLATION;

    public WordBreakCompoundRewriterFactory(final String rewriterId) {
        super(rewriterId);
    }

    @Override
    public void configure(final Map<String, Object> config) {

        final int minSuggestionFreq = ConfigUtils.getArg(config, "minSuggestionFreq", DEFAULT_MIN_SUGGESTION_FREQ);
        final int maxCombineLength = ConfigUtils.getArg(config, "maxCombineLength", DEFAULT_MAX_COMBINE_LENGTH);
        final int minBreakLength = ConfigUtils.getArg(config, "minBreakLength", DEFAULT_MIN_BREAK_LENGTH);
        dictionaryField = ConfigUtils.getStringArg(config, "dictionaryField")
                .map(String::trim)
                .filter(s -> !s.isEmpty())
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

        final String defaultMorphologyName = ConfigUtils.getStringArg(config, "morphology", DEFAULT_MORPHOLOGY_NAME);

        Map<String, Object> compoundConf = (Map<String, Object>) config.get("compound");
        if (compoundConf == null) {
            compoundConf = Collections.emptyMap();
        }

        final String compoundMorphologyName = ConfigUtils.getStringArg(compoundConf, "morphology",
                defaultMorphologyName);

        final Optional<Morphology> compoundMorphology = MORPHOLOGY_PROVIDER.get(compoundMorphologyName);

        if (compoundMorphology.isEmpty() || compoundMorphology.get() == MorphologyProvider.DEFAULT) {
            // use WordBreakSpellChecker when DEFAULT for backwards compatibility
            final WordBreakSpellChecker spellChecker = new WordBreakSpellChecker();
            spellChecker.setMaxChanges(MAX_CHANGES);
            spellChecker.setMinSuggestionFrequency(minSuggestionFreq);
            spellChecker.setMaxCombineWordLength(maxCombineLength);
            spellChecker.setMinBreakWordLength(minBreakLength);
            spellChecker.setMaxEvaluations(100);
            compounder = new SpellCheckerCompounder(spellChecker, dictionaryField, lowerCaseInput);
        } else {
            compounder = new MorphologicalCompounder(compoundMorphology.get(), dictionaryField, lowerCaseInput,
                    minSuggestionFreq);
        }


        Map<String, Object> decompoundConf = (Map<String, Object>) config.get("decompound");
        if (decompoundConf == null) {
            decompoundConf = Collections.emptyMap();
        }
        final String decompoundMorphologyName = ConfigUtils.getStringArg(decompoundConf, "morphology",
                defaultMorphologyName);
        final Optional<Morphology> decompoundMorphology = MORPHOLOGY_PROVIDER.get(decompoundMorphologyName);


        wordBreaker = new MorphologicalWordBreaker(decompoundMorphology.get(), dictionaryField, lowerCaseInput,
                minSuggestionFreq, minBreakLength, MAX_EVALUATIONS);

        reverseCompoundTriggerWords = ConfigUtils.getTrieSetArg(config, "reverseCompoundTriggerWords");
        protectedWords = ConfigUtils.getTrieSetArg(config, "protectedWords");

        maxDecompoundExpansions = ConfigUtils.getArg(decompoundConf, "maxExpansions",
                DEFAULT_MAX_DECOMPOUND_EXPANSIONS);
        verifyDecompoundCollation =  ConfigUtils.getArg(decompoundConf, "verifyCollation",
                DEFAULT_VERIFY_DECOMPOUND_COLLATION);

    }

    @Override
    public List<String> validateConfiguration(final Map<String, Object> config) {

        final List<String> errors = new LinkedList<>();
        final Optional<String> optValue = ConfigUtils.getStringArg(config, "dictionaryField").map(String::trim)
                .filter(s -> !s.isEmpty());
        if (!optValue.isPresent()) {
            errors.add("Missing config:  dictionaryField");
        }

        ConfigUtils.getStringArg(config, "morphology").ifPresent(morphologyName -> {
            if (!MORPHOLOGY_PROVIDER.exists(morphologyName)) {
                errors.add("Unknown morphology: " + morphologyName);
            }
        });

        final Map<String, Object> decompoundConf = (Map<String, Object>) config.get("decompound");
        if (decompoundConf != null) {
            ConfigUtils.getStringArg(decompoundConf, "morphology").ifPresent(morphologyName -> {
                if (!MORPHOLOGY_PROVIDER.exists(morphologyName)) {
                    errors.add("Unknown decompound morphology: " + morphologyName);
                }
            });
        }

        final Map<String, Object> compoundConf = (Map<String, Object>) config.get("compound");
        if (compoundConf != null) {
            ConfigUtils.getStringArg(compoundConf, "morphology").ifPresent(morphologyName -> {
                if (!MORPHOLOGY_PROVIDER.exists(morphologyName)) {
                    errors.add("Unknown compound morphology: " + morphologyName);
                }
            });
        }


        return errors;

    }

    @Override
    public RewriterFactory createRewriterFactory(final IndexShard indexShard) {

        return new RewriterFactory(getRewriterId()) {
            @Override
            public QueryRewriter createRewriter(final ExpandedQuery input,
                                                final SearchEngineRequestAdapter searchEngineRequestAdapter) {


                return new WordBreakCompoundRewriter(wordBreaker, compounder,
                        getShardIndexReader((DismaxSearchEngineRequestAdapter) searchEngineRequestAdapter),
                        lowerCaseInput, alwaysAddReverseCompounds, reverseCompoundTriggerWords, maxDecompoundExpansions,
                        verifyDecompoundCollation, protectedWords);


            }

            @Override
            public Set<Term> getGenerableTerms() {
                return QueryRewriter.EMPTY_GENERABLE_TERMS;
            }
        };
    }

    public WordBreakSpellChecker getSpellChecker() {
        return spellChecker;
    }

    public String getDictionaryField() {
        return dictionaryField;
    }


    public boolean isLowerCaseInput() {
        return lowerCaseInput;
    }

    public boolean isAlwaysAddReverseCompounds() {
        return alwaysAddReverseCompounds;
    }

    public TrieMap<Boolean> getReverseCompoundTriggerWords() {
        return reverseCompoundTriggerWords;
    }

    public TrieMap<Boolean> getProtectedWords() {
        return protectedWords;
    }

    public int getMaxDecompoundExpansions() {
        return maxDecompoundExpansions;
    }

    public boolean isVerifyDecompoundCollation() {
        return verifyDecompoundCollation;
    }

    private IndexReader getShardIndexReader(final DismaxSearchEngineRequestAdapter searchEngineRequestAdapter) {
        return searchEngineRequestAdapter.getSearchExecutionContext().searcher().getTopReaderContext().reader();
    }

    public LuceneCompounder getCompounder() {
        return compounder;
    }

    public MorphologicalWordBreaker getWordBreaker() {
        return wordBreaker;
    }


}
