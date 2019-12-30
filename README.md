[ ![Download](https://api.bintray.com/packages/renekrie/maven/querqy-elasticsearch/images/download.svg) ](https://bintray.com/renekrie/maven/querqy-elasticsearch/_latestVersion)

# Querqy for Elasticsearch

This is the Elasticsearch plugin version of [Querqy](https://github.com/renekrie/querqy),
a query preprocessing framework for Java-based search engines.

## Installation
### Installing the plugin

- Stop Elasticsearch if it is running.
- Open a shell and `cd` into your Elasticsearch directory.
- Replace the \<VERSION> placeholder in the following command and execute
(see below for available versions):

~~~
./bin/elasticsearch-plugin install  "https://dl.bintray.com/renekrie/maven/querqy/querqy-elasticsearch/<VERSION>/querqy-elasticsearch-<VERSION>.zip
~~~

- Answer '`y`es' to the security related questions. Querqy needs special
permissions to load query rewriters dynamically.
- When you start Elasticsearch, you should see an INFO log message
`loaded plugin [querqy]`.

### Versions

The Querqy version naming scheme is `<major version>.<minor version>.<Elasticsearch version string>.<bugfix version>`.

The following versions are available. Make sure you pick the version
that matches your Elasticsearch version:

|Elasticsearch version|Querqy for Elasticsearch (use this as \<VERSION> above)|Querqy Lucene version|
|----|-----------|-------------|
|7.2.1|1.0.es721.0|4.5.lucene800.1|
|7.2.0|1.0.es720.0|4.5.lucene800.1|
|7.1.1|1.0.es711.0|4.5.lucene800.1|
|7.1.0|1.0.es710.0|4.5.lucene800.1|
|7.0.1|1.0.es701.0|4.5.lucene800.1|

## Querying

Querqy defines its own query builder which can be can executed with a
rich set of parameters. We will walk through these parameters step by
step, starting with a minimal query, which does not use any rewriter, then
adding a 'Common Rules' rewriter and finally explaining the full set of
parameters, many of them not related to query rewriting but to search
relevance tuning in general.

We will provide the examples in JSON and Python. If you want to run
the Python examples, you will need to install the Elasticsearch
module, for example using

```bash
pip3 install Elasticsearch
```

and then import it via

```python
from elasticsearch import Elasticsearch
```



### Minimal search query

JSON:

~~~json
POST /myindex/_search
{
    "query": {
        "querqy": {
            "matching_query": {
                "query": "notebook"
            },
            "query_fields": [ "title^3.0", "brand^2.1", "shortSummary"]
        }
    }
}
~~~

Python:

~~~python
es = Elasticsearch()
resp = es.search(index='myindex', size=10, body = {
    "query": {
        "querqy": {                                         # 1
            "matching_query": {                             # 2
                "query": "notebook"                         # 3
            },
            "query_fields": [
                "title^3.0", "brand^2.1", "shortSummary"    # 4
            ]
        }
    }
})
print(resp)
~~~

Querqy provides a new query builder, `querqy` (#1) that can be used in a query 
just like any other Elasticsearch query type. 

The `matching_query` (#2) defines the query for
which documents will be matched and retrieved. The matching query is different
from boosting queries which would only influence the ranking but not the matching. We will later 
see that Querqy allows to specify information for boosting outside the `matching_query` 
object. The `query` element (#3) contains the query string. In most cases this
is just the query string as it was typed into the search box by the user.

The list of `query_fields` (#4) specifies in which fields to search. A field name
can have an optional field weight. In the example, the field weight for `title` 
is `3.0`. The default field weight is `1.0`. Field weights must be positive. We
will later see that the `query_fields` are applied to parts of the `querqy` query 
other than the `matching_query` as well. Hence the `query_fields` list is not
a child element of the `matching_query`.

The combination of a query string with a list of fields and field weights resembles Elasticsearch's [`multi_match`](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-multi-match-query.html) query. However, Querqy
always builds a query that is similar to a `multi_match` query of type `cross_fields`. Unlike the `multi_match` query, Querqy always builds a `cross_fields` query, even if the fields use different analyzers. Furthermore, Querqy uses a different approach to deal with document frequency and scoring when the input terms are expanded across fields and split into further terms by the analyzers. Details will be explained in section [(`matching_query`) `similarity_scoring`](https://github.com/renekrie/querqy-elasticsearch#matching_query-similarity_scoring-1).

### Using a rewriter

We will use the 'Common Rules rewriter' as an example. This is the best known rewriter from Querqy's Solr version. It uses a set of rules to rewrite the query. See [https://github.com/renekrie/querqy](https://github.com/renekrie/querqy#getting-started-setting-up-common-rules-under-solr) for a documentation of the rules format. Note that Querqy's Elasticsearch version does not implement `DECORATE` instructions and logging of the matching queries to the query result.

#### Defining a rewriter

Querqy provides a REST endpoint to manage rewriters at `/_querqy/rewriter`.

Creating/configuring a 'Common Rules rewriter':

JSON:

~~~JSON
PUT  /_querqy/rewriter/common_rules
{
    "class": "querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory",
    "config": {
        "rules" : "notebook =>\nSYNONYM: laptop"
    }
}
~~~    

Python:
   
~~~python
import requests


# 1
rules = """
 
notebook =>
  SYNONYM: laptop
  
"""
  
req = {
    "class": "querqy.elasticsearch.rewriter.SimpleCommonRulesRewriterFactory", # 2
    "config": {                                                                # 3
        "rules" : rules                                                        # 4
    }
}
   
rewriter_endpoint = 'http://localhost:9200/_querqy/rewriter/'
rewriter_name = 'common_rules'                                                 # 5

resp = requests.put(rewriter_endpoint + rewriter_name, json=req)               # 6
print(resp.json())
      
~~~   

Rewriter definitions are uploaded by sending a `PUT` request to the rewriter 
endpoint (#6). The last part of the request URL path will become the name of 
the rewriter. (#5)

A rewriter definition must contain a `class` element (#2). Its value references
an implementation of a `querqy.elasticsearch.ESRewriterFactory` which will provide
the rewriter that we want to use.

The rewriter definition can also have a `config` object (#3) which contains the 
rewriter-specific configuration. 

In the case of the SimpleCommonRulesRewriter, the configuration must contain 
the rewriting `rules`. The rules are defined (#1) using the same syntax 
like in the [Solr version](https://github.com/renekrie/querqy#getting-started-setting-up-common-rules-under-solr) (#4). Remember to escape line breaks etc. 
when you include your rules in a JSON document.

#### Applying rewriters

We can now apply one or more rewriters (#1) to a query:
 
JSON:
 
~~~json
POST /myindex/_search
{
    "query": {
        "querqy": {
            "matching_query": {
                "query": "notebook"
            },
            "query_fields": [ "title^3.0", "brand^2.1", "shortSummary"],
            "rewriters": ["common_rules"]
        }
    }
}
~~~

Python:

~~~python
es = Elasticsearch()
resp = es.search(index='myindex', size=10, body = {
    "query": {
        "querqy": {                                         
            "matching_query": {
                "query": "notebook"
            },
            "query_fields": [
                "title^3.0", "brand^2.1", "shortSummary"
            ],
            "rewriters": ["common_rules"]                   # 1
        }
    }
})
print(resp)
~~~

The rewriters are added to the query using a list named `rewriters` (#1). This list contains the rewrite chain - the list of rewriters in the order in which they will be applied and in which they will manipulate the query. 

Rewiters are referenced from `rewriters` either just by their name or by the `name` property of an object, which allows to pass request parameters to the rewriter. 

The following example shows two rewriters, one of them receiving additional parameters: 

JSON:

~~~json
POST /myindex/_search
{
    "query": {
        "querqy": {
            "matching_query": {
                "query": "notebook"
            },
            "query_fields": [ "title^3.0", "brand^2.1", "shortSummary"],
            "rewriters": [
                "word_break", 
                {
                    "name": "common_rules", 
                    "params": {
                        "criteria": {
                            "filter": "$[?(!@.prio || @.prio == 1)]"
                        }
                    }
                }
            ]
        }
    }
}
~~~

Python:

~~~python
es = Elasticsearch()
resp = es.search(index='myindex', size=10, body = {
    "query": {
        "querqy": {                                         
            "matching_query": {
                "query": "notebook"
            },
            "query_fields": [
                "title^3.0", "brand^2.1", "shortSummary"
            ],
            "rewriters": [
                "word_break",                                         # 1
                {                                                     # 2
                    "name": "common_rules",                           # 3
                    "params": {
                        "criteria": {
                            "filter": "$[?(!@.prio || @.prio == 1)]"  
                        }
                    }
                }
            ]
        }
    }
})
print(resp)
~~~


The first rewriter, `word_break` (#1), is just referenced by its name (we will see 
a 'word break rewriter' configuration later @TODO). The second rewriter is called
using a JSON object. Its `name` property references the rewriter definition by its
name, `"common_rules"` (#2). The `params` object (#3) is passed to the rewriter. 

The `criteria` parameter is specific to the Common Rules rewriter. The `filter` expression in the example ensures that only rules that either have a `prio` property set to `1` or that don't have any `prio` property at all will be applied (see [here](https://github.com/renekrie/querqy#rule-properties-rule-ordering-and-rule-filtering) for rule filtering and ordering in the Common Rules rewriter.)

In the above example rewrite chain, the `word_break` rewriter will be applied before the `common_rules` rewriter.

### More request parameters

In this section we will describe the remaining request parameters of a `querqy` query. The following example shows a 'full request' which uses all parameters. We are going to explain them one by one below.

JSON:

~~~json
POST /myindex/_search
{
    "query": {
        "querqy": {                                         
            
            "matching_query": {
                "query": "notebook",
                "similarity_scoring": "dfc",
                "weight": 0.75
            },
            
            "query_fields": [
                "title^3.0", "brand^2.1", "shortSummary"
            ],
            
            "minimum_should_match": "100%",
            "tie_breaker": 0.01,
            "field_boost_model": "prms",
            
            "rewriters": [
                "word_break",                                         
                {                                                     
                    "name": "common_rules",                           
                    "params": {
                        "criteria": {
                            "filter": "$[?(!@.prio || @.prio == 1)]"  
                        }
                    }
                }
            ],
            
            "boosting_queries": {
                "rewritten_queries": { 
                    "use_field_boost": false, 
                    "similarity_scoring": "off",
                    "positive_query_weight": 1.2,
                    "negative_query_weight": 2.0 
                },
                "phrase_boosts": {                                           
                    "full": { 
                        "fields": ["title", "brand^4"],
                        "slop": 2
                    },
                    "bigram": {
                        "fields": ["title"],
                        "slop": 3
                    },
                    "trigram": { 
                        "fields": ["title", "brand", "shortSummary"],
                        "slop": 6
                    },
                    "tie_breaker": 0.5 
                }
            },
            
            "generated" : { 
                "query_fields": [ 
                    "title^2.0", "brand^1.5", "shortSummary^0.0007"
                ],
                "field_boost_factor": 0.8 
            }
            
        }
    }
}
~~~

Python:

~~~python
es = Elasticsearch()
resp = es.search(index='myindex', size=10, body = {
    "query": {
        "querqy": {                                         
            
            "matching_query": {
                "query": "notebook",
                "similarity_scoring": "dfc",                          # 1
                "weight": 0.75                                        # 2
            },
            
            "query_fields": [
                "title^3.0", "brand^2.1", "shortSummary"
            ],
            
            "minimum_should_match": "100%",                           # 3
            "tie_breaker": 0.01,                                      # 4
            "field_boost_model": "prms",                              # 5
            
            "rewriters": [
                "word_break",                                         
                {                                                     
                    "name": "common_rules",                           
                    "params": {
                        "criteria": {
                            "filter": "$[?(!@.prio || @.prio == 1)]"  
                        }
                    }
                }
            ],
            
            "boosting_queries": {                                     # 6
                "rewritten_queries": {                                # 7
                    "use_field_boost": False,                         # 8
                    "similarity_scoring": "off",                      # 9
                    "positive_query_weight": 1.2,                     # 10
                    "negative_query_weight": 2.0                      # 11
                },
                "phrase_boosts": {                                    # 12                                          
                    "full": {                                         # 13
                        "fields": ["title", "brand^4"],               # 14
                        "slop": 2                                     # 15
                    },
                    "bigram": {                                       # 16
                        "fields": ["title"],
                        "slop": 3
                    },
                    "trigram": {                                      # 17
                        "fields": ["title", "brand", "shortSummary"],
                        "slop": 6
                    },
                    "tie_breaker": 0.5                                # 18
                }
            },
            
            "generated" : {                                           # 19
                "query_fields": [                                     # 20
                    "title^2.0", "brand^1.5", "shortSummary^0.0007"
                ],
                "field_boost_factor": 0.8                             # 21
            }
            
        }
    }
})
print(resp)
~~~

#### Global parameters and more on the matching query

We will start with parameters #1 to #5 which control the behaviour of the 
`matching_query`. Global parameters #3 to #5 also influcene `generated` queries and `boosting_queries` which we will describe later.


##### `minimum_should_match` (#3)

Default value: `1`

*The minimum number of query clauses that must match for a document to be returned* (description copied from Elasticsearch's ['match query' documentation] (https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html), which also see for valid parameter values). The minimum number of query clauses is counted across fields. For example, if the query `a b` is searched in `"query_fields":["f1", "f2"]` with `"minimum_should_match":"100%"`, the two terms need not match in the same field so that a document matching `f1:a` and `f2:b` will be included in the result set. 


##### `tie_breaker` (#4)

Default value: `0.0`

When a query term `a` is searched across fields (such as `f1`, `f2` and `f3`), the query is expanded into term queries (`f1:a`, `f2:a`, `f3:a`). The rewritten query will use as its own score the score of the highest scoring term query plus the sum of the scores of the other term queries multiplied with `tie_breaker`. Let's assume that `f2:a` produces the highest score, the resulting score will be `score(f2:a) + tie_breaker * (score(f1:a) + score(f3:a))`.   
  

##### `field_boost_model` (#5)

Values: `fixed` (default), `prms`

Querqy allows to choose between two approaches for field boosting in scoring:

- `fixed`: field boosts are specified at field names in `query_fields`. The same field weight will be used across all query terms for a given query field.
- `prms`: field boosts are derived from the distribution of the query terms in the index. More specifically, they are derived from the probability that a given query term occurs in a given field in the index. For example, given the query `apple iphone black` with query fields `brand`, `category` and `color`, the term `apple` will in most data sets have a greater probability and weight for the `brand` field compared to `category` and `color`, whereas `black` will have the greatest probability in the `color` field. This approach follows the ideas described in: J. Kim & W.B. Croft: *A Probabilistic Retrieval Model for Semi-structured Data*, 2009. Field weights specified in `query_fields` will be ignored if `field_boost_model` is set to `prms`.


##### (`matching_query`) `similarity_scoring` (#1)
 
Values: `dfc` (default), `on`, `off` 

Controls how Lucene's scoring implementation (= *similarity*) is used when an input query term is expanded across fields and when it is expanded during query rewriting: 

- `dfc`: 'document frequency correction' - use the same document frequency value for all terms that are derived from the same input query term. For example, let `a b` be the input query and let it be  rewritten into `(f1:a | f2:a | ((f1:x | f2:x) | (f1:y | f2:x)) (f1:b | f2:b)` by synonym and field expansion, then `(f1:a | f2:a | ((f1:x | f2:x) | (f1:y | f2:x))` (all derived from `a`) will use the same document frequency value. More specifically, Querqy will use the maximum document frequency of these terms as the document frequency value for all of them. Similarily, the maximum  document frequency of `(f1:b | f2:b)` will be used for these two terms. 
- `off`: Ignore the output of Lucene's similarity scoring. Only field boosts will be used for scoring.
- `on`: Use Lucene's similarity scoring output. Note that in Querqy field boosting is handled outside the similarity and it can be configured using the [`field_boost_model` parameter](https://github.com/renekrie/querqy-elasticsearch#field_boost_model-5).

##### (`matching_query`) `weight` (#2)

Default value: `1.0`

A weight that is multiplied with the score that is produced by the matching query before the score of the [boosting queries](https://github.com/renekrie/querqy-elasticsearch#boosting-queries-6) is added.

#### Boosting queries (#6)

The `boosting_queries` object contains information about sub-queries that do not influcence the matching of documents but contribute to the score of documents that are retrieved by the `matching_query`. A `querqy` query allows to control two main types of boosting queries:

1.  `rewritten_queries` - boost queries that are produced as part of query rewriting (#7). 
1.  `phrase_boosts` - (partial) phrases that are derived from the query string for boosting documents that contain corresponding phrase matches (#12)

Scores from both types of boosting queries will be added to the score of the matching query.

##### (`boosting_queries.rewritten_queries)` `use_field_boost` (#8)  

Default value: `true`

If `true`, the scores of the boost queries will include field weights. A field boost of `1.0` will be used otherwise.

##### (`boosting_queries.rewritten_queries)` `similarity_scoring` (#9)

Values: dfc (default), on, off

Controls how Lucene's scoring implementation (= *similarity*) is used when the boosting query is expanded across fields.

- `dfc`: 'document frequency correction' - use the same document frequency (*df*) value for all term queries that are produced from the same boost term. Querqy will use the maximum document frequency of the produced terms as the *df* value for all of them. If the `matching_query` also uses `similarity_scoring=dfc` (see [here](https://github.com/renekrie/querqy-elasticsearch#matching_query-similarity_scoring-1)), the maximum *df* of the matching query will be added to the *df* of the boosting query terms in order to put the *df*s in the two query parts on a comparable scale and to avoid giving extremely high weight to very sparse boost terms.  
- `off`: Ignore the output of Lucene's similarity scoring. 
- `on`: Use Lucene's similarity scoring output. 


##### (`boosting_queries.rewritten_queries)` `positive_query_weight` / `negative_query_weight` (#10 / #11)

Default value: `1.0`

Query rewriting in Querqy can produce boost queries that either promote matching documents to the top of the search result or that push matching documents to the bottom of the search result list. The `UP` rules in the Common Rules rewriter are an example of a positive boost query that promotes documents to the top of the search result. `DOWN` rules are an example of negative boost queries, which push the documents down the search result list.

Scores of postive boost queries are multiplied with `positive_query_weight`. Scores of negative boost queries are multiplied with `negative_query_weight`. Both weights must be positive decimal numbers. Note that increasing the value of `negative_query_weight` means to demote matching documents more strongly. 

##### (`boosting_queries.phrase_boosts)` `full` / `bigram` / `trigram` / `tie_breaker` (#12 - #18)

Unlike `rewritten_queries`, `phrase_boosts` can be applied regardless of query rewriting. If enabled, a boost query will be created from phrases which are derived from the query string. Documents matching this boost query will be promoted to towards the top of the search result.

The objects `full`, `bigram` and `trigram` control how phrase boost queries will be formed:

- `full`: boosts documents that contain the entire input query as a phrase
- `bigram`: creates phrase queries for boosting from pairs of adjacent query tokens
- `trigram`: creates phrase queries for boosting from triples of adjacent query tokens

The `fields` lists (#14 for `full`) defines the fields and their weights in which the phrases will be looked up. The `slop` defines the number of positions the phrase tokens are allowed to shift while still counting as a phrase. A `slop`of two or greater allows for token transposition (compare Elasticsearch's [*Match phrase query*](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase.html)). The default `slop` is `0`.

Depending on the number of query tokens, a matching `full` phrase query can imply one or more `bigram` and `trigram` matches. The scores of these matches will be sumed up, which can quickly result in a very large score for documents that match a long full query phrase. Setting `tie_breaker` for `phrase_boosts` to a low value will reduce this aggregation effect (#18). Querqy will use the highest score amongst `full`, `bigram` and `trigram` matches and multiply the score of the other matches with the `tie_breaker` value. A `tie_breaker` of `0.0` - which is the default value - will only use the highest score.

The concept of phrase boosting is very similar to the pf/pf2/pf3/ps/ps2/ps3 parameters of Solr's [Extended DisMax](https://lucene.apache.org/solr/guide/the-extended-dismax-query-parser.html) / [DisMax](https://lucene.apache.org/solr/guide/the-dismax-query-parser.html#the-dismax-query-parser) query parsers. However, Querqy adds control over the aggregation of the scores from the different phrase boost types using the `tie_breaker`.

The score produced by `phrase_boosts` is added to the boost of the `matching_query`.


#### Controlling generated query parts (#19)

The parameters in the `generated` object control the fields and the weights of the query parts that were created during query rewriting. These query parts can occur in the matching query - for example, synonyms or (de)compound words - or in boosting queries (see `rewritten_queries` in [`boosting_queries`](https://github.com/renekrie/querqy-elasticsearch#boosting-queries-6) above).

##### (`generated`) `query_fields` (#20)

Default value: copy from global `query_fields`

The list of fields and their weights for matching generated query terms. This overrides the global `query_fields` that are used for the matching query. If no `query_fields` are specified for the generated query parts, the global `query_fields` will be used.

##### (`generated`) `field_boost_factor` (#21)

Default value: `1.0`

A factor that is multiplied with the field weights of the generated query terms. The factor is applied regardless of where the `query_fields` for generated terms are defined, i.e. in the `query_fields` of the `generated` object or globally. This factor can be used to apply a penalty to all terms that were not entered by the user but inserted as part of the query rewriting.



