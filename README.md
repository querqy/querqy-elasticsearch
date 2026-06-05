![Querqy for ES Build](https://github.com/querqy/querqy-elasticsearch/actions/workflows/maven.yml/badge.svg) [ ![Download](https://img.shields.io/maven-central/v/org.querqy/querqy-elasticsearch.svg?label=Querqy%20for%20Elasticsearch) ](https://search.maven.org/search?q=g:%22org.querqy%22%20AND%20a:%22querqy-elasticsearch%22)

Developer channel: Join #querqy on the [Search Relevancy Slack space](https://opensourceconnections.com/slack)

:warning: **IMPORTANT: Starting from Querqy for Elasticsearch 9.3.3 the name of the index in which Querqy stores rewriter configurations has changed** from `.querqy` to `querqy_store`. This should usually only matter to you if you are installing Elasticsearch and Querqy over older versions. You will either have to send all rewriter configurations to Elasticsearch/Querqy again or alternatively copy or clone the `.querqy` index before updating your Elasticsearch (see `src/main/resources/querqy-mapping.json` for the index mapping; make sure you have one or more replicas defined).


# Querqy for Elasticsearch

This is the Elasticsearch plugin version of Querqy - a query preprocessing framework for Java-based search engines.


## Documentation and 'Getting started'


[Visits docs.querqy.org/querqy/ for detailed documentation.](https://docs.querqy.org/querqy/index.html) 

Check out [Querqy.org](https://querqy.org) for related projects that help you speed up search software development.

## License

Querqy is licensed under the [Apache License, Version 2](http://www.apache.org/licenses/LICENSE-2.0.html).


