package querqy.elasticsearch.rewriterstore;

public interface Constants {

    // TODO: configurable?
    String QUERQY_INDEX_NAME = ".querqy";

    String SETTINGS_QUERQY_INDEX_NUM_REPLICAS = "querqy.store.replicas";

    int DEFAULT_QUERQY_INDEX_NUM_REPLICAS = 1;
}
