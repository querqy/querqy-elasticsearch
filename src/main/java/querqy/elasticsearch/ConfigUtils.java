package querqy.elasticsearch;

import org.elasticsearch.SpecialPermission;
import querqy.trie.TrieMap;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

public interface ConfigUtils {


    static String getStringArg(final Map<String, Object> config, final String name, final String defaultValue) {
        final String value = (String) config.get(name);
        return value == null ? defaultValue : value;
    }

    static Optional<String> getStringArg(final Map<String, Object> config, final String name) {
        return Optional.ofNullable((String) config.get(name));
    }

    static <T extends Enum<T>> Optional<T> getEnumArg(final Map<String, Object> config, final String name,
                                                  final Class<T> enumClass) {
        final String value = (String) config.get(name);
        return (value == null) ? Optional.empty() : Optional.of(Enum.valueOf(enumClass, value));
    }


    static <T> T getArg(final Map<String, Object> config, final String name, final T defaultValue) {
        return (T) config.getOrDefault(name, defaultValue);
    }

    static TrieMap<Boolean> getTrieSetArg(final Map<String, Object> config, final String name) {
        final TrieMap<Boolean> result = new TrieMap<>();
        final Collection<String> collectionArg = (Collection<String>) config.get(name);
        if (collectionArg != null) {
            for (final String word : new HashSet<>(collectionArg)) {
                result.put(word, Boolean.TRUE);
            }
        }
        return result;
    }

    static <V> V getInstanceFromArg(final Map<String, Object> config, final String name, final V defaultValue) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }


        return AccessController.doPrivileged(
            (PrivilegedAction<V>) () -> {
                final String classField = (String) config.get(name);
                if (classField == null) {
                    return defaultValue;
                }

                final String className = classField.trim();
                if (className.isEmpty()) {
                    return defaultValue;
                }

                try {
                    return (V) Class.forName(className).getDeclaredConstructor().newInstance();
                } catch (final Exception e) {
                    throw new RuntimeException(e);
                }
            });

    }

}
