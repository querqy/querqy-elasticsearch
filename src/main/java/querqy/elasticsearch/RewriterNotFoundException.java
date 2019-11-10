package querqy.elasticsearch;

import java.io.IOException;

public class RewriterNotFoundException extends IOException {

    public RewriterNotFoundException(final String message) {
        super(message);
    }
}
