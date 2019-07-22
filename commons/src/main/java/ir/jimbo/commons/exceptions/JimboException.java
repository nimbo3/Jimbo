package ir.jimbo.commons.exceptions;

public class JimboException extends RuntimeException {
    public JimboException(String message) {
        super(message);
    }

    public JimboException() {
        super();
    }
}
