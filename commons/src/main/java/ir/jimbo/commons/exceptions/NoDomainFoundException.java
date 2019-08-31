package ir.jimbo.commons.exceptions;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class NoDomainFoundException extends JimboException {
    public NoDomainFoundException(String url) {
        super(url);
    }
}
