package ir.jimbo.crawler.exceptions;

import ir.jimbo.commons.exceptions.JimboException;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class NoDomainFoundException extends JimboException {
    public NoDomainFoundException(String url) {
        super(url);
    }
}
