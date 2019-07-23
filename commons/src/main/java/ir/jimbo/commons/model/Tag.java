package ir.jimbo.commons.model;

import java.util.HashMap;
import java.util.Map;

public class Tag {
    private String name;
    private String content;
    private Map<String, String> attrs;

    public String getName() {
        return name;
    }

    public String getContent() {
        return content;
    }

    public Map<String, String> getAttrs() {
        return attrs;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAttrs(Map<String, String> attrs) {
        this.attrs = attrs;
    }

    public Tag(String name, String content) {
        this.name = name;
        this.content = content;
        this.attrs = new HashMap<>();
    }


}
