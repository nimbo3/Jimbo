package ir.jimbo.commons.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class Page {
    private String url;
    private String title;
    private Map<String, String> metadata;
    private List<Link> links;
    private List<String> h1;
    private List<String> h2;
    private List<String> h3to6;
    private List<String> plainText; //<p> and <pre> tags
}