package ir.jimbo.commons.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Setter
@Getter
@NoArgsConstructor
public class Page {
    private String url;
    private String title;
    private Map<String, String> metadata = new HashMap<>();
    private Map<String, String> links = new HashMap<>();
    private List<String> h1List = new ArrayList<>();
    private List<String> h2List = new ArrayList<>();
    private List<String> h3to6List = new ArrayList<>();
    private List<String> plainTextList = new ArrayList<>(); //<p>, <pre> and <span> tags
}
