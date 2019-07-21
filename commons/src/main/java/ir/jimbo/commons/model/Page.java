package ir.jimbo.commons.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Page {
    String url;
    String title;
    Map<String, String> metadata = new HashMap<>();
    Map<String, String> links = new HashMap<>();
    Set<String> h1 = new HashSet<>();
    Set<String> h2 = new HashSet<>();
    Set<String> h3to6 = new HashSet<>();
    Set<String> pres = new HashSet<>();
}
