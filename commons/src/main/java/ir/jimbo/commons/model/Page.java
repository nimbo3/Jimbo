package ir.jimbo.commons.model;

import java.util.*;

public class Page {
    String url;
    String title;
    Map<String, String> metadata = new HashMap<>();
    Map<String, String> links = new HashMap<>();
    List<String> h1 = new ArrayList<>();
    List<String> h2 = new ArrayList<>();
    List<String> h3to6 = new ArrayList<>();
    List<String> plainText = new ArrayList<>(); //<p> and <pre> tags
}