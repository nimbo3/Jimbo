package ir.jimbo.commons.model;

import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Page {
    String url;
    String title;
    Map<String, String> metadata = new HashMap<>();
    @Getter
    Map<String, String> links = new HashMap<>();
    List<String> h1 = new ArrayList<>();
    List<String> h2 = new ArrayList<>();
    List<String> h3to6 = new ArrayList<>();
    List<String> plainText = new ArrayList<>(); //<p> and <pre> tags
}