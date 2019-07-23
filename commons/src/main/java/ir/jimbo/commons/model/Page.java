package ir.jimbo.commons.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;


@Setter
@Getter
@NoArgsConstructor
public class Page {
    private String url;
    private String title;
    private List<Tag> metadata = new ArrayList<>();
    private List<Tag> links = new ArrayList<>();
    private List<Tag> h1List = new ArrayList<>();
    private List<Tag> h2List = new ArrayList<>();
    private List<Tag> h3to6List = new ArrayList<>();
    private List<Tag> plainTextList = new ArrayList<>(); //<p>, <pre> and <span> tags
}
