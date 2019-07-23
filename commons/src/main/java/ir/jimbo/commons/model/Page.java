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
    private List<HtmlTag> metadata = new ArrayList<>();
    private List<HtmlTag> links = new ArrayList<>();
    private List<HtmlTag> h1List = new ArrayList<>();
    private List<HtmlTag> h2List = new ArrayList<>();
    private List<HtmlTag> h3to6List = new ArrayList<>();
    private List<HtmlTag> plainTextList = new ArrayList<>(); //<p>, <pre> and <span> tags
}
