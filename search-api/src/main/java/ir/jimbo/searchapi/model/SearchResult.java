package ir.jimbo.searchapi.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class SearchResult implements Serializable {
    private String title;
    private String text;
    private String url;
}
