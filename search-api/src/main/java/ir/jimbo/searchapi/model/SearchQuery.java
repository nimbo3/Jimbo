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
public class SearchQuery implements Serializable {
    private String exactQuery;
    private String normalQuery;
    private String fuzzyQuery;


    @Override
    public String toString() {
        return "SearchQuery{" +
                "exactQuery='" + exactQuery + '\'' +
                ", normalQuery='" + normalQuery + '\'' +
                ", fuzzyQuery='" + fuzzyQuery + '\'' +
                '}';
    }
}
