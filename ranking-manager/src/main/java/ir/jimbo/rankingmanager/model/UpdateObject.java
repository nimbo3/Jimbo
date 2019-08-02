package ir.jimbo.rankingmanager.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class UpdateObject implements Serializable {
    private String id;
    private int numberOfReferences;
    private List<String> topAnchors;

    @Override
    public String toString() {
        return "UpdateObject{" +
                "id='" + id + '\'' +
                ", numberOfReferences=" + numberOfReferences +
                ", topAnchors=" + Arrays.toString(topAnchors.toArray()) +
                '}';
    }
}
