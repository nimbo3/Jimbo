package ir.jimbo.rankingmanager.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class RankObject implements Serializable {
    private String id;
    private Double rank;

    @Override
    public String toString() {
        return "RankObject{" +
                "id='" + id + '\'' +
                ", rank=" + rank +
                '}';
    }
}
