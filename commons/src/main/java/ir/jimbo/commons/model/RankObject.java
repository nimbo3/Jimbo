package ir.jimbo.commons.model;

import lombok.*;

import java.io.Serializable;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class RankObject implements Serializable {
    private String id;
    private Double rank;
}
