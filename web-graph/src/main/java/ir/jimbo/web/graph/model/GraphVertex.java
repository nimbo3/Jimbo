package ir.jimbo.web.graph.model;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class GraphVertex {
    private String id;
    private double pagerank;
    private double color;
}
