package ir.jimbo.web.graph.model;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class GraphEdge {
    private String src;
    private String dst;
    private String anchor;
}
