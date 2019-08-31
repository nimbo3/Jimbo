package ir.jimbo.web.graph.model;

import lombok.*;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class GraphEdge {
    private String source;
    private String destination;
    private String anchor;
}
