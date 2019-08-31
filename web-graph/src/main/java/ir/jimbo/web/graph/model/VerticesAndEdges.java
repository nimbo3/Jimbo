package ir.jimbo.web.graph.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Setter
@Getter
@ToString
@NoArgsConstructor
public class VerticesAndEdges {
    List<GraphVertex> nodes;
    List<GraphEdge> links;
}
