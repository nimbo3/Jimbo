package ir.jimbo.searchapi.model;

import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Setter
public class Graph {
    private List<Node> nodes = new ArrayList<>();
    private List<Link> links = new ArrayList<>();
}
