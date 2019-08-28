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
public class Link implements Serializable {
    private String src;
    private String dst;
}
