package ir.jimbo.searchapi.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class HRow {
    private String rowKey;
    private String qualifier;
    private String value;
}
