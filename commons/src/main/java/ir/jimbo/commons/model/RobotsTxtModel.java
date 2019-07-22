package ir.jimbo.commons.model;

import com.panforge.robotstxt.RobotsTxt;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class RobotsTxtModel implements Serializable {
    private RobotsTxt robotsTxt;
}
