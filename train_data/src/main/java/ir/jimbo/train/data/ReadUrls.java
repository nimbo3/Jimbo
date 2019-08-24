package ir.jimbo.train.data;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * UNUSED
 * TODO delete this class
 */
public class ReadUrls extends Thread {

    private final Logger logger = LogManager.getLogger(this.getClass());
    private String filePath;

    public ReadUrls() throws FileNotFoundException {
        filePath = "urls.txt";
    }

    @Override
    public void run() {
        try(BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            for(String line; (line = br.readLine()) != null; ) {
                // process the line.
            }
            // line is not visible here.
        } catch (IOException e) {
            logger.error(e);
        }
    }
}
