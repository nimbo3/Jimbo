package ir.jimbo.espageprocessor.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import ir.jimbo.commons.model.Page;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class KafkaPageJsonDeserializer implements Deserializer {
    public KafkaPageJsonDeserializer() {
    }

    private Logger logger = LogManager.getLogger(this.getClass());

    private Class type = Page.class;

    public KafkaPageJsonDeserializer(Class type) {
        this.type = type;
    }

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        Page obj = null;
        try {
            obj = (Page) mapper.readValue(bytes, type);
        } catch (Exception e) {

            logger.error(e.getMessage());
        }
        return obj;
    }

    @Override
    public void close() {

    }
}
