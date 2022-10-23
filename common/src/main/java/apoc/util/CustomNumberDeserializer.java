package apoc.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.exc.InputCoercionException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

public class CustomNumberDeserializer extends StdDeserializer {

    public CustomNumberDeserializer(Class<?> vc) {
        super(vc);
    }
    
    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        
        if (p.hasToken(JsonToken.VALUE_NUMBER_FLOAT)) {

            // we convert it either to a double or plain string
            final double doubleValue = p.getDoubleValue();
            if (doubleValue != Double.POSITIVE_INFINITY && doubleValue != Double.NEGATIVE_INFINITY) {
                return doubleValue;
            }

            return p.getDecimalValue().toPlainString();
        }
        
        if (p.hasToken(JsonToken.VALUE_NUMBER_INT)) {

            try {
                return p.getLongValue();
            } catch (InputCoercionException e) {
                return p.getValueAsString();
            }
        }
        
        return p.getNumberValue();
    }
    
}
