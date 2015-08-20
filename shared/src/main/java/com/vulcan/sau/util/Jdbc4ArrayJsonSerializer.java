package com.vulcan.sau.util;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;
import org.postgresql.jdbc4.Jdbc4Array;

/**
 * Example of a Jackson custom serializer.
 *
 */
public class Jdbc4ArrayJsonSerializer extends JsonSerializer<Jdbc4Array> {

	@Override
	public void serialize(Jdbc4Array value, JsonGenerator jgen,
			SerializerProvider provider) throws IOException,
			JsonProcessingException {

		// TODO: Convert a Jdbc4Array to a JSON array.
		jgen.writeString("[]");
		
	}


}
