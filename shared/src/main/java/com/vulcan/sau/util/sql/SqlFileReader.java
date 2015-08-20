package com.vulcan.sau.util.sql;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

public class SqlFileReader {
    
    private byte recordDelim;
    
    private InputStream in;
    
    private int character = -1;
    
    public SqlFileReader(String file, byte recordDelim) throws UnsupportedEncodingException, FileNotFoundException, IOException {
        this.recordDelim = recordDelim;
        
        in = getClass().getResourceAsStream(file);
    }

    /**
     * Returns the record delimter for the file.
     * 
     * @return A byte used to delimit records in the file.
     */
    public byte getRecordDelim() {
        return recordDelim;
    }
    
    /**
     * When done reading from the file call the close method to close the reader.
     */
    public void close() throws IOException {
        if ( in != null ) {
            in.close();
        }
    }
    
    /**
     * Retrieves the list of values for a row from the {#link InputStreamReader}.  If the
     * row is a duplicate of another row then the result will be an empty list of values.
     * 
     * @return A list of values from the stream.
     * @throws IOException
     */
    public String findSqlString() throws IOException {
        StringBuilder values = null;
                                    
        character = in.read();
        
        if ( character != recordDelim && character != -1 ) {
            values = new StringBuilder();
  
            do {
                values.append( (char) character );
                character = in.read();
            }
            while ( character != recordDelim && character != -1 );

            return values.toString().trim();
        }
        
        return null;
    }
}
