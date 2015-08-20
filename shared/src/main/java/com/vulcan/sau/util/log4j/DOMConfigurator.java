package com.vulcan.sau.util.log4j;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.log4j.helpers.FileWatchdog;
import com.vulcan.sau.util.Utilities;

public class DOMConfigurator extends org.apache.log4j.xml.DOMConfigurator {
    
    public static void configureAndWatch( String fileName, String resourceName ) {
        configureAndWatch( fileName, resourceName, FileWatchdog.DEFAULT_DELAY );
    }
    
    public static void configureAndWatch( String fileName, String resourceName, long delay ) {
        File currFile = null;
        
        InputStream input   = null;
        OutputStream output = null;
        
        try {
            if (fileName != null && fileName.length() > 0) {
                currFile = new File( fileName );
                
                if ( ! currFile.exists() ) {
                    input = Utilities.getResourceStream( DOMConfigurator.class, resourceName );
                    
                    if ( input != null )
                    {
                        if ( currFile.getParentFile() != null ) {
                            currFile.getParentFile().mkdirs();
                        }
                        
                        output = new FileOutputStream( currFile );
            
                        byte[] temp = new byte[128];
            
                        while ( input.available() > 0 ) {
                            int nb = input.read(temp);
                            output.write(temp, 0, nb);
                        }
                        
                        output.flush();
                    }
                }
            }
        }
        catch( Exception e ) {
            e.printStackTrace();
        }
        finally {
            try { input.close(); } catch ( Exception e ) { }
            try { output.close(); } catch ( Exception e ) { }
        }
        
        if (currFile == null || !(currFile.exists() || currFile.canWrite())) {
            try {
                if (resourceName != null && resourceName.length() > 0) {
                    currFile = Utilities.getFileFromResource( DOMConfigurator.class, resourceName );
                }
                else {
                    System.err.println( "No file or resource name specified for log4j configuration.");
                }
            }
            catch( Exception e ) {
                e.printStackTrace();
            }
        }
            
        if ( currFile != null ) {
            configureAndWatch( currFile.getAbsolutePath(), delay );
        }
        else {
            System.err.println( "Unable to find log4j configuration." );
        }
    }
}
