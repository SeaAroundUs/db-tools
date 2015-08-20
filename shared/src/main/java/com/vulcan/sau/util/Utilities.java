package com.vulcan.sau.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.log4j.Logger;

public class Utilities {

    /**
     * Return a resource that is on the classpath as a {@link File}. Useful for unit-testing file
     * resources.
     * 
     * @param clazz
     *            The resource will be loaded using the passed in Class object.
     *            This is necessary to pacify the JVM classloader security model
     *            which does not allow a class to load a resource from anywhere
     *            but it's own .jar.
     * @param filename
     *            The name of the file to load. Note, the name should be
     *            absolute, "/data/filename, not data/filename".
     * @return A File representing the resource. The returned File is a temp
     *         file with a meaningless location that the VM will delete on exit.
     * @throws Exception
     */
    //@SuppressWarnings("unchecked")
	public static File getFileFromResource(Class clazz, String filename) throws Exception {
        InputStream input = clazz.getResourceAsStream(filename);
        if (input == null) {
            Logger.getLogger(clazz).warn("unable to find resource: " + filename);
            return null;
        }

        File f = File.createTempFile(clazz.getName(), null);
        f.deleteOnExit();
        OutputStream output = new FileOutputStream(f);
        byte[] temp = new byte[128];
        while (input.available() > 0) {
            int nb = input.read(temp);
            output.write(temp, 0, nb);
        }
        input.close();
        output.close();

        return f;
    }

    //@SuppressWarnings("unchecked")
	public static InputStream getResourceStream(Class clazz, String resource) {
        final InputStream resourceStream = clazz.getResourceAsStream(resource);
        if (resourceStream == null) {
            Logger.getLogger(clazz).warn("unable to find resource: " + resource);
            return null;
        }
        
        return resourceStream;
    }
}
