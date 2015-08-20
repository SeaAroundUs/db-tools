package com.vulcan.sau.backend;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

public enum RouterConfig {
    ACTIONSTORE_ROOT,
    ACTIONSTORE_ROOT_QA;

    public final String stringValue() {
        return valFromBundles(this.name());
    }
    
    public int intValue() throws NumberFormatException
    {
        return Integer.parseInt(stringValue());
    }
    
    public long longValue() throws NumberFormatException
    {
        return Long.parseLong(stringValue());
    }
    
    public float floatValue() throws NumberFormatException
    {
        return Float.parseFloat(stringValue());
    }
    
    public boolean booleanValue() 
    {
        return Boolean.parseBoolean(stringValue());
    }
    
    public static String valFromBundles(String name) {
        for (ResourceBundle bundle : bundles) {
            if (bundle != null && bundle.containsKey(name)){
                return bundle.getString(name);
            }
        }
        throw new MissingResourceException("Config property "  + name + " not found", RouterConfig.class.getSimpleName(), name);
    }
    private static ResourceBundle bundles[] = null;

    public static void init() {
		String environment = System.getProperty("ingestion.env");
		environment = environment == null ? "default" : environment;

		RouterConfig.init(environment);

    }
    public static void init(String clusterName) {
        if (bundles != null) {
            return;
        }
        if (clusterName == null) {
            clusterName = ResourceBundle.getBundle("site").getString("CLUSTER_NAME");
        }
        bundles = new ResourceBundle[] {
                ResourceBundle.getBundle(clusterName),
                ResourceBundle.getBundle("default")
        };
    }
    
    public static boolean isInitialized() {
    	return bundles != null;
    }
}
