package edu.hanyang.utils;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarFile;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

public class SubmitClassLoader {

     public static void loadAllSubmitInstance(final String submitJarPath) {
         File submitJarFile = new File(submitJarPath);

         if (!submitJarFile.exists()) {
             System.out.println("Submit jar file is not found!");
             return;
         }

         try (FileInputStream fin = new FileInputStream(submitJarPath);
              JarInputStream jarInputStream = new JarInputStream(fin)) {

             ClassLoader classLoader = new URLClassLoader(new URL[] {
                     submitJarFile.toURI().toURL()
             }, Thread.currentThread().getContextClassLoader());
             List<String> classNames = getClassNamesFromJar(jarInputStream);

             for (String className: classNames) {
                 Class<?>  cls = Class.forName(className, true, classLoader);
                 cls.newInstance();
             }

         } catch (Exception exc) {
 //            exc.printStackTrace();
         }
     }

    public static <T> T getSubmitInstance(final String submitJarPath, final String className) {
        File submitJarFile = new File(submitJarPath);

        if (!submitJarFile.exists()) {
            System.out.println("Submit jar file is not found!");
            return null;
        }

        try {

            System.out.println(submitJarFile.toURI());
            System.out.println(submitJarFile.toURI().toURL());
            URLClassLoader classLoader = new URLClassLoader(new URL[] {submitJarFile.toURI().toURL()}, Thread.currentThread().getContextClassLoader());
            for (URL url : classLoader.getURLs()) {
                System.out.println(url);
            }

            // Class<T> cls = (Class<T>) classLoader.loadClass(className);
            Class<T> cls = (Class<T>) Class.forName(className, true, classLoader);
            classLoader.close();

            return cls.newInstance();
        } catch (Exception exc) {
            exc.printStackTrace();
            return null;
        }
    }

    private static List<String> getClassNamesFromJar(JarInputStream jarFile) throws Exception {
        List<String> classNames = new ArrayList<>();
        try {
            //JarInputStream jarFile = new JarInputStream(jarFileStream);
            JarEntry jar;

            //Iterate through the contents of the jar file
            while (true) {
                jar = jarFile.getNextJarEntry();
                if (jar == null) {
                    break;
                }
                //Pick file that has the extension of .class
                if ((jar.getName().endsWith(".class"))) {
                    String className = jar.getName().replaceAll("/", "\\.");
                    String myClass = className.substring(0, className.lastIndexOf('.'));
                    classNames.add(myClass);
                }
            }
        } catch (Exception e) {
            throw new Exception("Error while getting class names from jar", e);
        }
        return classNames;
    }
}
