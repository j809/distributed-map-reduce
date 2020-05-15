import java.io.File;
import java.io.IOException;

public class JavaProcess {
    private Class klass;

    public  JavaProcess(Class klass) {
        this.klass = klass;
    }

    public Process exec() throws IOException {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome +
                File.separator + "bin" +
                File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = klass.getName();

        ProcessBuilder builder = new ProcessBuilder(javaBin, "-cp", classpath, className);
        return builder.inheritIO().start();
    }
}
