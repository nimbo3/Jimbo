package ir.jimbo;


import java.io.IOException;

public class App {
    public static void main( String[] args ) throws IOException {
        Shuffler shuffler = new Shuffler();
        shuffler.start();
        Runtime.getRuntime().addShutdownHook(new Thread(shuffler::close));
    }
}
