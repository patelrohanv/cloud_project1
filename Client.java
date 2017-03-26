import java.io.File;

public class Client{
    public static void main(String[] args){
        File directory = new File(args[0]);
        File[] contents = directory.listFiles();
        int i = 0;
        for ( File f : contents) {
            //System.out.println(f.toString());
        }


    }
}