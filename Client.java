import java.io.File;
import java.util.Scanner;

public class Client{
    public static void main(String[] args){
        File directory = new File(args[0]);
        File[] contents = directory.listFiles();
        int i = 0;
        for ( File f : contents) {
            //System.out.println(f.toString());
        }

        int input = 0;
        Scanner kbd = new Scanner(System.in);
        do{
            if(input > 3 || input < 1){
                System.out.println("Not a valid option");
                continue;
            }
            System.out.println("Enter an option:\n\t1. Search for a word \n\t2. Add a document \n\t3. Quit");
            input = kbd.nextInt();
        }while(input != 3);
    }
}