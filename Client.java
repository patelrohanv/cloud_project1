import java.io.File;
import java.util.Scanner;

public class Client{
    public static void main(String[] args){
        int input;
        Scanner kbd = new Scanner(System.in);
        if (!indexed()) {
            while(true) {
                System.out.println("No index found on disk.\nWould you like to generate one now?\n\t1. Yes\n\t2. No");
                input = kbd.nextInt();
                if(input > 2 || input < 1){
                    System.out.println("Not a valid option. Please try again.");
                }
                else if (input == 1) {
                    System.out.print("Please input the directory containing the files to be indexed:\t");
                    String bookDir = kbd.nextLine();
                    break;
                }
                else {
                    System.out.println("Ok. Note: No searches will be possible until an index is generated.");
                    break;
                }
            }
        }

        do{
            System.out.println("Enter an option:\n\t1. Search for a word \n\t2. Add a document \n\t3. Generate Index from Directory\n\t4. Quit");
            input = kbd.nextInt();
            if(input > 4 || input < 1){
                System.out.println("Not a valid option");
                continue;
            }
            if (input==1 && !indexed()) {
                System.out.println("Search not possible untill index is generated.");
            }
            else if (input == 1) {
                search();
            }
            else if (input == 2) {
                indexFile();
            }
            else if (input == 3) {
                index();
            }
        }while(input != 4);
        System.out.println("Goodbye!");
    }

    public static boolean indexed(){
        File currDir = new File(".");
        currDir = new File(currDir.getAbsolutePath());
        File[] contents = currDir.listFiles();

        for (File f :contents) {
            if (f.toString().equals("WHATEVER WE NAME OUR INDEX")) {
                return true;
            }
        }
        return false;
    }

    public static void index(){
        System.out.println("Creating index from directory.");
        /*File directory = new File(args[0]);
        File[] contents = directory.listFiles();
        int i = 0;
        for ( File f : contents) {
            //System.out.println(f.toString());
        }*/
    }

    public static void search() {
        System.out.println("Searching...");
    }

    public static void indexFile() {
        System.out.println("Indexing file.");
    }
}
