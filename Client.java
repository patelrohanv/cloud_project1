import java.io.File;
import java.util.Scanner;

public class Client{
    public static void main(String[] args){
        System.out.println("__________________________________");
        System.out.println("Welcome to tiny-Google");
        System.out.println("__________________________________");
        int input;
        Scanner kbd = new Scanner(System.in);
        if (!indexed()) {
            while(true) {
                System.out.println("No index found on disk.\nWould you like to generate one now?\n\t1. Yes\n\t2. No");
                input = kbd.nextInt();
                if(input > 2 || input < 1){
                    System.out.println("Not a valid option. Please try again.\n");
                }
                else if (input == 1) {
                    index();
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
        System.out.println("__________________________________");
        System.out.println("Creating index from directory.");
        System.out.println("__________________________________");

        System.out.println("Please select the directory containing the files to be indexed from the following:\t");
        Scanner kbd = new Scanner(System.in);
        File currDir = new File(".");
        currDir = new File(currDir.getAbsolutePath());
        File[] contents = currDir.listFiles();
        while(true) {
            for (int i = 0; i < contents.length; i++) {
                File f = contents[i];
                System.out.println("\t"+i+".\t"+f.getName());
            }
            System.out.println("\t"+(contents.length+1)+".\tCancel");
            int selection = kbd.nextInt();
            if (selection < 0 || selection > contents.length+1) {
                System.out.println("Invalid Selection.\nPlease try again.");
            }
            else if (selection == contents.length+1) {
                System.out.println("Cancelling Indexing Operation.");
                return;
            }
            else {
                File indexDir = contents[selection];
                if (!contents[selection].isDirectory())
                {
                    System.out.println(indexDir.toString() + " is not a directory.\nPlease try again.");
                }
                else {
                    System.out.println("Generating index from the files in directory " + indexDir.toString());
                    /*File directory = new File(args[0]);
                    File[] contents = directory.listFiles();
                    int i = 0;
                    for ( File f : contents) {
                        //System.out.println(f.toString());
                    }*/
                    break;
                }
            }
        }
        System.out.println("__________________________________");
        return;
    }

    public static void search() {
        System.out.println("__________________________________");
        System.out.println("Searching...");
        System.out.println("__________________________________");
    }

    public static void indexFile() {
        System.out.println("__________________________________");
        System.out.println("Indexing file.");
        System.out.println("__________________________________");
    }
}
