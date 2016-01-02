package master;

/**
 *
 * @author Yolanda de la Hoz Simon - 53826071E
 * @version 1.2 2 de Enero de 2016
 */
public class hbaseApp {
	private int startTS;
	private int endTS;
	private int N;
	private String[] languages;
	private String outputFolderPath;
	private String dataFolder;

	/**
	 * Method to perform the first query
	 * Given a language (lang), do find the Top-N most used words for the given language in
	 * a time interval defined with a start and end timestamp. Start and end timestamp are
	 * in milliseconds.      
	 */
	private void firstQuery() {
	}

	/**
	 * Method to perform the second query
	 * Do find the list of Top-N most used words for each language in a time interval defined
     * with the provided start and end timestamp. Start and end timestamp are in
     * milliseconds. 
	 */
	private void secondQuery() {
	}

	/**
	 * Method to perform the third query
	 * Do find the Top-N most used words and the frequency of each word regardless the
     * language in a time interval defined with the provided start and end timestamp. Start
     * and end timestamp are in milliseconds.      
	 */
	private void thirdQuery() {
	}

	/**
	 * Method to load the files in hbase
	 */
	private void load() {
	}

	/**
	 * Method to set the necessary parameters
	 * @param args Arguments passed by command line       
	 */
	private void setContext(String[] args, int mode) {
		switch (mode) {
		case 1: 	dataFolder=args[1];
		break;
		case 2: 	startTS=Integer.parseInt(args[1]);
		endTS=Integer.parseInt(args[2]);
		N=Integer.parseInt(args[3]);
		languages=args[4].split(",");;
		outputFolderPath=args[5];
		break;
		case 3: 	startTS=Integer.parseInt(args[1]);
		endTS=Integer.parseInt(args[2]);
		N=Integer.parseInt(args[3]);
		languages=args[4].split(",");;
		outputFolderPath=args[5];
		break;
		case 4: 	startTS=Integer.parseInt(args[1]);
		endTS=Integer.parseInt(args[2]);
		N=Integer.parseInt(args[3]);
		outputFolderPath=args[4];
		break;     	
		}
	}

	/**
	 * Method to start the hbase app with the selected query
	 * @param mode Mode to start the app. Mode 1 reads from file. Mode 2 reads from twitter API.     
	 */
	private void start(int mode) {

		switch (mode) {
		case 1: 	load();
		break;
		case 2: 	firstQuery();
		break;
		case 3: 	secondQuery();
		break;
		case 4: 	thirdQuery();
		break;     	
		}
	}

	/**
	 * Main method
	 * @param args Arguments: mode dataFolder startTS endTS N language outputFolder    
	 * @throws java.lang.Exception    
	 */
	public static void main(String[] args) throws Exception {
		int mode = 0;
		if (args.length > 0) {
			System.out.println("Started hbaseApp with mode: " + args[0]);
			try {
				mode = Integer.parseInt(args[0]);
				if (mode==1 && args.length!=2 ) {
					System.out.println("To start the App with mode 1 it is required the mode and the dataFolder");
					System.exit(1);  
				}
				if (mode==2 && args.length!=6 ) {
					System.out.println("To start the App with mode 2 it is required the mode startTS endTS N language outputFolder");
					System.exit(1);  
				}
				if (mode==3 && args.length!=6 ) {
					System.out.println("To start the App with mode 3 it is required the mode startTS endTS N language outputFolder");
					System.exit(1);  
				}     
				if (mode==4 && args.length!=5 ) {
					System.out.println("To start the App with mode 4 it is required the mode startTS endTS N outputFolder");
					System.exit(1);  
				}  

				hbaseApp app = new hbaseApp();
				app.setContext(args,mode);
				app.start(mode);

			} catch (Exception e) {
				System.out.println(e.getMessage());
			}

		} else {
			System.out.println("Arguments: mode dataFolder startTS endTS N language outputFolder");
			System.exit(1);
		}

	}

}