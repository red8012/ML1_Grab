import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Grab {
	static int YYYY, MM, DD, yyyy, mm, dd;
	static Calendar start, current, end;
	static HashMap<String, ArrayList<String>> map;

	public static void main(String[] args) {
		try {
			if (args.length == 1) {
				if (args[0].equals("update")) {
					determineUpdateRange();
					startWorking(true);
					return;
				}
			} else if (args.length == 6) {
				try {
					determineGrabRange(args);
					startWorking(false);
					return;
				} catch (NumberFormatException e) {
				}
			}

			System.err.println("\nUsage:\n" +
					"1. Batched grab mode\n" +
					"\tjava Grab YYYY MM DD yyyy mm dd\n" +
					"\t\t-> the program will grab data from YYYY MM DD to yyyy mm dd.\n\n" +
					"2. Update mode\n" +
					"\tjava Grab update\n" +
					"\t\t-> the program will update data to the newest version.\n");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static void startWorking(boolean isUpdateMode) {
		System.out.println((isUpdateMode ? "You are using update mode. \tThe data will be downloaded" :
				"The data will be grabbed") +
				"from [ " + start.getTime().toString().replace("00:00:00 CST ", "") +
				" ] to [ " + end.getTime().toString().replace("00:00:00 CST ", "") + " ]\n" +
				"Downloading data, please wait...");
		try {
			grab();
			System.out.println("Arranging data, please wait...");
			arrange();
			System.out.println("Saving arranged data to files, please wait...");
			writeArranged(isUpdateMode);
			System.out.println("Finished!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	static void determineGrabRange(String args[]) throws NumberFormatException {
		YYYY = Integer.parseInt(args[0]);
		MM = Integer.parseInt(args[1]);
		DD = Integer.parseInt(args[2]);
		yyyy = Integer.parseInt(args[3]);
		mm = Integer.parseInt(args[4]);
		dd = Integer.parseInt(args[5]);
		start = new GregorianCalendar(YYYY, MM - 1, DD, 0, 0, 0);
		current = new GregorianCalendar(YYYY, MM - 1, DD, 0, 0, 0);
		end = new GregorianCalendar(yyyy, mm - 1, dd, 0, 0, 0);
	}

	static void grab() throws Exception {
		cleanUpDir("temp");
		ExecutorService pool = Executors.newFixedThreadPool(4);

		while (current.compareTo(end) <= 0) {
			pool.execute(new Worker(current.get(Calendar.YEAR),
					current.get(Calendar.MONTH) + 1, current.get(Calendar.DAY_OF_MONTH)));
			current.add(Calendar.DAY_OF_MONTH, 1);
		}
		pool.shutdown();
		if (pool.awaitTermination(10, TimeUnit.MINUTES)) {
			System.out.println("\nFile downloading finished.");
		} else System.out.println("Error: Timeout! (cannot connect to server)");
	}

	static void arrange() throws Exception {
		map = new HashMap<String, ArrayList<String>>();
		String line;
		current = new GregorianCalendar(YYYY, MM - 1, DD, 0, 0, 0);

		while (current.compareTo(end) <= 0) {
			Integer yy = current.get(Calendar.YEAR),
					mm = current.get(Calendar.MONTH) + 1,
					dd = current.get(Calendar.DAY_OF_MONTH);
			current.add(Calendar.DAY_OF_MONTH, 1);
			String year = yy.toString(),
					month = (mm < 10 ? "0" : "") + mm.toString(),
					date = (dd < 10 ? "0" : "") + dd.toString();
			File f = new File("temp/" + year + month + date + ".csv");
			if (!f.exists()) continue;
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(f), "big5"));
			while ((line = reader.readLine()) != null) {
				String[] split = line.split(",");
				try {
					if (split[0].length() != 4) continue;
					split = removeUrusaiTokens(line).split(",");
					Integer.parseInt(split[0]); // check legal 4 digit code
				} catch (NumberFormatException e) {
					continue;
				}
				if (map.get(split[0]) == null) map.put(split[0], new ArrayList<String>());
				ArrayList<String> list = map.get(split[0]);
				StringBuffer arranged = new StringBuffer(year);
				arranged.append("-").append(month).append("-").append(date).append(",")
						.append(split[4]).append(",").append(split[5]).append(",").append(split[6])
						.append(",").append(split[7]).append(",").append(split[8]);
				list.add(arranged.toString());
			}
			reader.close();
		}
	}

	static void writeArranged(boolean append) throws Exception {
		if (!append) cleanUpDir("arranged");
		for (Map.Entry<String, ArrayList<String>> entry : map.entrySet()) {
			File f = new File("arranged/" + entry.getKey() + ".csv");
			boolean alreadyExist = f.exists();
			BufferedWriter writer = new BufferedWriter(new FileWriter("arranged/" + entry.getKey() + ".csv", append));
			if (!alreadyExist) {
				writer.write("year-month-date,volume,open,high,low,close");
				writer.newLine();
			}
			for (String s : entry.getValue()) {
				writer.write(s);
				writer.newLine();
			}
			writer.close();
		}
	}

	static void determineUpdateRange() throws Exception {
		String line, prevLine = "";
		File f = new File("arranged/2330.csv");
		if (!f.exists()) {
			System.out.println("You have not run batched grab yet, please run in batched grab mode first.");
			return;
		}
		BufferedReader reader = new BufferedReader(new FileReader(f));
		while ((line = reader.readLine()) != null) prevLine = line;
		reader.close();
		String date[] = prevLine.split(",")[0].split("-");
		YYYY = Integer.parseInt(date[0]);
		MM = Integer.parseInt(date[1]);
		DD = Integer.parseInt(date[2]) + 1;
		Calendar calendar = Calendar.getInstance();
		yyyy = calendar.get(Calendar.YEAR);
		mm = calendar.get(Calendar.MONTH) + 1;
		dd = calendar.get(Calendar.DAY_OF_MONTH);
		start = new GregorianCalendar(YYYY, MM - 1, DD, 0, 0, 0);
		current = new GregorianCalendar(YYYY, MM - 1, DD, 0, 0, 0);
		end = new GregorianCalendar(yyyy, mm - 1, dd, 0, 0, 0);
	}

	static String removeUrusaiTokens(String s) {
		int start = s.indexOf("\""), end = s.indexOf("\"", start + 1);
		String left, middle, right;
		if (start > 0 && end > 0) {
			left = s.substring(0, start);
			middle = s.substring(start + 1, end).replaceAll(",", "");
			right = s.substring(end + 1);
			return removeUrusaiTokens(left + middle + right);
		}
		return s;
	}

	static void cleanUpDir(String name) {
		File temp = new File(name);
		if (!temp.exists()) temp.mkdir();
		File[] files = temp.listFiles();
		if (files != null)
			for (File file : files) file.delete();
	}
}
