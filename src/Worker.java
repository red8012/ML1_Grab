import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class Worker implements Runnable {
	String yy, mm, dd;

	public Worker(Integer yyyy, Integer mm, Integer dd) {
		this.yy = yyyy.toString();
		this.mm = (mm < 10 ? "0" : "") + mm.toString();
		this.dd = (dd < 10 ? "0" : "") + dd.toString();
	}

	public void run() {
		try {
			String url = "http://www.twse.com.tw/ch/trading/exchange/MI_INDEX/MI_INDEX3_print.php?" +
					"genpage=genpage/Report[yy][mm]/A112[yy][mm][dd]ALLBUT0999_1.php&type=csv";
			url = url.replace("[yy]", yy).replace("[mm]", mm).replace("[dd]", dd);
			URLConnection conn = new URL(url).openConnection();
			ReadableByteChannel rbc = Channels.newChannel(conn.getInputStream());
			FileOutputStream fos = new FileOutputStream("temp/" + yy + mm + dd + ".csv");
			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
			rbc.close();
			fos.close();
			// remove empty files
			File file = new File("temp/" + yy + mm + dd + ".csv");
			if (file.length() < 1024L) file.delete();
			System.out.print("*");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}