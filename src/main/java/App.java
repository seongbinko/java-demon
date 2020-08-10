package main.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * <pre>
 * 쉘스크립트를 통해 작업을 DB 인서트 작업을 수행하는 기능
 * </pre>
 */
public class App {

	private final static Logger Log = Logger.getGlobal();

	public static void main(String[] args)  {
		Log.setLevel(Level.INFO);


		BufferedReader bf = null;
		try {
			Log.info("======================= App 작업 수행 ================================");

			File file = new File("resources/data.txt");
			bf = new BufferedReader(new FileReader(file));

			String info;
			
			// SID면 : SERVICE NAME이면 /
			String url = "";
			Class.forName("oracle.jdbc.driver.OracleDriver");

			while((info = bf.readLine()) != null) {

				Log.info("데이터 정보\n" + info);

				Connection con = DriverManager.getConnection(url, "", "");
				con.setAutoCommit(false);
				String insertSql = "INSERT INTO TABLE VALUES (?,to_date(?,'YYYY-MM-DD HH24:MI:SS'),?,?,?,?,?,?,?,?,?,nvl(?,'10'),?,to_date(?,'YYYY-MM-DD HH24:MI:SS'),?,?,nvl(to_date(?,'YYYY-MM-DD HH24:MI:SS'),sysdate),to_date(?,'YYYY-MM-DD HH24:MI:SS'),nvl(?,'ADMIN'),?,?,?,nvl(?,'N'),?,?,?,to_number(?),to_date(?,'YYYY-MM-DD HH24:MI:SS'),?)";

				PreparedStatement pmst = con.prepareStatement(insertSql);
				try {
					// int limit 인자 0일경우 zero length는 무시, -1(음수)일 경우는 공백도 포함, 양수일시 length의 길이를 제한한다.
					String[] arr =  info.split(" \\|",-1);

					for(int i=1; i<=arr.length; i++) {
						if("".equals(arr[i-1])) {
							pmst.setNull(i, java.sql.Types.NULL);
						} else {
							pmst.setString(i, arr[i-1]);
						}
					}
					pmst.executeUpdate();

					con.commit();
				} catch (SQLException e) {
					e.printStackTrace();
					Log.info("===========================오류발생 rollback===============================");
					con.rollback();

				} finally {
					pmst.close();
					con.close();
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(bf != null) bf.close();
				Log.info("==================App 작업 완료 =================");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
}
