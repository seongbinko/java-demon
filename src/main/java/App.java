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
 *  현재 보다 pmst.addBatch() , pmst.executeBatch(); 를 활용하는 것이 더 효율적이라 생각된다
 *  StringTokenizer는 구분자를 통한 공백을 제대로 표현 할 수 없다. 중간 부분 끝부분(-1) 등 split은 표현 가능하다. 정확히 구분을 위해서는 split이 낫고 속도적인 측면은 전자가 낫다한다.
 * </pre>
 */
public class App {

	private final static Logger Log = Logger.getGlobal();

	public static void main(String[] args)  {
		
		double beginTime = ((double)System.currentTimeMillis()/1000);
		
		Log.setLevel(Level.INFO);
		BufferedReader bf = null;
		try {
			Log.info("======================= App 작업 수행 ================================");

			File file = new File("data.txt");
			bf = new BufferedReader(new FileReader(file));

			String info;
			
			// SID면 : SERVICE NAME이면 /
			String url = "";
			Class.forName("oracle.jdbc.driver.OracleDriver");

			
			double startConnectionTime = ((double)System.currentTimeMillis()/1000);
			Connection con = DriverManager.getConnection(url, "", "");
			double endConnectionTime = ((double)System.currentTimeMillis()/1000);
			Log.info("데이터 커넥션 시간: " + (endConnectionTime - startConnectionTime) + "초");

			con.setAutoCommit(false);
			String insertSql = "INSERT INTO SOMETABLE (?,to_date(?,'YYYY-MM-DD HH24:MI:SS'),?,?,?,?,?,?,?,?,?,nvl(?,'10'),?,to_date(?,'YYYY-MM-DD HH24:MI:SS'),?,?,nvl(to_date(?,'YYYY-MM-DD HH24:MI:SS'),sysdate),to_date(?,'YYYY-MM-DD HH24:MI:SS'),nvl(?,'ADMIN'),?,?,?,nvl(?,'N'),?,?,?,to_number(?),to_date(?,'YYYY-MM-DD HH24:MI:SS'),?)";
			
			PreparedStatement pmst = null;
			// sql구문을 수행하는 역할
			
			while((info = bf.readLine()) != null) {

				Log.info("데이터 정보\n" + info);
				pmst = con.prepareStatement(insertSql);
				try {
					// int limit 인자 0일경우 zero length는 무시, -1(음수)일 경우는 공백도 포함, 양수일시 length의 길이를 제한한다.
					String[] arr =  info.split(" \\|",-1);
					
					// pmst의 set beginIndex는 1부터 시작이어서 int i=1로 잡음 arr은 0이 beginIndex
					for(int i=1; i<=arr.length; i++) {
						if("".equals(arr[i-1])) {
							pmst.setNull(i, java.sql.Types.NULL);
						} else {
							pmst.setString(i, arr[i-1]);
						}
					}
					pmst.addBatch();
					
				} catch (SQLException e) {
					e.printStackTrace();
					Log.info("===========================오류발생 rollback===============================");
					con.rollback();
				} finally {
					pmst.executeBatch();
					con.commit();
					pmst.close();
				}
			}

			con.close();
		} catch (Exception e) {
			//에러 메세지의 발생 근원지를 찾아서 단계별로 에러를 출력한다
			e.printStackTrace();
		} finally {
			if(bf != null) {
				try {
					bf.close();
				} catch (IOException e) {
					e.printStackTrace();
					Log.info("=====================data.txt 읽는 중 오류 발생=============================");
				}
			}
			Log.info("==================App 작업 완료 =================");
		}
		double endTime = ((double)System.currentTimeMillis()/1000);
		Log.info("총 소요 시간: " + (endTime - beginTime) + "초");
	}
}
