import org.apache.solr.common.cloud.ZkStateReader;


public class SystemPropsTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//System.out.println("args=" + args[0]);
		System.out.println("system param:" + Integer.getInteger("numShards"));
	}
}
