import java.util.Random;

public class TestRandomNumber {
	public static void main(String[] args) throws Exception {
		Random generator = new Random();
		
		int idx = 10;
		while(idx-- > 0) {
			int r = generator.nextInt(4);
			System.out.println("random number=" + r);
		}
		
	}
}
