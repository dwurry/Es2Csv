package dwurry;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * Created by davidurry on 3/9/16.
 */

@RunWith(MockitoJUnitRunner.class)
public class UtilTests {

    @Test
    public void ReturnTypeTimestampTests() {
        String returnType = Util.returnType("2016-02-27T01:05:12.056Z");
        assertTrue(returnType.equals("TIMESTAMP"));
        System.out.println("Test ReturnTypeTimestampTests(): " + returnType);
        returnType = Util.returnType("2016-02-26T17:05:17.919-08:00");
        assertTrue(returnType.equals("TIMESTAMP"));
        System.out.println("Test ReturnTypeTimestampTests(): " + returnType);
    }
}
