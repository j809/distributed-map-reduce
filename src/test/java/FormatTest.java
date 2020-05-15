import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;


@RunWith(Parameterized.class)
public class FormatTest {
    @Parameter
    public String name;

    @Parameters
    public static Iterable<Object> data() {
        return Arrays.asList("king-james-version-bible", "war-and-peace","pride_and_prejudice","christmas_carol");
    }

    @Test
    public void testFormat() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        WordCount wordCount = new WordCount(1, TestUtil.getInputArguments(this.name));
        wordCount.setOutputStream(new PrintStream(out));
        wordCount.run();

        assertEquals(TestUtil.getReferenceOutput(this.name), out.toString());
    }
}
