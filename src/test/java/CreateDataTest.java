import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.*;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class CreateDataTest {

  private static String TEST_ROOT_DIR = new File("/tmp/testData").toURI().getPath();
  private static Configuration  CONF;
  private static FileSystem     FS;

  private static Path           INDIR;

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {

     CONF = new Configuration();
     FS   = FileSystem.get(CONF);

     SequenceFile.Writer l_writer = SequenceFile.createWriter(CONF,
         SequenceFile.Writer.file(new Path(TEST_ROOT_DIR,"LongWMyW_Seq/data.seq")),
         SequenceFile.Writer.keyClass(LongWritable.class),
         SequenceFile.Writer.valueClass(MyWritable.class));

    l_writer.append(new LongWritable(1L),new MyWritable(1.0));
    l_writer.append(new LongWritable(2L),new MyWritable(2.0,new Long[] { 1L, 6L, 1L }));
    l_writer.append(new LongWritable(5L),new MyWritable(5.0,new Long[] { 1L, 4L, 6L } ));

    l_writer.close();

  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    SequenceFile.Reader l_reader = new SequenceFile.Reader(CONF,
        SequenceFile.Reader.file(new Path(TEST_ROOT_DIR,"LongWMyW_Seq/data.seq"))
        );
    LongWritable  l_k =  new LongWritable();
    MyWritable    l_v = new MyWritable();
    while(l_reader.next(l_k,l_v)){
      System.out.println(l_k + " -> " + l_v.toString());
    }

    l_reader.close();

    FS.delete(new Path(TEST_ROOT_DIR),true);

  }

    @Before
  public void setUp() throws Exception {

  }

  @After
  public void tearDown() throws Exception {

  }


  @Test
  public void test() {
    //fail("Not implemented yet");
  }



}