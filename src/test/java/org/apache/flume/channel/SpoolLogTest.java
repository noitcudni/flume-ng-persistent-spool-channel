package org.apache.flume.channel;
import java.lang.reflect.Field;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class SpoolLogTest {
  private SpoolLog chkpt;
  private Field fullPathField;
  private Field currDataFilenameField;
  private Field currDataOffsetField;
  private Field startPlaybackOffsetField;

  private static String TEST_CHKPT_FILE = "/tmp/test_chkpt.data";
  private static String COMPLETE_SUFFIX = "COMPLETE";

  @Before
  public void setUp() {
    chkpt = new SpoolLog(TEST_CHKPT_FILE, COMPLETE_SUFFIX);
    try {
      fullPathField = SpoolLog.class.getDeclaredField("fullPath");
      currDataFilenameField = SpoolLog.class.getDeclaredField("currDataFilename");
      currDataOffsetField = SpoolLog.class.getDeclaredField("currDataOffset");
      startPlaybackOffsetField = SpoolLog.class.getDeclaredField("startPlaybackOffset");

      fullPathField.setAccessible(true);
      currDataFilenameField.setAccessible(true);
      currDataOffsetField.setAccessible(true);
      startPlaybackOffsetField.setAccessible(true);

    } catch (NoSuchFieldException e) {
      e.printStackTrace();
      assert(false);
    }
  }

  private String getDataFilename() throws IllegalAccessException {
    return (String) currDataFilenameField.get(chkpt);
  }

  private int getDataOffset() throws IllegalAccessException {
    return ((Integer) currDataOffsetField.get(chkpt)).intValue();
  }

  private int getStartPlaybackOffset() throws IllegalAccessException {
    return ((Integer) startPlaybackOffsetField.get(chkpt)).intValue();
  }

  @Test
  public void testCommit() throws IllegalAccessException {
    int accumOffset = 0;

    String dataFilename = "/bogus/path/bogus_data_file.log";
    int dataOffset = 1;
    chkpt.commit(dataFilename, dataOffset);
    accumOffset += dataOffset;

    assertEquals(dataFilename, getDataFilename());
    assertEquals(accumOffset, getDataOffset());
    assertEquals(getStartPlaybackOffset(), 0);

    dataOffset = 9;
    chkpt.commit(dataFilename, dataOffset);
    accumOffset += dataOffset;

    assertEquals(dataFilename, getDataFilename());
    assertEquals(accumOffset, getDataOffset());
    assertEquals(getStartPlaybackOffset(), 0);
  }

}
