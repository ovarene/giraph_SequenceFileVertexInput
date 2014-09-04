import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;

/**
 * Created by ovarene on 03/09/2014.
 */
public class MyWritable implements Writable {

  Double m_value1;
  Double m_value2;
  int m_nbLinks;
  HashSet<Long> m_links;


  MyWritable() {
    this(0.0,null);
  }

  MyWritable(Double p_value) {
    this(p_value,null);
  }


  MyWritable(Double p_value,Long[] p_links) {

    m_value1 = p_value;
    m_value2 = 0.0;

    m_nbLinks = 0;

    if(null != p_links && p_links.length > 0)
    {
      m_links = new HashSet<Long>(p_links.length);
      for(long l_l : p_links){
         m_links.add(l_l);
      }
      // adjust real nb of elements
      m_nbLinks = m_links.size();
    }
  }

  public Double getV1() {
    return m_value1;
  }

  public Double getV2() {
    return m_value2;
  }

  public void setV1(Double l_v) {
    m_value1 = l_v;
  }

  public void setV2(Double l_v) {
    m_value2 = l_v;
  }

  public int getNbLinks() {
    return m_nbLinks;
  }

  public HashSet<Long> getLinks() {
    return m_links;
  }



  @Override
  public void write(DataOutput out) throws IOException {
    out.writeDouble(m_value1);
    out.writeDouble(m_value2);

    out.writeInt(m_nbLinks);
    if(m_nbLinks > 0 && m_links != null)
    {
      for(Long l_l : m_links) {
        out.writeLong(l_l);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    m_value1 = in.readDouble();
    m_value2 = in.readDouble();

    m_nbLinks = in.readInt();

    if(m_nbLinks > 0) {
      m_links = new HashSet<Long>(m_nbLinks);
      for (int i = 0; i < m_nbLinks; i++) {
        m_links.add(in.readLong());
      }
      // update in case of ...
      m_nbLinks = m_links.size();
    }
  }

  @Override
  public String toString()
  {
    StringBuilder l_strB = new StringBuilder();
    l_strB.append(m_value1.toString() + "\t" + m_value2.toString() + "\t" + m_nbLinks + ":");

    if (m_links != null)
    {
      for (Long lv : m_links)
      {
        l_strB.append(lv.toString() + "|");
      }
    }
    return l_strB.toString();
  }
}
