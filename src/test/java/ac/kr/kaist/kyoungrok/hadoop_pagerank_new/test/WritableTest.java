package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.junit.Test;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageMetaNode;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNode;

public class WritableTest {
	public static byte[] serialize(Writable writable) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(out);
		writable.write(dataOut);
		dataOut.close();
		return out.toByteArray();
	}

	public static byte[] deserialize(Writable writable, byte[] bytes)
			throws IOException {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dataIn = new DataInputStream(in);
		writable.readFields(dataIn);
		dataIn.close();
		return bytes;
	}

	@Test
	public void testMetaNode() throws IOException {
		PageMetaNode node = new PageMetaNode(1, "test",
				Arrays.asList("A", "B"), 2, 2.0f);

		byte[] bs = serialize(node);
		PageMetaNode restored = new PageMetaNode();
		deserialize(restored, bs);

		System.out.println(node);
	}

	@Test
	public void testRankNode() throws IOException {
		PageRankNode node = new PageRankNode(1, 3, Arrays.asList(1, 2, 3), 1.0f);

		byte[] bs = serialize(node);
		PageRankNode restored = new PageRankNode();
		deserialize(restored, bs);

		System.out.println(node);
	}
}
