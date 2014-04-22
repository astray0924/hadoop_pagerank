package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util.PageRankHelper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNode;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.VIntArrayWritable;

public class TestPageRank {
	private Map<VIntWritable, PageRankNode> graph = new HashMap<VIntWritable, PageRankNode>();
	private PageRankHelper helper;

	public void debugGraphStructure() {
		for (PageRankNode node : graph.values()) {
			Writable[] links = node.getInLinks().get();

			System.out.printf("[%s] - ", node.getId());

			for (Writable l : links) {
				System.out.print(l + ",");
			}

			System.out.println("");
		}
	}

	public void debugGraphScore() {
		for (PageRankNode node : graph.values()) {
			System.out.printf("[%s] - %s\n", node.getId(), node.getScore());
		}
	}

	@Test
	public void testPageRank() {
		int K = 20;

		for (int i = 0; i < K; i++) {
			System.out.printf("--------%s--------\n", i + 1);

			helper.updateGraphScores(graph);

			// debugGraphStructure();
			debugGraphScore();
		}
	}

	@Before
	public void setUp() throws IOException {
		/*
		 * I - 1 H - 2 L - 3 M - 4 G - 5 E - 6 F - 7 D - 8 B - 9 A - 10 C - 11
		 */
		final VIntWritable idI = new VIntWritable(1);
		final VIntWritable idH = new VIntWritable(2);
		final VIntWritable idL = new VIntWritable(3);
		final VIntWritable idM = new VIntWritable(4);
		final VIntWritable idG = new VIntWritable(5);
		final VIntWritable idE = new VIntWritable(6);
		final VIntWritable idF = new VIntWritable(7);
		final VIntWritable idD = new VIntWritable(8);
		final VIntWritable idB = new VIntWritable(9);
		final VIntWritable idA = new VIntWritable(10);
		final VIntWritable idC = new VIntWritable(11);

		FloatWritable initScore = new FloatWritable(0.1f);

		PageRankNode I = new PageRankNode();
		VIntWritable[] linksI = new VIntWritable[] {};
		I.set(idI, new VIntWritable(2), new VIntArrayWritable(linksI),
				initScore);

		PageRankNode H = new PageRankNode();
		VIntWritable[] linksH = new VIntWritable[] {};
		H.set(idH, new VIntWritable(2), new VIntArrayWritable(linksH),
				initScore);

		PageRankNode L = new PageRankNode();
		VIntWritable[] linksL = new VIntWritable[] {};
		L.set(idL, new VIntWritable(1), new VIntArrayWritable(linksL),
				initScore);

		PageRankNode M = new PageRankNode();
		VIntWritable[] linksM = new VIntWritable[] {};
		M.set(idM, new VIntWritable(1), new VIntArrayWritable(linksM),
				initScore);

		PageRankNode G = new PageRankNode();
		VIntWritable[] linksG = new VIntWritable[] {};
		G.set(idG, new VIntWritable(1), new VIntArrayWritable(linksG),
				initScore);

		PageRankNode E = new PageRankNode();
		VIntWritable[] linksE = new VIntWritable[] { idI, idH, idL, idM, idG };
		E.set(idE, new VIntWritable(2), new VIntArrayWritable(linksE),
				initScore);

		PageRankNode F = new PageRankNode();
		VIntWritable[] linksF = new VIntWritable[] { idE };
		F.set(idF, new VIntWritable(2), new VIntArrayWritable(linksF),
				initScore);

		PageRankNode D = new PageRankNode();
		VIntWritable[] linksD = new VIntWritable[] { idE };
		D.set(idD, new VIntWritable(2), new VIntArrayWritable(linksD),
				initScore);

		PageRankNode B = new PageRankNode();
		VIntWritable[] linksB = new VIntWritable[] { idI, idH, idF, idE, idD,
				idG };
		B.set(idB, new VIntWritable(1), new VIntArrayWritable(linksB),
				initScore);

		PageRankNode A = new PageRankNode();
		VIntWritable[] linksA = new VIntWritable[] { idD };
		A.set(idA, new VIntWritable(0), new VIntArrayWritable(linksA),
				initScore);

		PageRankNode C = new PageRankNode();
		VIntWritable[] linksC = new VIntWritable[] { idB };
		C.set(idC, new VIntWritable(1), new VIntArrayWritable(linksC),
				initScore);

		graph.put(idA, A);
		graph.put(idB, B);
		graph.put(idC, C);
		graph.put(idD, D);
		graph.put(idE, E);
		graph.put(idF, F);
		graph.put(idG, G);
		graph.put(idH, H);
		graph.put(idI, I);
		graph.put(idL, L);
		graph.put(idM, M);

		helper = new PageRankHelper(graph);
	}
}
