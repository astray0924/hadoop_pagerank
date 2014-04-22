package ac.kr.kaist.kyoungrok.hadoop_pagerank_new.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Before;
import org.junit.Test;

import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.util.PageRankHelper;
import ac.kr.kaist.kyoungrok.hadoop_pagerank_new.writable.PageRankNode;

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

		Float initScore = 0.1f;
//
//		PageRankNode I = new PageRankNode();
//		VIntWritable[] linksI = new VIntWritable[] {};
//		I.set(idI, linksI, 2, initScore);
//
//		PageRankNode H = new PageRankNode();
//		List<VIntWritable> linksH = new ArrayList<VIntWritable>();
//		H.set(idH, linksH, 2, initScore);
//
//		PageRankNode L = new PageRankNode();
//		List<VIntWritable> linksL = new ArrayList<VIntWritable>();
//		L.set(idL, linksL, 1, initScore);
//
//		PageRankNode M = new PageRankNode();
//		List<VIntWritable> linksM = new ArrayList<VIntWritable>();
//		M.set(idM, linksM, 1, initScore);
//
//		PageRankNode G = new PageRankNode();
//		List<VIntWritable> linksG = new ArrayList<VIntWritable>();
//		G.set(idG, linksG, 1, initScore);
//
//		PageRankNode E = new PageRankNode();
//		List<VIntWritable> linksE = new ArrayList<VIntWritable>();
//		linksE.add(new VIntWritable(idI));
//		linksE.add(new VIntWritable(idH));
//		linksE.add(new VIntWritable(idL));
//		linksE.add(new VIntWritable(idM));
//		linksE.add(new VIntWritable(idG));
//
//		E.set(idE, linksE, 2, initScore);
//
//		PageRankNode F = new PageRankNode();
//		List<VIntWritable> linksF = new ArrayList<VIntWritable>();
//		linksF.add(new VIntWritable(idE));
//		F.set(idF, linksF, 2, initScore);
//
//		PageRankNode D = new PageRankNode();
//		List<VIntWritable> linksD = new ArrayList<VIntWritable>();
//		linksD.add(new VIntWritable(idE));
//		D.set(idD, linksD, 2, initScore);
//
//		PageRankNode B = new PageRankNode();
//		List<VIntWritable> linksB = new ArrayList<VIntWritable>();
//		linksB.add(new VIntWritable(idI));
//		linksB.add(new VIntWritable(idH));
//		linksB.add(new VIntWritable(idF));
//		linksB.add(new VIntWritable(idE));
//		linksB.add(new VIntWritable(idD));
//		linksB.add(new VIntWritable(idG));
//		B.set(idB, linksB, 1, initScore);
//
//		PageRankNode A = new PageRankNode();
//		List<VIntWritable> linksA = new ArrayList<VIntWritable>();
//		linksA.add(new VIntWritable(idD));
//		A.set(idA, linksA, 0, initScore);
//
//		PageRankNode C = new PageRankNode();
//		List<VIntWritable> linksC = new ArrayList<VIntWritable>();
//		linksC.add(new VIntWritable(idB));
//		C.set(idC, linksC, 1, initScore);
//
//		graph.put(new VIntWritable(idA), A);
//		graph.put(new VIntWritable(idB), B);
//		graph.put(new VIntWritable(idC), C);
//		graph.put(new VIntWritable(idD), D);
//		graph.put(new VIntWritable(idE), E);
//		graph.put(new VIntWritable(idF), F);
//		graph.put(new VIntWritable(idG), G);
//		graph.put(new VIntWritable(idH), H);
//		graph.put(new VIntWritable(idI), I);
//		graph.put(new VIntWritable(idL), L);
//		graph.put(new VIntWritable(idM), M);
	}
}
