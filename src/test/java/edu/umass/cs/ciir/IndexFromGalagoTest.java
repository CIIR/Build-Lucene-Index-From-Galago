package edu.umass.cs.ciir;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.File;
import java.nio.file.Paths;

/**
 * Created by michaelz on 11/2/2016.
 */
public class IndexFromGalagoTest {

  private File inputText = null;
  private File galagoIndex = null;
  private File luceneIndex = null;

  private final String doc_1_text = "<TEXT>\nfirst document</TEXT>\n";
  private final String doc_2_text = "<TEXT>\nsecond document<br>second line of second document</TEXT>\n";
  private final String doc_3_1_text = "<TEXT>\nthird document<br>page one</TEXT>\n";
  private final String doc_3_2_text = "<TEXT>\nthird document<br>page two<br>Lorem ipsum dolor sit amet.</TEXT>\n";

  @Before
  public void setUp() throws Exception {

    // create a Galago index
    StringBuilder sb = new StringBuilder();
    sb.append("<DOC>\n<DOCNO>doc-1</DOCNO>\n" +
            doc_1_text +
            "</DOC>\n");

    sb.append("<DOC>\n<DOCNO>doc-2</DOCNO>\n" +
            doc_2_text +
            "</DOC>\n");

    sb.append("<DOC>\n<DOCNO>doc-3_1</DOCNO>\n" +
            doc_3_1_text +
            "</DOC>\n");
    sb.append("<DOC>\n<DOCNO>doc-3_2</DOCNO>\n" +
            doc_3_2_text +
            "</DOC>\n");

    // add a really long document - this is to test that we do NOT get the error:
    // java.lang.IllegalArgumentException: Document contains at least one immense term in field="body"
    // (whose UTF8 encoding is longer than the max length 32766), all of which were skipped.
    // Please correct the analyzer to not produce such terms.
    sb.append("<DOC>\n<DOCNO>doc-4</DOCNO>\n<TEXT>\n");
    for (int i = 0; i < 1000; i++){
      sb.append("Aenean vehicula volutpat sem sed "); // 33 bytes generated from http://www.lipsum.com/
    }
    sb.append("</TEXT>\n</DOC>\n");

    inputText = FileUtility.createTemporary();
    galagoIndex = FileUtility.createTemporaryDirectory();

    StreamUtil.copyStringToFile(sb.toString(), inputText);
    Parameters p = Parameters.create();
    p.set("indexPath", galagoIndex.getAbsolutePath());
    p.set("inputPath", inputText.getAbsolutePath());
    p.set("corpus", true);
    p.set("tokenizer", Parameters.create());

    App.run("build", p, System.err);

  }

  @After
  public void tearDown() throws Exception {
    FSUtil.deleteDirectory(galagoIndex);
    FSUtil.deleteDirectory(luceneIndex);
    inputText.delete();
  }

  @Test
  public void testMain() throws Exception {

    // build a Lucene index from the Galago index...
    luceneIndex = FileUtility.createTemporaryDirectory();
    IndexFromGalago obj = new IndexFromGalago();
    IndexFromGalago.main(new String[]{"--galagoIndex=" + galagoIndex, "--luceneIndex=" + luceneIndex});

    IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(luceneIndex.getAbsolutePath())));
    IndexSearcher searcher = new IndexSearcher(reader);
    Analyzer analyzer = new StandardAnalyzer(CharArraySet.EMPTY_SET);
    QueryParser parser = new QueryParser("tokens", analyzer);

    // make sure we didn't index the tags
    Query luceneQuery = parser.parse("tokens:TEXT");
    TopDocs luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(0, luceneResults.totalHits);

    // check lower case
    luceneQuery = parser.parse("tokens:text");
    luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(0, luceneResults.totalHits);

    luceneQuery = parser.parse("tokens:br");
    luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(0, luceneResults.totalHits);

    luceneQuery = parser.parse("tokens:first");
    luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(1, luceneResults.totalHits);
    ScoreDoc[] hits = luceneResults.scoreDocs;
    org.apache.lucene.document.Document doc = searcher.doc(hits[0].doc);
    Assert.assertEquals(doc_1_text, doc.get("body"));

    luceneQuery = parser.parse("tokens:LoRem");
    luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(1, luceneResults.totalHits);
    hits = luceneResults.scoreDocs;
    doc = searcher.doc(hits[0].doc);
    Assert.assertEquals(doc_3_2_text, doc.get("body"));

    luceneQuery = parser.parse("tokens:document");
    luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(4, luceneResults.totalHits);

    // phrase search
    luceneQuery = parser.parse("tokens:\"page one\"");
    luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(1, luceneResults.totalHits);
    hits = luceneResults.scoreDocs;
    doc = searcher.doc(hits[0].doc);
    Assert.assertEquals(doc_3_1_text, doc.get("body"));

    // boolean search
    luceneQuery = parser.parse("tokens:page one");
    luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(2, luceneResults.totalHits);
    hits = luceneResults.scoreDocs;
    doc = searcher.doc(hits[0].doc);
    Assert.assertEquals(doc_3_1_text, doc.get("body"));
    doc = searcher.doc(hits[1].doc);
    Assert.assertEquals(doc_3_2_text, doc.get("body"));

    luceneQuery = parser.parse("tokens:page OR one");
    luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(2, luceneResults.totalHits);
    hits = luceneResults.scoreDocs;
    doc = searcher.doc(hits[0].doc);
    Assert.assertEquals(doc_3_1_text, doc.get("body"));
    doc = searcher.doc(hits[1].doc);
    Assert.assertEquals(doc_3_2_text, doc.get("body"));

    luceneQuery = parser.parse("tokens:page AND one");
    luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(1, luceneResults.totalHits);
    hits = luceneResults.scoreDocs;
    doc = searcher.doc(hits[0].doc);
    Assert.assertEquals(doc_3_1_text, doc.get("body"));

    // per documentation: Note: The NOT operator cannot be used with just one term. For example, the following search will return no results:
    // so "tokens:NOT document" would raise an exception
    luceneQuery = parser.parse("tokens:first NOT document");
    luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(0, luceneResults.totalHits);

    luceneQuery = parser.parse("tokens:document NOT ipsum");
    luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(3, luceneResults.totalHits);

    // wildcard search
    luceneQuery = parser.parse("tokens:d??or");
    luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(1, luceneResults.totalHits);
    hits = luceneResults.scoreDocs;
    doc = searcher.doc(hits[0].doc);
    Assert.assertEquals(doc_3_2_text, doc.get("body"));

    luceneQuery = parser.parse("tokens:li*");
    luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(1, luceneResults.totalHits);
    hits = luceneResults.scoreDocs;
    doc = searcher.doc(hits[0].doc);
    Assert.assertEquals(doc_2_text, doc.get("body"));

    // fuzzy search
    luceneQuery = parser.parse("tokens:zorem~1");
    luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(1, luceneResults.totalHits);
    hits = luceneResults.scoreDocs;
    doc = searcher.doc(hits[0].doc);
    Assert.assertEquals(doc_3_2_text, doc.get("body"));

    // we should NOT have indexed the actual document text
    parser = new QueryParser("body", analyzer);
    luceneQuery = parser.parse("body:document");
    luceneResults = searcher.search(luceneQuery, 10);
    Assert.assertEquals(0, luceneResults.totalHits);

    reader.close();

  }
}