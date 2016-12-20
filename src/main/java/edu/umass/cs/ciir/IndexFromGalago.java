/*  Copyright (C) <2016>  University of Massachusetts Amherst
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package edu.umass.cs.ciir;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.jsoup.Jsoup;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Create a Lucene index from a Galago index.
 *
 * @author jfoley
 *         from: https://gist.github.com/jjfiv/517d80167b1bdbc9a8d0ef08ff163a77
 *         modification: michaelz
 *         influenced by: https://github.com/jiepujiang/cs646_tutorials
 */
public class IndexFromGalago {
  public static final Logger logger = Logger.getLogger(IndexFromGalago.class.getName());

  public static void main(String[] args) throws Exception {
    Parameters argp = Parameters.parseArgs(args);
    String galagoIndexPath = null;
    String luceneIndexPath = null;
    try {
      galagoIndexPath = argp.getString("galagoIndex");
      luceneIndexPath = argp.getString("luceneIndex");
    } catch (Exception e) {
      System.out.println(getUsage());
      return;
    }

    logger.setUseParentHandlers(false);
    FileHandler lfh = new FileHandler("indexing-errors.log");
    SimpleFormatter formatter = new SimpleFormatter();
    lfh.setFormatter(formatter);
    logger.addHandler(lfh);

    final DiskIndex index = new DiskIndex(argp.get("index", galagoIndexPath));
    final CorpusReader corpus = (CorpusReader) index.getIndexPart("corpus");
    long total = corpus.getManifest().getLong("keyCount");
    final CorpusReader.KeyIterator iterator = corpus.getIterator();

    final Document.DocumentComponents dcp = Document.DocumentComponents.JustText;
    // Analyzer includes options for text processing
    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        // Step 1: tokenization (Lucene's StandardTokenizer is suitable for most text retrieval occasions)
        TokenStreamComponents ts = new TokenStreamComponents(new StandardTokenizer());
        // Step 2: transforming all tokens into lowercased ones
        ts = new Analyzer.TokenStreamComponents(ts.getTokenizer(), new LowerCaseFilter(ts.getTokenStream()));
        // Step 3: whether to remove stop words
        // Uncomment the following line to remove stop words
        // ts = new TokenStreamComponents( ts.getTokenizer(), new StopwordsFilter( ts.getTokenStream(), StandardAnalyzer.ENGLISH_STOP_WORDS_SET ) );
        // Step 4: whether to apply stemming
        // Uncomment the following line to apply Krovetz or Porter stemmer
        // ts = new TokenStreamComponents( ts.getTokenizer(), new KStemFilter( ts.getTokenStream() ) );
        // ts = new TokenStreamComponents( ts.getTokenizer(), new PorterStemFilter( ts.getTokenStream() ) );
        return ts;
      }
    };

    try (final FSDirectory dir = FSDirectory.open(Paths.get(argp.get("output", luceneIndexPath)))) {
      final IndexWriterConfig cfg = new IndexWriterConfig(analyzer);
      System.out.println("Similarity: " + cfg.getSimilarity());
      cfg.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
      try (IndexWriter writer = new IndexWriter(dir, cfg)) {
        iterator.forAllKeyStrings(docId -> {
          try {
            Document document = iterator.getDocument(dcp);

            String text = document.text;
            String id = document.name;
            System.out.println("Processing document: " + id);
            org.apache.lucene.document.Document doc = new org.apache.lucene.document.Document();
            doc.add(new StringField("id", id, Field.Store.YES));
            // this stores the actual text with tags so formatting is preserved
            doc.add(new StoredField("body", text));
            org.jsoup.nodes.Document jsoup = Jsoup.parse(text);

            // tokens of the document
            FieldType fieldTypeText = new FieldType();
            fieldTypeText.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            fieldTypeText.setStoreTermVectors(true);
            fieldTypeText.setStoreTermVectorPositions(true);
            fieldTypeText.setTokenized(true);
            fieldTypeText.setStored(false);
            fieldTypeText.freeze();
            doc.add(new Field("tokens", jsoup.text(), fieldTypeText));

            try {
              writer.addDocument(doc);
              System.out.println("Doc count: " + writer.numDocs());
            } catch (IOException e) {
              logger.log(Level.WARNING, "Pull-Document-Exception", e);
              System.err.println(e.toString());
            }

          } catch (Exception e) {
            logger.log(Level.WARNING, "Pull-Document-Exception", e);
            System.err.println(e.toString());
          }
        });

      }
    }

    System.out.println("Indexing Done. ");
  }

  private static String getUsage() {
    return "Usage: --galagoIndex=<path to an existing Galago index> --luceneIndex=<path where the Lucene index will be created>";
  }
}