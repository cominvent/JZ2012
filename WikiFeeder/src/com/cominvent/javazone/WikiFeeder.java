package com.cominvent.javazone;

import java.io.IOException;
import java.net.MalformedURLException;
import edu.jhu.nlp.wikipedia.*;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrServer;
import org.apache.solr.common.SolrInputDocument;


public class WikiFeeder {

	WikiXMLParser wxsp;
	int offset = 0;
	int from = 0;
	int to = Integer.MAX_VALUE;
	static String solrUrl = "http://localhost:6100/solr/collection1";
	static String zk = "localhost:7100";

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int from = 0, to = Integer.MAX_VALUE;
		if (args.length < 1) {
			System.out.println("Usage: java -jar WikiFeeder.jar [-u <solr-update-url>] <dumpfile> [fromOffset] [toOffset]");
			System.exit(1);
		}
		int offset = 0;

		if(args[offset].equals("-u")) {
			solrUrl = args[++offset];
			offset++;
		}

		String file = args[offset++];
		if (args.length > offset) 
			from = Integer.parseInt(args[offset++]);
		if (args.length > offset)
			to = Integer.parseInt(args[offset++]);
		
		WikiFeeder wf = new WikiFeeder(file, from, to);
		wf.start();
	}

	public WikiFeeder(String file, int from, int to) {
		this.from = from;
		this.to = to;
		wxsp = WikiXMLParserFactory.getSAXParser(file);
		try {
			wxsp.setPageCallback(new MyCallbackHandler(from, to));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void start() throws Exception {
		wxsp.parse();
	}
	
	class MyCallbackHandler implements PageCallbackHandler {
		int from;
		int to;
		SolrServer server;
		
		MyCallbackHandler(int from, int to) throws MalformedURLException {
			this.from = from;
			this.to = to;
			this.server = new ConcurrentUpdateSolrServer(solrUrl, 100, 8);
//			this.server = new CloudSolrServer("localhost:7100");
//			((CloudSolrServer)server).setDefaultCollection("collection1");
			System.out.println("Server="+server);
		}
				
		public void process(WikiPage page) {
			try {
				if(offset >= to) {
					server.commit();
					server.shutdown();
					System.exit(0);
				}
				if(page.isSpecialPage() || page.isRedirect()) {
					System.out.print(".");
					return;
				}
				offset++;
				if(offset < from) {
					System.out.print(".");
					return;
				}					
				System.out.println("Indexing page " + page.getTitle().trim() + " - "+page.getCategories()+ " -- "+page.getWikiText());
				SolrInputDocument doc = new SolrInputDocument();
				doc.setField("title", page.getTitle());
				doc.setField("url", "http://no.wikipedia.org/wiki/"+page.getTitle());
				String text = page.getText().replaceAll("(?s)\\{\\{.*?\\}\\}", "");
//				System.out.println("Text: "+text);
				doc.setField("content", text);
				doc.setField("id", page.getID());
				doc.setField("cat", page.getCategories());
//				doc.setField("foo_s", page.)
				
				server.add(doc, 10000);
			} catch (SolrServerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
