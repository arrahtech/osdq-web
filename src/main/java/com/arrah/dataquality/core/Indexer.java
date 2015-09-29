package com.arrah.dataquality.core;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





@SuppressWarnings("deprecation")
public class Indexer {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(Indexer.class);
	private IndexWriter indexWriter;
	@SuppressWarnings("rawtypes")
	private ArrayList<ArrayList> dataList;
	@SuppressWarnings("rawtypes")
	private ArrayList<ArrayList> headersList;
	private ArrayList<String> tableNames;
	@SuppressWarnings("rawtypes")
	private ArrayList<ArrayList> hits = new ArrayList<ArrayList>();
	private String indexDirectory = "./indexdir" ;
	
	
	
	private Searcher indexSearcher;


	
	@SuppressWarnings("rawtypes")
	public Indexer(String indexDirectory, ArrayList<ArrayList> dataList, ArrayList<ArrayList> headersList, ArrayList<String> tableNames){
		
		//this.indexDirectory = indexDirectory ;
		this.dataList = dataList;
		this.headersList = headersList;
		this.tableNames = tableNames;
	}
	
	/**
	 * This method creates an instance of IndexWriter which is used
	 * to add Documents and write indexes on the disc.
	 */
	
	
	void createIndexWriter(){
		if(indexWriter == null){
			try{
				
				//Create instance of Directory where index files will be stored
				FSDirectory fsDirectory =  FSDirectory.open(new File(indexDirectory));
				/* Create instance of analyzer, which will be used to tokenize
				the input data */
				Analyzer standardAnalyzer = new StandardAnalyzer(Version.LUCENE_36);
				//Create a new index
				boolean create = true;
				//Create the instance of deletion policy
				IndexDeletionPolicy deletionPolicy = 
										new KeepOnlyLastCommitDeletionPolicy(); 
				indexWriter =
					 new IndexWriter(fsDirectory,standardAnalyzer,create,
							 deletionPolicy,IndexWriter.MaxFieldLength.UNLIMITED);
				
			}catch(IOException ie){
				
				LOGGER.error("Error in creating IndexWriter");
				throw new RuntimeException(ie);
			}
		}
	}
	
	
	@SuppressWarnings({ "unchecked" })
	void indexData() throws  IOException {
		
	
		for(int i=0; i<headersList.size(); i++){
			
			String doc_name = tableNames.get(i);
			Document doc = new Document();
			
			ArrayList<String> headersName = headersList.get(i); 
			ArrayList<String> dataValues = dataList.get(i);
			
			int size = headersName.size();
			
			for(int j=0; j<dataValues.size(); j++){
					
//				System.out.print(headersName.get(j%size).toString());
//				System.out.print(" : " +dataValues.get(j).toString() + "datavalues size :" + dataValues.size() +"\n");
		
				Field field =
						new Field(headersName.get(j%size).toString(),dataValues.get(j).toString(),Field.Store.YES,Field.Index.NOT_ANALYZED);
		
				
				doc.add(field);
				
				if((j+1)%size==0){
					doc.add(new Field("Document_Name_",doc_name, Field.Store.YES,Field.Index.ANALYZED));
//					System.out.println("doc added   : " +doc.toString());
					//Indexer.addDocument(doc);
					doc = new Document();
				}
			}
		
			
//			System.out.println("Doc added");
		}

		indexWriter.optimize();

		indexWriter.close();
//		System.out.println("Doc added last");
	}
	

	void createIndexSearcher() throws CorruptIndexException, IOException{
		/* Create instance of IndexSearcher 
		 */
		indexSearcher = new IndexSearcher(FSDirectory.open(new File(indexDirectory)), true);	
	}
	
	 /*
	  * FUZZY QUERY
	  */
	@SuppressWarnings("rawtypes")
	ArrayList<ArrayList> fuzzyQueryExample(String param, String paramValue ){

		FuzzyQuery query = new FuzzyQuery(new Term(param, paramValue));
		 showSearchResults(query);
		 return hits;
	}
	
	
	 /*
	  * RANGE/TIME QUERY
	  */
	@SuppressWarnings({ "rawtypes", "unused" })
	ArrayList<ArrayList> rangeQueryExample(String param, String startRange, String endRange ){
		
		Term begin = new Term(param, startRange);
	    Term end = new Term(param, endRange);
	    TermRangeQuery query = new TermRangeQuery(param, startRange, endRange, true, true);
	    showSearchResults(query);
	    return hits;
	}
	
	
	 private void showSearchResults(TermRangeQuery query) {
		// TODO Auto-generated method stub
		
	}

	/*
	  * MULTI COLUMN (range and parameter/s)
	  */
	@SuppressWarnings("rawtypes")
	ArrayList<ArrayList> multiQueryExample(){
	
		TermQuery query1 = new TermQuery(new Term("stdName","dhiraj"));
	    NumericRangeQuery<Long> query2 = NumericRangeQuery.newLongRange("time", 24L, 1255L, true, true);
		//Query query2 = new TermQuery(new Term("address","bang"));
		BooleanQuery query = new BooleanQuery();
		query.add(query1,BooleanClause.Occur.SHOULD);
		query.add(query2,BooleanClause.Occur.SHOULD);
		showSearchResults(query);
		return hits;
	}
	
	
	 private void showSearchResults(BooleanQuery query) {
		// TODO Auto-generated method stub
		
	}

	/*
	  * MULTI COLUMN (parameter and parameter/s)
	  */
	@SuppressWarnings("rawtypes")
	ArrayList<ArrayList> multiQueryExample1(){
	
		TermQuery query1 = new TermQuery(new Term("Name","Vivek"));
		TermQuery query2 = new TermQuery(new Term("Address","Bangalore"));
		BooleanQuery query = new BooleanQuery();
		query.add(query1,BooleanClause.Occur.SHOULD);
		query.add(query2,BooleanClause.Occur.SHOULD);
		showSearchResults(query);
		return hits;
	}
	

	void showSearchResults(FuzzyQuery query){
		
		try{
			/* First parameter is the query to be executed and 
			   second parameter indicates the no of search results to fetch
			 */
			
			
			
			TopDocs topDocs = indexSearcher.search(query,20);	
			
			LOGGER.debug("Total hits "+topDocs.totalHits);
			
			// Get an array of references to matched documents
			ScoreDoc[] scoreDosArray = topDocs.scoreDocs;	
			
			
			for(int i=0; i<tableNames.size(); i++){
				
				ArrayList<String> records = new ArrayList<>();
				String currentDocName = tableNames.get(i);
				
				LOGGER.debug(currentDocName);
				for(ScoreDoc scoredoc: scoreDosArray){
					
					
					//Retrieve the matched document and show relevant details
					Document doc = indexSearcher.doc(scoredoc.doc);
					String docName = doc.getField("Document_Name_").stringValue();
					//System.out.println(docName);
					if(docName.equals(currentDocName)){
						List<Fieldable> fiel= doc.getFields();
						for(int j=0; j<fiel.size(); j++){
							
							if(fiel.get(j).name()!= "Document_Name_")
							records.add(doc.getField(fiel.get(j).name()).stringValue());
							
						}
					}
					
				}
				hits.add(records);
			}
			
			
			
			
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public void getMergeResults(){
	
		
	}
	

}
