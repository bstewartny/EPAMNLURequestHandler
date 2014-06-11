/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.epam.search;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.StringHelper;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SimpleFacets;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.StrField;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;


/**
 *
 * @author bstewart
 */
public class EPAMNLURequestHandler extends RequestHandlerBase {
    
    String[] entityFields;
    
    String[] facetSpecs;
    
    private class NLUQuery
    {
        public String SolrQuery;
        public String FacetField;
        public String AnswerType;
        public String ResponseTextMultiple;
        public String ResponseTextNone;
        public String ResponseTextSingle;
    }
    
    private String entityToTermQuery(NE ne)
    {
        return ne.type+":\""+org.apache.lucene.queryparser.classic.QueryParser.escape(ne.value)+"\"";
    }
    
    private String createSolrQuery(List<NE> entities)
    {
        if(entities.size()==0)
            return "*:*";
        
        String query="(";
        
        for(NE entity:entities)
        {
            query+=entityToTermQuery(entity)+" ";
        }
        
        query+=")";
        
        return query;
    }
    
    private NLUQuery transformQuery(String naturalLanguageQuery,SolrIndexSearcher searcher) throws Exception
    {
        System.out.println("EPAMNLURequestHandler::transformQuery: "+naturalLanguageQuery);
        
        String[] words=tokenize(naturalLanguageQuery);
        
        List<NE> entities=new ArrayList<NE>();
        List<String> facets=new ArrayList<String>();
        List<String> keywords=new ArrayList<String>();
        
        for(String word:words)
        {
            if(isNoiseWord(word))
            {
                continue;
            }
            String facet=getFacetForWord(word);
            if(facet!=null)
            {
                facets.add(facet);
            }
            else    
            {
                List<NE> matched_entities=getEntitiesForWord(word,searcher);
                 
                for(NE entity:matched_entities)
                {
                    entities.add(entity);
                }
            }
        }
        
        NLUQuery query=new NLUQuery();
        
        query.SolrQuery=createSolrQuery(entities);
        
        query.FacetField=null;
        
        if(facets.size()==1)
        {
            String fieldName=facets.get(0);
            if(!fieldName.equalsIgnoreCase("documents"))
                query.FacetField=fieldName;
        }
        query.AnswerType=query.FacetField==null?"documents":query.FacetField;
        
        String criteria="";
        
        if(entities.size()>0)
        {
            for(NE entity:entities)
            {
                if(criteria.length()>0)
                    criteria+=", ";
                criteria+=entity.value;
            }
        }
        
        query.ResponseTextMultiple="I found <N> "+query.AnswerType +"s related to "+criteria+". Here they are.";
        
        query.ResponseTextNone="I did not find any "+query.AnswerType+"s related to "+criteria;
        
        query.ResponseTextSingle="I found one "+query.AnswerType+" related to "+criteria+". Here it is.";
        
        return query;
    }
    
    
    
    @Override
    public String getDescription()
    {
        return "EPAMNLURequestHandler";
    }
    
    @Override
    public String getSource()
    {
        return "EPAM";
    }
    
    @Override
    public void init(NamedList args)
    {
        System.out.println("EPAMNLURequestHandler::init");
        
        super.init(args);
        // get config settings for this handler from invariants
         
        this.facetSpecs=getParams("nlu.facet.synonyms");
        this.entityFields=getParams("nlu.entity.fields");
    }
   
    private String[] getParams(String name)
    {
         
        String[] params=invariants.getParams(name);
        if(params.length==1)
        {
            return trim(removeStupidBrackets(params[0]).split(","));
        }
        for(int i=0;i<params.length;i++)
        {
            params[i]=removeStupidBrackets(params[i]);
        }
        return trim(params);
    }
    
    private String[] trim(String[] a)
    {
        for(int i=0;i<a.length;i++)
        {
            a[i]=a[i].trim();
        }
        return a;
    }
    private String removeStupidBrackets(String p)
    {
        // no idea why there are [ ] surrounding these strings loaded from solrconfig.xml!!!!!!!!!!!! WTF?
        
        return p.replaceAll("\\[", "").replaceAll("\\]","");
    }
    
    private SolrDocument createSolrDocument(Document doc)
    {
        SolrDocument d=new SolrDocument();
         
        for(IndexableField fld:doc.getFields())
        {
            d.addField(fld.name(), fld.stringValue());
        }
        return d;
    }
    
    private SolrDocumentList createSolrDocuments(TopDocs docs,SolrIndexSearcher searcher) throws Exception
    {
        System.out.println("EPAMNLURequestHandler::createSolrDocuments");
        SolrDocumentList results=new SolrDocumentList();
         
        for(ScoreDoc scoreDoc:docs.scoreDocs)
        {
            results.add(createSolrDocument(searcher.doc(scoreDoc.doc)));
        }
         
        results.setNumFound(docs.scoreDocs.length);
        results.setStart(0);
        return results;
    }
    
    private NamedList<Integer> getFacetCounts(String facetField,SolrQueryRequest req,DocSet docSet,SolrParams params ) throws Exception
    {
        System.out.println("EPAMNLURequestHandler::getFacetCounts: "+facetField);
        
        ModifiableSolrParams p=new ModifiableSolrParams(params);
        
        p.set(FacetParams.FACET_FIELD, facetField);
        p.set(FacetParams.FACET_MINCOUNT,1);
        p.set(FacetParams.FACET_ZEROS,false);
        
        SimpleFacets f=new SimpleFacets(req,docSet,params);
         
        return f.getTermCounts(facetField);
    }
    
    private NamedList getFacetResult(NLUQuery nluQuery, String facetValue, SolrIndexSearcher searcher, Filter filter) throws Exception
    {
         
        NamedList facetResult=new NamedList();
            
        facetResult.add(nluQuery.FacetField, facetValue);
        facetResult.add("nlu_facet_field", nluQuery.FacetField);
        facetResult.add("nlu_answer_type",nluQuery.AnswerType);

        // get top N docs for this value given original criteria
        TopDocs topDocs=searcher.search(new TermQuery(new Term(nluQuery.FacetField,facetValue)),filter, 5);

        facetResult.add("top_docs",createSolrDocuments(topDocs,searcher));
        
        return facetResult;
    }
    
     
    @Override
    public void handleRequestBody(SolrQueryRequest req,SolrQueryResponse rsp) throws Exception
    {
        /*
         * 1. Process question:
         *  1a. Form SOLR Query
         *  1b. Detect Answer Type
         * 2. Exectute Query
         * 3. Process Answers
         * 4. Return Answer
         * 
         * 
         * 
         */
        
        
        
        System.out.println("EPAMNLURequestHandler::handleRequestBody");
        SolrParams params=req.getParams();
        
        String naturalLanguageQuery=params.get(CommonParams.Q);
        
        SolrIndexSearcher searcher=req.getSearcher();
        
        NLUQuery nluQuery=transformQuery(naturalLanguageQuery,searcher);
        
        System.out.println("EPAMNLURequestHandler::nluQuery: "+nluQuery.SolrQuery);
        
        QParser parser=QParser.getParser(nluQuery.SolrQuery,null,req);
        
        DocSet docSet=searcher.getDocSet(parser.getQuery());
        List facetResults=new ArrayList();
        if(nluQuery.FacetField!=null)
        {
            NamedList<Integer> counts=getFacetCounts(nluQuery.FacetField,req,docSet,params);

            Filter filter=docSet.getTopFilter();

            for(Map.Entry<String,Integer> kv:counts)
            {
                String fieldValue=kv.getKey();
                Integer count=kv.getValue();
                if(count>0)
                {

                    facetResults.add(getFacetResult(nluQuery,fieldValue,searcher,filter));
                }
            }
        }
        addFacetResults(rsp,facetResults,nluQuery);
    }
    
    private void addFacetResults(SolrQueryResponse rsp,List facetResults, NLUQuery nluQuery)
    {
        rsp.add("nlu_answer_type", nluQuery.AnswerType);
        rsp.add("nlu_facet_field", nluQuery.FacetField);
        rsp.add("nlu_query",nluQuery.SolrQuery);
        
        if(facetResults.size()==0)
            rsp.add("nlu_response_text", nluQuery.ResponseTextNone);
        else
            if(facetResults.size()==1)
                rsp.add("nlu_response_text", nluQuery.ResponseTextSingle);
            else
                rsp.add("nlu_response_text", nluQuery.ResponseTextMultiple.replaceAll("<N>", String.valueOf(facetResults.size())));
                
        rsp.add("nlu_results",facetResults);
    }
    
    private String[] tokenize(String text)
    {
        // TODO: use NLP tokenization to properly handle punctuation, etc.
        return text.toLowerCase().trim().split(" ");
    }
    
    private boolean isNoiseWord(String word)
    {
        if(word.length()<3) return true;
        // TODO: use comprehensive list of noise words
        String[] noise="the|show|me|tell|find|about".split("|");
        for(String n:noise)
        {
            if(word.equalsIgnoreCase(n)) return true;
        }
        return false;
    }
    
    private String getFacetForWord(String word)
    {
        System.out.println("getFacetForWord: "+word);
        
        for(String facetSpec:facetSpecs)
        {
            String[] p=facetSpec.split(":");
            String facet=p[0];
            if(word.equalsIgnoreCase(facet))
            {
                System.out.println("Found facet "+facet+" for word: "+word);
                return facet;
            }
            String[] words=p[1].split("\\|");
            for(String facetWord:words)
            {
                System.out.println("facetWord: "+facetWord);
                if(word.equalsIgnoreCase(facetWord))
                {
                    System.out.println("Found facet "+facet+" for word: "+word);
                    return facet;
                }
            }
        }
    
        return null;
    }
    
     
    private List<NE> getEntitiesForWord(String word,SolrIndexSearcher searcher) throws Exception
    {
        System.out.println("getEntitiesForWord: "+word);
       
        List<NE> entities=new ArrayList<NE>();
        
        if(word.length()>2)
        {
            final AtomicReader indexReader = searcher.getAtomicReader();
            
            Fields lfields=indexReader.fields();

            for(String field:entityFields)
            {
                FieldType ft=new StrField();

                Terms terms=lfields.terms(field);

                if(terms==null)
                {
                    continue;
                }
                
                TermsEnum termsEnum=terms.iterator(null);

                BytesRef prefixBytes=new BytesRef(word); // word is lowercase

                BytesRef term=null;

                if(termsEnum.seekCeil(prefixBytes)==TermsEnum.SeekStatus.END)
                {
                    continue;
                }

                term=termsEnum.term();

                CharsRef external=new CharsRef();
                
                while(term!=null)
                {
                    ft.indexedToReadable(term, external);
                    
                    if(!StringHelper.startsWith(term,prefixBytes))
                    {
                        break;
                    }
                    else    
                    { 
                        NE ne=new NE(field,external.toString());
                        System.out.println("Found entity "+ne.toString()+" for word: "+word);
                        entities.add(ne);
                    }
                    term=termsEnum.next();
                }
            }
        }
        
        return entities;
    }
}
