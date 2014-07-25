/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.epam.search;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.apache.solr.search.SortedIntDocSet;


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
        public String Keywords;
        public String AnswerType;
        public String ResponseTextMultiple;
        public String ResponseTextNone;
        public String ResponseTextSingle;
    }
    
    private String escape(String s)
    {
        return org.apache.lucene.queryparser.classic.QueryParser.escape(s);
    }
    private String entityToTermQuery(NE ne)
    {
        return ne.type+":\""+org.apache.lucene.queryparser.classic.QueryParser.escape(ne.value)+"\"";
    }
    
    private NLUQuery transformQuery(String naturalLanguageQuery,SolrIndexSearcher searcher) throws Exception
    {
        System.out.println("EPAMNLURequestHandler::transformQuery: "+naturalLanguageQuery);
        
        String[] words=tokenize(naturalLanguageQuery);
        
        List<String> facets=new ArrayList<String>();
        
        String text_query="";
        
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
                text_query+=word+" ";   
            }
        }
        
        NLUQuery query=new NLUQuery();
        
        query.SolrQuery="text:(" + text_query + ")";// titleText:("+text_query+")"; //createSolrQuery(entities);
        query.Keywords=text_query;
        query.FacetField=null;
        
        if(facets.size()==1)
        {
            String fieldName=facets.get(0);
            if(!fieldName.equalsIgnoreCase("documents"))
                query.FacetField=fieldName;
        }
        query.AnswerType=query.FacetField==null?"documents":query.FacetField;
        
        String criteria=text_query;
        
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
    
    private NamedList<Integer> getFacetCounts(String facetField,SolrQueryRequest req,DocSet docSet,SolrParams params ) throws Exception
    {
        ModifiableSolrParams p=new ModifiableSolrParams(params);
        
        p.set(FacetParams.FACET_FIELD, facetField);
        p.set(FacetParams.FACET_MINCOUNT,1);
        p.set(FacetParams.FACET_ZEROS,false);
        p.set(FacetParams.FACET_LIMIT,50);
        
        SimpleFacets f=new SimpleFacets(req,docSet,params);
         
        return f.getTermCounts(facetField);
    }
    
    private HashMap getFacetResult(SolrQueryRequest req,NLUQuery nluQuery, String facetValue, SolrIndexSearcher searcher, Filter filter,int span) throws Exception
    {
        HashMap facetResult=new HashMap();
               
        facetResult.put(nluQuery.FacetField, facetValue);
        facetResult.put("nlu_facet_field", nluQuery.FacetField);
        facetResult.put("nlu_answer_type",nluQuery.AnswerType);

        String query="text:\""+escape(facetValue) +" "+nluQuery.Keywords+"\"~"+span;
        
        query+=" OR text:\""+nluQuery.Keywords +" "+escape(facetValue)+"\"~"+span;
        
        query+=" OR (titleText:\""+escape(facetValue)+"\" AND text:("+nluQuery.Keywords+"))";
        
        QParser parser=QParser.getParser(query,null,req);
         
        TopDocs topDocs=searcher.search(parser.getQuery(),1);
        
        if(topDocs.totalHits>0)
        {
            return facetResult;
        }
        else
        {
            return null;
        }
    }
    
    @Override
    public void handleRequestBody(SolrQueryRequest req,SolrQueryResponse rsp) throws Exception
    {   
        SolrParams params=req.getParams();
        
        String naturalLanguageQuery=params.get(CommonParams.Q);
        
        SolrIndexSearcher searcher=req.getSearcher();
        
        NLUQuery nluQuery=transformQuery(naturalLanguageQuery,searcher);
        
        System.out.println("EPAMNLURequestHandler::nluQuery: "+nluQuery.SolrQuery);
        
        QParser parser=QParser.getParser(nluQuery.SolrQuery,null,req);
        
        TopDocs topDocs=searcher.search(parser.getQuery(), params.getInt(CommonParams.ROWS,20));
        
        List facetResults=new ArrayList();
        
        if(nluQuery.FacetField!=null)
        {
            DocSet docSet;

            int[] docs=new int[topDocs.scoreDocs.length];
            
            int i=0;
            
            for(ScoreDoc scoreDoc:topDocs.scoreDocs)
            {
                docs[i++]=scoreDoc.doc;
            }
            
            docSet=new SortedIntDocSet(docs);

            NamedList<Integer> counts=getFacetCounts(nluQuery.FacetField,req,docSet,params);

            Filter filter=docSet.getTopFilter();

            int span=10;
            
            int max_span=120;
            
            int good_facets=0;
            
            while(span<=max_span)
            {
                for(Map.Entry<String,Integer> kv:counts)
                {
                    String fieldValue=kv.getKey().trim();
                    
                    Integer count=kv.getValue();
                    
                    if(naturalLanguageQuery.toLowerCase().contains(fieldValue.toLowerCase()))
                    {
                        continue; // HACK
                    }
                    
                    if(fieldValue.contains(" ")) // HACK: entity names as NLP NE should have more than one word...
                    {
                        if(count>0)
                        {
                            good_facets++;
                            HashMap facetResult=getFacetResult(req,nluQuery,fieldValue,searcher,filter,span);
                            if(facetResult!=null)
                                facetResults.add(facetResult);
                        }
                    }
                }
                
                if(facetResults.size()>0)
                {
                    System.out.println("Found "+facetResults.size()+" facets with span="+span);
                    break;
                }
                   
                if(good_facets==0)
                {
                    System.out.println("There are no 'good' facets...");
                    break;
                }
                span+=10;
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
        String[] noise="the|did|was|show|tell|find|about|this|that|there|their|then|this".split("\\|");
        for(String n:noise)
        {   
            if(word.equalsIgnoreCase(n)){
                return true;
            }
        }
        return false;
    }
    
    private String getFacetForWord(String word)
    {
        for(String facetSpec:facetSpecs)
        {
            String[] p=facetSpec.split(":");
            String facet=p[0];
            if(word.equalsIgnoreCase(facet))
            {
                return facet;
            }
            String[] words=p[1].split("\\|");
            for(String facetWord:words)
            {
                if(word.equalsIgnoreCase(facetWord))
                {
                    return facet;
                }
            }
        }
    
        return null;
    }
}
