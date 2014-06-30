/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.epam.search;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import java.util.Properties;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

/**
 *
 * @author bstewart
 */
public class EPAMNLUUpdateRequestProcessorFactory extends UpdateRequestProcessorFactory {
    
    private NamedList args;
    private StanfordCoreNLP pipeline;
    
    @Override
    public void init(NamedList args)
    {
        System.out.println("EPAMNLUUpdateRequestProcessorFactory::init");
        System.out.println("args = "+args.toString());
        this.args=args;
        
        Properties props=new Properties();
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
        this.pipeline=new StanfordCoreNLP(props);
    }
    
    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req,SolrQueryResponse rsp,UpdateRequestProcessor next)
    {
        System.out.println("EPAMNLUUpdateRequestProcessorFactory::getInstance");
        return new EPAMNLUUpdateRequestProcessor(this.pipeline,next);
    }
}
