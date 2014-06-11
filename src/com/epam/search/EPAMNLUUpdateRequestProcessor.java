/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.epam.search;

import java.io.IOException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.util.ArrayCoreMap;
import edu.stanford.nlp.util.CoreMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
/**
 *
 * @author bstewart
 */
public class EPAMNLUUpdateRequestProcessor extends UpdateRequestProcessor {
    
    static final int MAX_TEXT_LENGTH=100000;
    
    public EPAMNLUUpdateRequestProcessor(StanfordCoreNLP pipeline,UpdateRequestProcessor next){
        super(next);
        this.pipeline=pipeline;
         
    }
    
    private StanfordCoreNLP pipeline;
    
    private void extractEntities(CoreMap sentence,List<NE> entities)
    {
        NE current_ne=null;
       
        for(CoreLabel token:sentence.get(CoreAnnotations.TokensAnnotation.class)){
            String value=token.get(CoreAnnotations.TextAnnotation.class);
            String type=token.get(CoreAnnotations.NamedEntityTagAnnotation.class);

            if(!type.equalsIgnoreCase("O"))
            {
                if(current_ne!=null)
                {
                    if(current_ne.type.equalsIgnoreCase(type))
                    {
                        // part of same token
                        current_ne.value+=" "+value;
                    }
                    else
                    {
                        if(current_ne!=null)
                        {
                            entities.add(current_ne);
                        }
                        current_ne=new NE(type,value);
                    }
                }
                else
                {
                    current_ne=new NE(type,value);
                }
            }
            else
            {
                if(current_ne!=null)
                {
                    entities.add(current_ne);
                    current_ne=null;
                }
            }
        }
        if(current_ne!=null)
        {
            entities.add(current_ne);
        }
    }
    
    private void extractEntities(String text,List<NE> entities)
    {
        Annotation annotation=new Annotation(text);
        
        pipeline.annotate(annotation);
        
        List<CoreMap> sentences=annotation.get(CoreAnnotations.SentencesAnnotation.class);
        
        if(sentences!=null)
        {
            for(CoreMap sentence:sentences)
            {
                extractEntities(sentence,entities);
            }
        }
    }
    
    private List<NE> extractEntities(SolrInputDocument doc)
    {
        List<NE> entities=new ArrayList<NE>();
        
        List<String> texts=getTextsToAnalyze(doc);
            
        for(String text:texts)
        {
            extractEntities(text,entities);    
        }
        
        return entities;
    }
    
    private void addEntityFields(List<NE> entities,SolrInputDocument doc)
    {
        for(NE entity:entities)
        {
            System.out.println("Adding field: "+"epamnlu_"+entity.type.toLowerCase()+":"+entity.value.toLowerCase().trim());
            doc.addField("epamnlu_"+entity.type.toLowerCase(), entity.value.toLowerCase().trim());
        }
    }
    
    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException
    {
        System.out.println("EPAMNLUUpdateRequestProcessor::processAdd");
        try
        {
            SolrInputDocument doc=cmd.getSolrInputDocument();
        
            List<NE> entities=extractEntities(doc);
            
            addEntityFields(entities,doc);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        super.processAdd(cmd);
    }
    
    private List<String> getTextsToAnalyze(SolrInputDocument doc)
    {
        ArrayList<String> texts=new ArrayList<String>();
        
        // TODO: for now get all the fields, but later on use some configuration from solrconfig.xml
        for(String fieldName:doc.getFieldNames())
        {
            for(Object fieldValue:doc.getFieldValues(fieldName))
            {
                String text=String.valueOf(fieldValue);
                if(text!=null && text.length()>0)
                {
                    // very large texts process too slowly 
                    if(text.length()>MAX_TEXT_LENGTH)
                    {
                        text=text.substring(0, MAX_TEXT_LENGTH-1);
                    }
                    texts.add(text);
                }
            }
        }
        
        return texts;
    }
}
