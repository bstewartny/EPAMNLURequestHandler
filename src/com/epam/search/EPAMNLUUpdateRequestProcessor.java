/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.epam.search;

import java.io.IOException;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.util.CoreMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.apache.solr.common.SolrInputDocument;
/**
 *
 * @author bstewart
 */
public class EPAMNLUUpdateRequestProcessor extends UpdateRequestProcessor {
    
    static final int MAX_TEXT_LENGTH=10000;
    
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
        System.out.println("EPAMNLUUpdateRequestProcessor::exractEntities: text.length="+text.length());
    
        Annotation annotation=new Annotation(text);
       
        System.out.println("pipeline.annotate start");
        pipeline.annotate(annotation);
        System.out.println("pipeline.annotate end");
        
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
        HashSet<String> h=null;//new HashSet<String>();
        for(NE entity:entities)
        {
            String name=entity.type.toLowerCase();
            if(name.equalsIgnoreCase("person") ||
                    name.equalsIgnoreCase("location") ||
                    name.equalsIgnoreCase("date") ||
                    name.equalsIgnoreCase("organization"))
            {
                if(h==null) h=new HashSet<String>();
                String value=entity.value.toLowerCase();
                String key=name+":"+value;
                if(!h.contains(key))
                {
                    h.add(key);
                    System.out.println("Adding field: epamnlu_"+key);
                    doc.addField("epamnlu_"+name, value);
                }
            }
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
        System.out.println("EPAMNLUUpdateRequestProcessor::getTextsToAnalyze");
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
