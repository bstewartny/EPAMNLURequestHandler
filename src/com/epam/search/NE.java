/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.epam.search;

/**
 *
 * @author bstewart
 */
public class NE {
    
    public String type;
    public String value;

    public NE(){}

    public NE(String type,String value)
    {
        this.type=type;
        this.value=value;
    }

    @Override
    public String toString()
    {
        return this.type+":"+this.value;
    }
}
