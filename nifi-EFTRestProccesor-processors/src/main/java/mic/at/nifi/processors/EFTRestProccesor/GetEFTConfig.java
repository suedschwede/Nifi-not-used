/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package mic.at.nifi.processors.EFTRestProccesor;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;


import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@Tags({"eft","rest","restservice"})
@CapabilityDescription("Reads EFT Config and write attributes, Check Token")
@SeeAlso({})
@ReadsAttributes({
	@ReadsAttribute(attribute="http.request.uri", description="Request URI"),
	@ReadsAttribute(attribute="http.method", description="HTTP METHOD"),
	@ReadsAttribute(attribute="http.headers.Token", description="Authentication Token")
 })
@WritesAttributes({
    @WritesAttribute(attribute="InDirectory", description="Incoming EFT Directory"),
    @WritesAttribute(attribute="OutDirectory", description="Outgoing EFT Directory"),
    @WritesAttribute(attribute="ConfirmProcessing", description="Command for Confirmation"),
    @WritesAttribute(attribute="FailedProcessing", description="Command for Failure"),
    @WritesAttribute(attribute="Customer", description="EFT Customer"),
    @WritesAttribute(attribute="Country", description="EFT Country"),
    @WritesAttribute(attribute="Count", description="Number of Files to List")
})

public class GetEFTConfig extends AbstractProcessor {

	static final PropertyDescriptor EFT_PROPERTIES = new PropertyDescriptor.Builder()
            .name("EFT Properties")
            .description("")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

	 static final Relationship REL_LISTFILES = new Relationship.Builder()
	            .name("listfiles")
	            .description("")
	            .build();
	    static final Relationship REL_GETFILE = new Relationship.Builder()
	            .name("getfile")
	            .description("")
	            .build();
	    static final Relationship REL_SAVEFILE = new Relationship.Builder()
	            .name("savefile")
	            .description("")
	            .build();
	    static final Relationship REL_AUTHENTICATION = new Relationship.Builder()
	            .name("authentication")
	            .description("")
	            .build();
	    static final Relationship REL_NOTFOUND= new Relationship.Builder()
	            .name("notfound")
	            .description("")
	            .build();
	    static final Relationship REL_CONFIRM= new Relationship.Builder()
	            .name("notfound")
	            .description("")
	            .build();
	    static final Relationship REL_FAILED= new Relationship.Builder()
	            .name("notfound")
	            .description("")
	            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;


    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(EFT_PROPERTIES);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_LISTFILES);
        relationships.add(REL_GETFILE);
        relationships.add(REL_SAVEFILE);
        relationships.add(REL_AUTHENTICATION);
        relationships.add(REL_NOTFOUND);
        relationships.add(REL_CONFIRM);
        relationships.add(REL_FAILED);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        
        // Get EFT Config from String (NIFI Property)
        final String eftproperties = context.getProperty(EFT_PROPERTIES).getValue();
        
        Properties prop = new Properties();
        // TODO implement
        try {
			prop.load(new StringReader(eftproperties));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        String requesturi = flowFile.getAttribute("http.request.uri");
        String httpmethod = flowFile.getAttribute("http.method");
        String httptoken = flowFile.getAttribute("http.headers.Token");
        
        String keyname = null;

        Set<String> keys = prop.stringPropertyNames();
        for (String key : keys) {
			 if (key.contains(".Context")) {
			   // examples of Context   "customer", "customer/de/atlas"  
		       String value = prop.getProperty(key);    
		       if (requesturi.startsWith("/" + value + '/')) {
		    	   keyname = key.split("\\.")[0];
		       }
			 }
        }
					
		String inDirectory = "";
		String outDirectory = "";
		String customer = "";
		String country = "";
		String confirmProcessing = "";
		String failedProcessing = "";
		String token = "";
		
		for (String key : keys) {
			String value = prop.getProperty(key);
			String data = key.split("\\.")[1];
			if (key.startsWith(keyname + ".")) {
			  switch (data) {
				case "InDirectory":
					inDirectory = value;
					break;
				case "OutDirectory":
					outDirectory = value;
					break;	
				case "Customer":
					customer = value;
					break;	
				case "Country":
					outDirectory = value;
					break;	
				case "ConfirmProcessing":
					confirmProcessing = value;
					break;
				case "FailedProcessing":
					failedProcessing = value;
					break;
				case "AuthToken":
					token = value;
					break;
			  }
			}
		}
				
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("InDirectory", inDirectory);
        attributes.put("OutDirectory", outDirectory);
        attributes.put("ConfirmProcessing", confirmProcessing);
        attributes.put("FailedProcessing", failedProcessing);
        attributes.put("Customer", customer);
        attributes.put("Country", country);

        if (requesturi.contains("/list/")) {
          String listCount = requesturi.substring(requesturi.lastIndexOf('/') + 1);
          attributes.put("Count", listCount);
        }
        
		flowFile = session.putAllAttributes(flowFile, attributes);
		
     
        if (!token.equals(httptoken)) {
          session.transfer(flowFile, REL_AUTHENTICATION);
        } else	if (httpmethod.equals("GET") && requesturi.contains("/file/")) {
          session.transfer(flowFile, REL_GETFILE);
		} else if (httpmethod.equals("GET") && requesturi.contains("/list/")) {
		  session.transfer(flowFile, REL_LISTFILES);
		} else if (httpmethod.equals("GET") && requesturi.contains("/confirm/")) {
		  session.transfer(flowFile, REL_CONFIRM);
		} else if (httpmethod.equals("GET") && requesturi.contains("/failed/")) {
		  session.transfer(flowFile, REL_FAILED);
		} else if (httpmethod.equals("POST")) {
		  session.transfer(flowFile, REL_LISTFILES);
		} else {
		  session.transfer(flowFile, REL_NOTFOUND);
		}
    }
}
