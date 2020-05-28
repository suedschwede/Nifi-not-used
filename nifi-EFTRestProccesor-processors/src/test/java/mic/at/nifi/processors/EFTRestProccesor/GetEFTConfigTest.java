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

import static org.junit.Assert.assertTrue;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class GetEFTConfigTest {

    private TestRunner testRunner;
    private String config;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(GetEFTConfig.class);
        
   	  config = "1.Context=1" + "\n" +
        		"1.InClient=GET" +  "\n" +
        		"1.OutDirectory=/data/de-atlas/out" +  "\n" +
        		"1.InDirectory=/data/adiatlas/in" +  "\n" +
        		"1.ConfirmProcessing=mv {0} ../data/adiatlas/in/done" +  "\n" +
        		"1.FailedProcessing=mv {0} ../data/adiatlas/in/error" +  "\n" +
        		"1.AuthToken=XXXXX" +  "\n" +
        		"1.Customer=Customer1"  +  "\n" +
        		"1.Country=DE" +  "\n" +
                "2.Context=2" + "\n" +
	            "2.InClient=GET" +  "\n" +
	            "2.OutDirectory=/data/de-atlas/out" +  "\n" +
	            "2.InDirectory=/data/adiatlas/in" +  "\n" +
	            "2.ConfirmProcessing=mv {0} ../data/adiatlas/in/done" +  "\n" +
	            "2.FailedProcessing=mv {0} ../data/adiatlas/in/error" +  "\n" +
	            "2.AuthToken=YYYYY" +  "\n" +
	            "2.Customer=Customer2";
        
    }
    
    @Test
    public void AuthenticationFailure() {
    	    	         
    	 testRunner.setProperty(GetEFTConfig.EFT_PROPERTIES, config);
         
         ProcessSession session = testRunner.getProcessSessionFactory().createSession();
         FlowFile ff = session.create();
             
         ff = session.putAttribute(ff,"http.request.uri", "/1/list/10");
         ff = session.putAttribute(ff,"http.method", "GET");
         ff = session.putAttribute(ff,"http.headers.Token", "11111");
    
         
         testRunner.enqueue(ff);
         testRunner.run();

         testRunner.assertValid();
         testRunner.assertAllFlowFilesTransferred(GetEFTConfig.REL_AUTHENTICATION, 1);

    }

    @Test
    public void testListFiles() {
    	    	         
    	 testRunner.setProperty(GetEFTConfig.EFT_PROPERTIES, config);
         
         ProcessSession session = testRunner.getProcessSessionFactory().createSession();
         FlowFile ff = session.create();
             
         ff = session.putAttribute(ff,"http.request.uri", "/1/list/10");
         ff = session.putAttribute(ff,"http.method", "GET");
         ff = session.putAttribute(ff,"http.headers.Token", "XXXXX");
    
         
         testRunner.enqueue(ff);
         testRunner.run();

         testRunner.assertValid();
         testRunner.assertAllFlowFilesTransferred(GetEFTConfig.REL_LISTFILES, 1);
         final MockFlowFile out = testRunner.getFlowFilesForRelationship(GetEFTConfig.REL_LISTFILES).get(0);
         final String count = out.getAttribute("Count");
         assertTrue(count.contains("10"));
         final String customer = out.getAttribute("Customer");
         assertTrue(customer.contains("Customer1"));
    }

}
