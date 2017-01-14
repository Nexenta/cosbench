/** 
 
Copyright 2013 Intel Corporation, All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. 
*/ 

package com.intel.cosbench.driver.generator;

import java.io.File;
import java.io.FileOutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Random;

import org.apache.commons.io.input.NullInputStream;

import com.intel.cosbench.driver.util.HashUtil;
import com.intel.cosbench.log.LogFactory;
import com.intel.cosbench.log.Logger;

/**
 * This class is to generate random data as input stream for data uploading.
 * 
 * @author ywang19, qzheng7
 * 
 */
public class RandomInputStream extends NullInputStream {
	static int buf_size = 16384;

    private boolean hashCheck = false;
    private HashUtil util = null;
    private int hashLen = 0;
    private byte[] hashBytes;
    private long size = 0;
    private long processed = 0;
	private Random r;
	private long seed;

    private static Logger logger = LogFactory.getSystemLogger();

    public RandomInputStream(long size, Random random, boolean isRandom,
            boolean hashCheck) {
        super(size);
        
        this.seed = System.currentTimeMillis() + random.nextInt(1000);
        this.r = new Random(seed);
        		
        this.hashCheck = hashCheck;
        try {
            this.util = new HashUtil();
        } catch (NoSuchAlgorithmException e) {
            logger.error("Alogrithm not found", e);
        }
        this.hashCheck = false;
        this.util = null;
        this.hashLen = 0;
        this.size = size;

    }

    @Override
    protected int processByte() {
        throw new UnsupportedOperationException("do not read byte by byte");
    }

    @Override
    protected void processBytes(byte[] bytes, int offset, int length) {
    	long start = this.getPosition() - length;
    	

    	// Mark/reset
    	if (start < processed) {
    		out("position: " + getPosition() + " start: " + start + " processed: " + processed);
    		this.r = new Random(this.seed);
    		processed = 0;
    		byte buf[] = new byte[buf_size];
    		while (processed < start) {
               if ((processed+buf_size) <= start) {
            	   r.nextBytes(buf);
            	   processed += buf_size;
            	   continue;
               }
               int l = (int) (start - processed);
               buf = new byte[l];
               processed += l;
        	   r.nextBytes(buf);
    		}
    	}
    	
		byte buf[] = new byte[buf_size];
		int o = offset;
		while (processed < this.getPosition()) {
           if ((processed+buf_size) <= this.getPosition()) {
        	   r.nextBytes(buf);
        	   System.arraycopy(buf, 0, bytes, o, buf_size);
        	   processed += buf_size;
        	   o += buf_size;
        	   continue;
           }
           int l = (int) (this.getPosition() - processed);
           buf = new byte[l];
           processed += l;
    	   r.nextBytes(buf);
    	   System.arraycopy(buf, 0, bytes, o, l);
		}
    }

	public static void out(String s) {
		System.out.println(s);
	}
    
	public static void main(String[] args) {
		byte buf[] = new byte[4096];
		try{
			// entity without data value
			// entity with random input stream
			long length = 1192010;
			System.out.println("=== Test Entity with value stream from random generator ===");
	        RandomInputStream fi = new RandomInputStream(length, new Random(23), true, false);
			FileOutputStream  fo = new FileOutputStream(new File("rnd.txt"));
			fi.mark((int)length);
			byte b[] = new byte[9];
			fi.read(b);
			out("First 9 bytes: " + Arrays.toString(b));
			out("Mark");
			fi.mark((int)length);
			fi.read(b);
			out("Next  9 bytes: " + Arrays.toString(b));

			int len = 0;
			while ((len = fi.read(buf)) > 0) {
			   fo.write(buf, 0, len);
			}
			
			out("Reset");
			fi.reset();
			
			fi.read(b);
			out("First 9 bytes: " + Arrays.toString(b));

			fi.close();
			fo.close();
			
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
    
}
