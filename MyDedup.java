// Group 8

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest; 
import java.lang.System.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.io.Serializable;
import javax.xml.bind.DatatypeConverter;
import java.util.HashMap;
import java.util.Queue;
import java.util.LinkedList;
import java.lang.Math;

import java.nio.charset.StandardCharsets;

class MyDedup { 

private static String indexFile = "mydedup.index";
private static String mode = "local";

public HashMap<String, Queue<String>> fileRecipe;

    static class Chunk {
        public long zeroLength = 0;
        public Boolean isZero;
        public byte[] data;
        public long size;
        public String hash = null;

        public Chunk(){
            zeroLength = 0;
            size = 0;
            isZero = false;
            data = null;
            hash = null;
        }

        public Chunk(long zeros){          //zero chunk
            zeroLength = zeros;
            size = zeros;
            isZero = true;
            data = new byte[(int)zeros];
            hash = null;
        }

        public Chunk(byte[] b){            //data chunk
            data = b;
            size = b.length;
            isZero = false;
        }
    }
	
	public static void main(String args[]) throws Exception{ 
        
        //for debugging---------------------------------------------------
        int minChunk = 2*1024; 
        int avgChunk = 8*1024;
        int maxChunk = 16*1024;
        int d = 257;
		// String inputFile = args[1];
        //for debugging---------------------------------------------------
        
        File directory = new File("azure_tmp/data");
		if (! directory.exists()){
			directory.mkdirs();
		}

        if(args[0].equals("upload")){
			minChunk = Integer.parseInt(args[1]);
			avgChunk = Integer.parseInt(args[2]);
			maxChunk = Integer.parseInt(args[3]);
			d = Integer.parseInt(args[4]);
			String inputFile = args[5];
			mode = args[6];
			if(mode.equals("azure")){
				AzureAPI.download(indexFile);
				indexFile= "azure_tmp/mydedup.index";
			}
			Index index = new Index();
            upload(index, inputFile, minChunk, avgChunk, d, maxChunk, mode);
        }
        else if(args[0].equals("download")){
			String inputFile = args[1];
			String localPath = args[2];
			mode = args[3];
			if(mode.equals("azure")){
				AzureAPI.download(indexFile);
				indexFile= "azure_tmp/mydedup.index";
			}
			Index index = new Index();
            // download(index, inputFile, "download/" + inputFile);
            download(index, inputFile, localPath, mode);
        }
        else if(args[0].equals("delete")){
			String inputFile = args[1];
			mode = args[2];
			if(mode.equals("azure")){
				AzureAPI.download(indexFile);
				indexFile= "azure_tmp/mydedup.index";
			}
			Index index = new Index();
            delete(index, inputFile, mode);
        }
		
		deleteDirectory(directory.getParentFile());
		
		
    }
	
	static void upload(Index index, String file, int minChunk, int avgChunk, int d, int maxChunk, String mode) {
		if(index.fileRecipe.containsKey(file)) {     //check if file already exists
            // delete(index, file);    //overwrite the original file
			System.out.println("File exists, Upload failed");
            return;
        }		
		
		try{
			
			File directory = new File("data");
			if(mode.equals("azure")) directory = new File("azure_tmp/data");
			if (! directory.exists()){
				directory.mkdirs();
			}
			byte[] t = Files.readAllBytes(Paths.get(file));	
			
			Queue<String> recipe = new LinkedList<String>();
						
			LinkedList<Chunk> chunks = getChunks(t,minChunk,avgChunk,d,maxChunk);
			           
            for (Chunk chunk : chunks) {
                chunk.hash = getMD5(chunk.data);
				if(chunk.isZero){
                    recipe.offer("*" + Long.toString(chunk.zeroLength));
                }else{
                    recipe.offer(chunk.hash);
                    uploadChunk(chunk.hash, chunk.data);
                    if(!index.chunkCount.containsKey(chunk.hash)){
                        index.chunkCount.put(chunk.hash,1);
                        index.chunkSize.put(chunk.hash,chunk.data.length);
                    }
                    else
                        index.chunkCount.put(chunk.hash,index.chunkCount.get(chunk.hash) + 1);
                }
            }

			index.fileRecipe.put(file, recipe);
            index.update();
            index.report();

			
		}catch(Exception e){
            System.out.println(e);
        }  
		
    }
	
	static void download(Index index, String filename, String download_path, String mode) throws NullPointerException{
		try{
			if(!index.fileRecipe.containsKey(filename)) {
				System.err.println("Error: file not found. Download failed");
				return;
            }
            else {
				// checking if dir exists
				if(download_path.contains("/")){
					File tmp = new File(download_path);
					tmp.getParentFile().mkdirs();
				}
				
                FileOutputStream fos = new FileOutputStream(download_path);
                
                Queue<String> chunkNames = new LinkedList<>(index.fileRecipe.get(filename));
				String chunkpath = "";
                while(true) {
                    String chunkName = chunkNames.poll();
					chunkpath = "data/" + chunkName;
					if(mode.equals("azure")){
						AzureAPI.download(chunkpath);
						chunkpath = "azure_tmp/data/" + chunkName;
					}
                    if(chunkName != null) {
                        byte[] chk = null;
                        if(chunkName.charAt(0) != '*'){
                            chk = Files.readAllBytes(Paths.get(chunkpath));
                        }
                        else if(chunkName.charAt(0) == '*'){
                            chk = new byte[(int)Long.parseLong(chunkName.substring(1))];
                        }
                        fos.write(chk);                   
                        fos.flush();
                    }
                    else{
                        break;
                    }
                }              
                fos.close();
            }
		} catch(Exception e){
            System.out.println(e);
        }  
	}
	
	static String getMD5(byte[] data){
		String ret = null;
		try{
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(data);
			ret =DatatypeConverter.printHexBinary(md.digest());
		}catch(Exception e){
			System.out.println(e);
		}
		return ret;
	}
	
	static LinkedList<Chunk> getChunks(byte[] buffer, int m, int q, int d, int maxChunkSize) {
        LinkedList<Chunk> chunkList = new LinkedList<Chunk>();
        Queue<Integer> queue = new LinkedList<Integer>();

        if(m >= buffer.length) {
            chunkList.add(new Chunk(buffer));
            return chunkList;
        }

        int buf = 0;
        int index = 0;
        int rfp = 0;
        int length = 0;
        int offset = 0;
        boolean zeroFlag = true;
        while(true){
            if(length == 0){
                //if last chunk size < min_chunk
                if(offset + m >= buffer.length){
                    while(index < buffer.length){
                        buf = buffer[index++];
                        if(buf != 0)
                            zeroFlag = false;
                        length++;
                    }
                    if(zeroFlag){
                        Chunk c = new Chunk(new byte[0]);
                        c.isZero = true;
                        c.zeroLength = length;
                        chunkList.add(c);
                    }else
                        chunkList.add(new Chunk(Arrays.copyOfRange(buffer,offset,buffer.length)));
                    break;
                }else{
                    for(int i = 0;i < m;i++,index++,length++){
                        buf = buffer[index];
                        if(buf != 0)
                            zeroFlag = false;
                        queue.offer(buf);
						rfp = rfp + (buf*(int)Math.pow(d,m-i-1));
                    }
                    if(zeroFlag){
                        while(true){
                            if(index == buffer.length){
                                Chunk c = new Chunk(new byte[0]);
                                c.isZero = true;
                                c.zeroLength = length;
                                chunkList.add(c);
                                offset = index;
                                break;
                            }
                            buf = buffer[index];
                            if(buf != 0){
                                Chunk c = new Chunk(new byte[0]);
                                c.isZero = true;
                                c.zeroLength = length;
                                chunkList.add(c);
                                offset = index;
                                break;
                            }else{
                                index++;
                                length++;
                            }
                        }
                        queue.clear();
                        length = 0;
                        if(index == buffer.length)
                            break;
                        else
                            continue;
                    }
                }
            }
            //mark anocher point when meet requirement
            if((rfp&(q-1)) == 0 || length == maxChunkSize){
                chunkList.add(new Chunk(Arrays.copyOfRange(buffer,offset,offset + length)));
                offset = index;
                length = 0;
                queue.clear();
                zeroFlag = true;
            }else{
                buf = buffer[index];
                queue.offer(buf);
                index++;
                length++;
				rfp = d*(rfp - (int)Math.pow(d,m-1) * queue.remove()) +buf;
            }
            if(index == buffer.length){
                if(length > 0)
                    chunkList.add(new Chunk(Arrays.copyOfRange(buffer,offset,buffer.length)));
                break;
            }
        }
        return chunkList;
    }
	
	static void uploadChunk(String chunkName, byte[] data){
        try{
            String outputFileName = "data/" + chunkName;
			if (mode.equals("azure")) 
				outputFileName = "azure_tmp/data/" + chunkName;
            FileOutputStream fos = new FileOutputStream(outputFileName);
            fos.write(data);
            fos.flush();
            fos.close();
			if (mode.equals("azure")) 
				AzureAPI.upload(outputFileName);
        }catch(Exception e){
            System.out.println(e);
        }
    }
		
	static void delete(Index index, String file, String mode){
        if(!index.fileRecipe.containsKey(file)) {
            System.err.println("Error: File not exists.");
			return;
        }
        else {
            index.fileRecipe.get(file).parallelStream().forEach(chunk -> {
                if(chunk.charAt(0) != '*'){
                    int count = index.chunkCount.get(chunk);
                    if(count - 1 == 0){
                        index.chunkCount.remove(chunk);
                        index.chunkSize.remove(chunk);
						if(mode.equals("azure")){
							try{
								AzureAPI.delete("data/" + chunk);
							}
							catch(Exception e){
								return;
							}
						}else{
							File fileChunk = new File("data/" + chunk);
							fileChunk.delete(); 
						}                    
                    }
                    else
                        index.chunkCount.put(chunk, count-1);
                }
            });
            index.fileRecipe.remove(file);
            index.update();
			index.report();
            
        }
    }
	
	
	static class Index{

		public HashMap<String, Integer> chunkCount;      
        public HashMap<String, Queue<String>> fileRecipe;
        public HashMap<String, Integer> chunkSize;
        public int total_file_size;
        public int total_chunk_no;
		
		@SuppressWarnings("unchecked")
		public Index(){
			try{
				File f = new File(indexFile);
				if(f.exists() && f.length()>0) { 
					FileInputStream fis = new FileInputStream(indexFile);
					ObjectInputStream ois = new ObjectInputStream(fis);
					chunkCount = (HashMap<String,Integer>)ois.readObject();
                    fileRecipe = (HashMap<String, Queue<String>>)ois.readObject();
                    chunkSize = (HashMap<String, Integer>)ois.readObject();
                    total_file_size = (int)ois.readObject();
                    total_chunk_no = (int)ois.readObject();
					ois.close();
                }
                else{
					chunkCount = new HashMap<String,Integer>();
                    fileRecipe = new HashMap<String,Queue<String>>();
                    chunkSize = new HashMap<String, Integer>();
                    total_file_size = 0;
                    total_chunk_no = 0;
				}
			}catch(Exception e){
				System.out.println(e);
			}
		}

		public void update() {
			try {
				FileOutputStream fos = new FileOutputStream(indexFile);
				ObjectOutputStream oos = new ObjectOutputStream(fos);
				oos.writeObject(chunkCount);
                oos.writeObject(fileRecipe);
                oos.writeObject(chunkSize);
                oos.writeObject(total_file_size);
                oos.writeObject(total_chunk_no);
				oos.close();
				if(mode.equals("azure"))
					AzureAPI.upload(indexFile);
			} catch(Exception e) {
				System.out.println(e);
			}
        }
        
        public void report(){
            int ded_no = 0;
            int unique_byte = 0;
            int ded_byte = 0;
            for (String chunkID : chunkCount.keySet()) {
                unique_byte += chunkSize.get(chunkID);
            }           

            for (String fileName : fileRecipe.keySet()) {
                Queue<String> chunkNames = new LinkedList<>(fileRecipe.get(fileName));			
                while(true) {
                    String chunkName = chunkNames.poll();
                    if(chunkName != null) {
                        byte[] chk;
                        if(chunkName.charAt(0) != '*'){
                            ded_no++;
                            ded_byte += chunkSize.get(chunkName);
                        }
                        else if(chunkName.charAt(0) == '*'){
                            ded_no++;
                            ded_byte += (int)Long.parseLong(chunkName.substring(1));
                        }
                    }else{
                    break;
                    }
                }
            }

            double ratio = (double)Math.round(((double)ded_byte / (double)unique_byte) * 100d) / 100d;

            System.out.println("Total number of files that have been stored: " + Integer.toString(fileRecipe.size()));
            System.out.println("Total number of pre-deduplicated chunks in storage: " + Integer.toString(ded_no));
            System.out.println("Total number of unique chunks in storage: " + Integer.toString(chunkCount.size()));
            System.out.println("Total number of bytes of pre-deduplicated chunks in storage: " + Integer.toString(ded_byte));
            System.out.println("Total number of bytes of unique chunks in storage: " + Integer.toString(unique_byte));
            System.out.println("Deduplication ratio: " + Double.toString(ratio));
        }

	}
	
	
	private static boolean deleteDirectory(File directoryToBeDeleted) {
		File[] allContents = directoryToBeDeleted.listFiles();
		if (allContents != null) {
			for (File file : allContents) {
				deleteDirectory(file);
			}
		}
		return directoryToBeDeleted.delete();
	}
	
	
	
} 
	