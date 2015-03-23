package lru;
import java.util.*;
public class FrameBuffer {
		private int frameSize;
		private int frameCount;
		public  Vector< Vector<String> > frameBuffer ;
		
		public  FrameBuffer(int pageSize , int pageCount)
		{
			this.frameSize = pageSize;
			this.frameCount = pageCount;
			allocateBuffer();
			initializeBuffer();
		}
		
		public void allocateBuffer() {
			
			frameBuffer = new Vector< Vector<String> >();
			frameBuffer.ensureCapacity(frameCount); 
		}
		
		
		public void initializeBuffer() {
			//Vector <String> s;
			for(int i=0;i<frameCount ; i++){
				
				frameBuffer.add(i,new Vector<String>());
				//frameBuffer.get(i).add("Hi");
				//frameBuffer.get(i).add("Hello");
				//s.add("Hello");
				//this.frameBuffer.addElement(s);
			}	
		}
		
		public void printBuffer() {
			
			for(int i=0;i<frameCount ; i++) {
				System.out.println(i+" "+new String(frameBuffer.get(i).elementAt(0)));
				System.out.println(i+" "+new String(frameBuffer.get(i).elementAt(1)));
			}	
			
		}
		/*
		public static void main(String []args) {
			
			FrameBuffer f = new FrameBuffer(10,10);
			//f.allocateBuffer();
			//f.initializeBuffer();
			f.printBuffer();
			
		}*/
			
}
