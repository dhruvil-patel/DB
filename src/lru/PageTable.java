package lru;

import java.util.Iterator;
import java.util.LinkedList;


public class PageTable {

		public PageTableEntry []p; 
		public int pagesOccupied;
		public int pageCount;
		//
		LinkedList <Integer> timestamp = new LinkedList<Integer>();
		//
		PageTable(int pageCount) {        // Start of constructor  		
			p = new  PageTableEntry [pageCount];
			pagesOccupied = 0;
			this.pageCount = pageCount;
			for(int i=0;i<this.pageCount;i++) { // For loop
				p[i] = new PageTableEntry();
				p[i].pageNumber = -1;
				p[i].tableId = -1;
				p[i].modifiedBit = false;
			}    //End of For loop
				
		}  // End of constructor
		
		
		public int lru(int pageNumber , int tableId) {    // main lru		
			int check = searchPage(pageNumber , tableId);
			if(check == -1) {					 // if miss
				//System.out.println("Miss");
				if(pagesOccupied < pageCount){   //if miss and frames are empty
					//System.out.println("Miss "+pagesOccupied );
					fillPageTable(pageNumber,tableId);
					return -1;
				}	
				else {							//if miss and frames are full
						int frame = timestamp.getLast();
						//System.out.println("Miss " +frame);
						
						//System.out.println("Page removed is "+p[frame].pageNumber+" from frame " + frame);
						updatePageTable(frame,pageNumber,tableId);
						return -(frame+2);
						
					
				}
			}	
			else{								//if hit
				//System.out.println("Hit");
				updateRefrence(check);
				return check;
			}
		}	
			
		
		
		public int searchPage(int pageNumber , int tableId) { 	//check in frames		
			for(int i = 0 ; i< pagesOccupied ; i++) {
				if(p[i].tableId == tableId && p[i].pageNumber == pageNumber)
					return i;
			}
			return -1;
				
		}
		
		public void fillPageTable(int pageNumber , int tableId) {	//if frames are empty fill it
		
			p[pagesOccupied].pageNumber = pageNumber;
			p[pagesOccupied].tableId = tableId;
			timestamp.addFirst(pagesOccupied);
			pagesOccupied++;
			//11
		}
		
		public void updateRefrence(int frameNumber) { 		// if hit update time
			
			//if Content is updated set modified bit
			timestamp.removeFirstOccurrence(frameNumber);
			//printPageTable();
			timestamp.addFirst(frameNumber);
		}
		
		
		//If pagesOccupied is greater than pageCount
		public void updatePageTable(int frameNumber,int pageNumber , int tableId) {
			
			if(p[frameNumber].modifiedBit == true )
				;// Perform disk writes
			else {
				
				p[frameNumber].modifiedBit = false;
				p[frameNumber].pageNumber = pageNumber;
				p[frameNumber].tableId = tableId;
				timestamp.removeLast();
				//printPageTable();
				timestamp.addFirst(frameNumber);
			}
				
		}
		
		public void printPageTable(){
			
			Iterator<Integer> x = timestamp.listIterator(0);

		      // print list with the iterator
		      while (x.hasNext()) {
		         System.out.println(x.next());
		      } 
		         /*
			for(int i=0;i<pageCount;i++){
				System.out.println("Frame "+i+" Page "+p[i].pageNumber) ;
			}*/
				
		}
			
	}


class PageTableEntry {
	
	int pageNumber;
	int tableId;
	boolean modifiedBit;

}