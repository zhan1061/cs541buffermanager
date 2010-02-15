package bufmgr;

import java.util.*;


public class ClockReplace {
    private LinkedList<Integer> replaceFrameList;
   
    public ClockReplace(){
        replaceFrameList = new LinkedList<Integer>();
    }
   
   
    public void addToList(int frame){
        replaceFrameList.add(new Integer(frame));
    }
   
    public void removeFromList(int frame){
        for(int i = 0; i < replaceFrameList.size(); i++)
        {
            if((replaceFrameList.get(i)).intValue() == frame)
            {
                replaceFrameList.remove(i); break;
            }
        }
    }

    public int getReplaceFrame() {
        if (replaceFrameList.isEmpty())
            {
            return -1;
             }
        else
        {
            return ((replaceFrameList).removeFirst()).intValue();
        }
    }
   
   
}