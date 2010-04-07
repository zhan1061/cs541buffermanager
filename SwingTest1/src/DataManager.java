import java.util.Hashtable;


public class DataManager {
	static int WRONG_DM = -1;
	static int NO_SUCH_ACCOUNT = -2;
	static int SUCCESSFUL_OPERATION = 1;
	
	private int nextAccountNo = 2;
	private int branchName;
	Hashtable<Integer,Double> account = new Hashtable<Integer,Double>();
	
	public DataManager(int branch)
	{
		this.branchName = branch;
		account.put(1, 100.00);
		account.put(0,10.50);
	}
	
	public int createAccount(int branch)
	{
		if (branch!=this.branchName)
		{
			return WRONG_DM; //-1 means error (this DM is of another branch) 
		}
		else{
			account.put(nextAccountNo,00.00);
			nextAccountNo++;
			return (nextAccountNo-1);
		}
	}
	public int deleteAccount(int branch, int accNo)
	{
		if (branch!=this.branchName)
		{
			return WRONG_DM; // means wrong branch  
		}
		else{
			if (account.containsKey(accNo))
			{
				account.remove(accNo);
				return SUCCESSFUL_OPERATION; //successfully deleted accnt
			}
			else
			{
				return NO_SUCH_ACCOUNT; //means no such account exists
			}
			
		}
	}
	
	public double[] readBalance(int branch, int accNo)
	{
		if (branch!=this.branchName)
		{   double a[] = {WRONG_DM,0.0};  // means wrong branch
			return a;   
		}
		else{
			if (account.containsKey(accNo))
			{
				double a[] = {SUCCESSFUL_OPERATION,account.get(accNo)};//successfully return bal in accnt
				return a; 
			}
			else
			{
				double a[] = {NO_SUCH_ACCOUNT,0.0};//means no such account exists
				return a; 
			}
			
		}
	}
	
	public int writeBalance(int branch, int accNo, double amount)
	{
		if (branch!=this.branchName)
		{
			return WRONG_DM; // means wrong branch  
		}
		else{
			if (account.containsKey(accNo))
			{
				account.remove(accNo);
				account.put(accNo, amount);
				return SUCCESSFUL_OPERATION; //successfully updated bal in accnt
			}
			else
			{
				return NO_SUCH_ACCOUNT; //means no such account exists
			}
			
		}
		
	}
		
	public int depositAmount(int branch, int accNo, double amount)
	{
			if (branch!=this.branchName)
			{
				return WRONG_DM; // means wrong branch  
			}
			else{
				if (account.containsKey(accNo))
				{   double currentBalance = account.get(accNo);
					currentBalance = currentBalance + amount;
					account.remove(accNo);
					account.put(accNo, currentBalance);
					return SUCCESSFUL_OPERATION; //successfully updated bal in accnt
				}
				else
				{
					return NO_SUCH_ACCOUNT; //means no such account exists
				}
				
			}
		
	}
	
	public int withdrawAmount(int branch, int accNo, double amount)
	{
			if (branch!=this.branchName)
			{
				return WRONG_DM; // means wrong branch  
			}
			else{
				if (account.containsKey(accNo))
				{   double currentBalance = account.get(accNo);
					currentBalance = currentBalance - amount;
					account.remove(accNo);
					account.put(accNo, currentBalance);
					return SUCCESSFUL_OPERATION; //successfully updated bal in accnt
				}
				else
				{
					return NO_SUCH_ACCOUNT; //means no such account exists
				}
				
			}
		
	}
	
}
