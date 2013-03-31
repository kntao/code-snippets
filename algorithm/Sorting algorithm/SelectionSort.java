public class SelectionSort{
	public static int [] sort(int [] a){
		for(int i = a.length-1; i >= 0; i--){
			for(int j = i -1; j >= 0; j--){
				if(a[j] > a[i]){
					int t = a[i];
					a[i]= a[j];
					a[j] = t;
				}
			}
		}
		return a;
	}
	public static void main(String[] args){
		int [] a ={6,2,4,1,5,9};
		System.out.println("排序前:");
		printArray(a);
		System.out.println("排序后:");
		sort(a);
		printArray(a);
	}
	
	public static void printArray(int[] a){
		for(int i : a){
			System.out.print(i + ",");
		}
		System.out.println();
	}
}
