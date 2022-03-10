package com.lidl;

public class SummationOfNumbers {

	public static void main(String[] args) {

		int intArr[] = {-1, -7, 2, 1, 0, 5, 3, 4, 6, -2};
		int target = 3;
		int len = intArr.length;

		SummationOfNumbers obj = new SummationOfNumbers();
		int output = obj.check(intArr, target, len);

		System.out.println("Total number of pair (indices) are :: "+output);
	}

	public int check(int intArr[], int target, int len) {

		int counter = 0;
		System.out.println("****************************************************");
		for (int i = 0; i < len; i++) {
			for (int j = 1; j < len - 1; j++) {
				if (intArr[i] + intArr[j] == target) {
					System.out.println("Pair found for the target with indices :: " + i + " , " + j
							+ " and values are :: " + intArr[i] + " , " + intArr[j]);
					counter += 1;
				}
			}

		}
		System.out.println("****************************************************");
		return counter;
	}
}
