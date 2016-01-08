package Util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

/**
 * Created by liye on 15/7/19.
 *
 */
public class InputData {
    static public void input(){
        Scanner scanner = new Scanner(System.in);
        String str1 = scanner.nextLine();
        int i1 = scanner.nextInt();
        float f1 = scanner.nextFloat();

        BufferedReader bu = new BufferedReader(new InputStreamReader(System.in));
        try {
            String str2 = bu.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
