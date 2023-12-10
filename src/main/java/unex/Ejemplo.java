package unex;

import com.github.javafaker.Faker;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Scanner;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class Ejemplo {
    static Scanner in = new Scanner(System.in);

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        boolean opcionValida = false;
        while (!opcionValida) {
            System.out.print("Introduce un numero: ");
            // Verificar si la entrada es un número
            if (scanner.hasNextInt()) {
                int numero = scanner.nextInt();
                opcionValida = true;
                System.out.println("Has introducido un número: " + numero);
            } else {
                // La entrada no es un número
                System.out.println("La entrada no es un número.");
                scanner.next();
            }
        }

        scanner.close();
    }


}
