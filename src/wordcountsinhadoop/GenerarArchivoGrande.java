package wordcountsinhadoop;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;

public class GenerarArchivoGrande {
    List<String> palabras = new ArrayList<>();
    
    public GenerarArchivoGrande() throws Exception{
        File file = new File("Palabras.txt");
        if (file.exists()) {
            Scanner input=new Scanner(file);

            while(input.hasNext()){
                palabras.add(input.next());
            }
        } else {
            System.out.println("Archivo no existe.");
        }
    }
    
    public void generar(String archivoSalida, long cantidad) throws Exception{
        try (FileWriter salida = new FileWriter(archivoSalida)) {
            PrintWriter writer = new PrintWriter(salida);
            Random rm = new Random();
            for(long index=0;index<cantidad;index++) {
                writer.print(palabras.get(rm.nextInt(palabras.size())) + " ");
            }
        }
    }
}
