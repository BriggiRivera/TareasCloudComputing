
package wordcountsinhadoop;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class Cantidad {
    private int cantidad;
    
    public Cantidad(int cantidad) {
        this.cantidad = cantidad;
    }
    
    public int getCantidad() {
        return cantidad;
    }
    
    public void sumar(int otraCantidad) {
        cantidad += otraCantidad;
    }
}

class LeePalabras implements Callable<Map<String, Cantidad>> {    
    private final Map<String, Cantidad> palabras;
    private RandomAccessFile fileInput;
    private long realPosition;
    private final long nChunks;
    private long end;
    private final byte[] buffer;
    
    public LeePalabras(File file, int nChunks, int position, int tamanhoBuffer) {
        palabras = new HashMap<>();
        crearAcceso(file, nChunks, position);
        this.nChunks = nChunks;
        this.end = 0;
        buffer = new byte[tamanhoBuffer];
    }
    
    private void crearAcceso(File file, int nChunks, int position) {
        try {
            realPosition = (file.length() / nChunks) * position;
            fileInput = new RandomAccessFile(file, "r");
            fileInput.seek(getRealPosition());
            moverPosicionInicialCorrecta();
        } catch (Exception ex) {
            Logger.getLogger(LeePalabras.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void moverPosicionInicialCorrecta() throws IOException
    {
        final byte EOF = -1;
        
        if(fileInput.getFilePointer() == 0)
            return;
        
        char c = (char)fileInput.readByte();
        
        while (c != EOF && Character.isAlphabetic(c)){
            realPosition++;
            c = (char)fileInput.readByte();
        }
    }
    
    @Override
    public Map<String, Cantidad> call() {
        try {
            StringBuilder builder = new StringBuilder();
            char c;
            Cantidad cantidad;
            String palabra;
            int bufferIndex = 0;
            
            while(this.realPosition <= end)
            {
                bufferIndex = bufferIndex%buffer.length;
                if (bufferIndex == 0) {
                    fileInput.read(buffer);
                }
                c = (char)buffer[bufferIndex];
                if ( Character.isAlphabetic(c) ) {
                    builder.append(c);
                } else {
                    palabra = builder.toString();
                    if(palabra.length()>0) {
                        cantidad = palabras.get(palabra);
                        if (cantidad == null) {
                            palabras.put(palabra, new Cantidad(1));
                        } else {
                            cantidad.sumar(1);
                        }
                        
                        builder.setLength(0);
                    }
                }
                bufferIndex++;
                realPosition++;
            }
        } catch (IOException ex) {
            Logger.getLogger(LeePalabras.class.getName()).log(Level.SEVERE, null, ex);
        }
        return this.palabras;
    }

    public long getRealPosition() {
        return realPosition;
    }
    
    public void setEnd(long end) {
        this.end = end;
    }
}

public class WordCountSinHadoop {
    
    public static void main(String[] args) throws Exception{
        if (args.length > 0) {
            
            if ("--generar".equals(args[0])) {
                GenerarArchivoGrande generador = new GenerarArchivoGrande();
                generador.generar(args[1], Long.parseLong(args[2]));
            } else {
            
                File file = new File(args[0]);
                int nChunks = 48;
                int tamanhoBuffer = 1024 * 1024;

                // Creamos lectores dependiendo de cada pedazo del archivo gigante
                List<LeePalabras> lectores = IntStream
                        .range(0, nChunks)
                        .mapToObj(position -> new LeePalabras(file, nChunks, position, tamanhoBuffer))
                        .collect(Collectors.toList());

                // Establecemos el fin de lectura para cada lector
                for(int index=1; index<lectores.size() ;index++) {
                    lectores.get(index-1).setEnd(lectores.get(index).getRealPosition());
                }
                lectores.get(lectores.size()-1).setEnd(file.length()-1);

                // Creamos "nChunks" hilos para lanzar y lean paralelamente
                ExecutorService service = Executors.newFixedThreadPool(nChunks);

                // Invocamos los hilos
                List<Future<Map<String, Cantidad>>> resultados = service.invokeAll(lectores);

                // Esperamos que los hilos terminen su trabajo
                service.shutdown();

                Map<String, Cantidad> total = new HashMap<>();

                resultados.forEach(palabras -> {
                    try {
                        palabras.get().forEach((palabra, cantidad) -> {
                            Cantidad cantidadTotal = total.get(palabra);
                            if (cantidadTotal == null) {
                                total.put(palabra, new Cantidad(cantidad.getCantidad()));
                            } else {
                                cantidadTotal.sumar(cantidad.getCantidad());
                            }
                        });
                    } catch (InterruptedException | ExecutionException ex) {
                        Logger.getLogger(WordCountSinHadoop.class.getName()).log(Level.SEVERE, null, ex);
                    }
                });

                total.forEach((palabra, cantidad) -> {
                    System.out.println(palabra + ":" + cantidad.getCantidad());
                } );
            }
        }
    }  
}
