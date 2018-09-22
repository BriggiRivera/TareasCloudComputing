# TareasCloudComputing

Para generar archivo grande:

1. Situarse en la carpeta build por medio de la consola.
2. Ejecutar el siguiente comando (archivo > 10Gb, depende del parametro el tamaño del archivo de salida):
   java wordcountsinhadoop.WordCountSinHadoop --generar <archivo_salida> 1000000000
   
Para ejecutar conteo:
1. Situarse en la carpeta build por medio de la consola.
2. Ejecutar el siguiente comando:
   java wordcountsinhadoop.WordCountSinHadoop <archivo_a_procesar> > <archivo_resultado>
