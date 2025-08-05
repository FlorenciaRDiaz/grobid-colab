
# Procesamiento por lotes de PDFs académicos usando GROBID

Este repositorio automatiza la extracción de texto limpio, título, abstract y keywords desde PDFs científicos usando GROBID en Google Colab o localmente.

## Instrucciones

1. Ejecutá el servidor GROBID:
   ```bash
   git clone https://github.com/kermitt2/grobid.git
   cd grobid
   ./gradlew clean install
   nohup ./gradlew run &
