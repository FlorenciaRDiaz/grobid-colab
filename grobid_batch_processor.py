"""
Script para procesar PDFs en lotes con GROBID y guardar resultados en Google Drive.

Flujo:
1. Instala dependencias necesarias (forzando versión estable de grobid-client).
2. Monta Google Drive en Colab.
3. Procesa PDFs desde /MyDrive/articles.
4. Guarda TEI y TXT en /MyDrive/results.

Requisitos:
- Archivos PDF deben estar en Google Drive, carpeta: /MyDrive/articles
"""

# grobid_batch_processor.py

import os, shutil, re, time, gc, requests
import xml.etree.ElementTree as ET
from tqdm import tqdm
from grobid_client.grobid_client import GrobidClient

NS = {'tei': 'http://www.tei-c.org/ns/1.0'}

def is_grobid_alive(url="http://localhost:8070/api/isalive"):
    try:
        r = requests.get(url, timeout=5)
        return r.status_code == 200
    except:
        return False

def extract_clean_text(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()
    content = []

    title = root.find(".//tei:titleStmt/tei:title", NS)
    if title is not None and title.text:
        content.append("TITLE: " + title.text.strip())

    kws = root.findall(".//tei:keywords/tei:term", NS)
    if kws:
        content.append("KEYWORDS: " + ", ".join(kw.text.strip() for kw in kws if kw.text))

    abstract = root.find(".//tei:abstract", NS)
    if abstract is not None:
        content.append("ABSTRACT: " + " ".join(abstract.itertext()).strip())

    body = root.find(".//tei:body", NS)
    if body is not None:
        parts = []
        for e in body.iter():
            tag = e.tag.replace(f"{{{NS['tei']}}}", "")
            if tag in {"head", "p", "label", "list", "figure"}:
                txt = " ".join(e.itertext()).strip()
                if txt:
                    parts.append(txt)
        full = "\n\n".join(parts)
        clean = re.split(r"(references|bibliography|acknowledg|agradecimientos)", full, flags=re.IGNORECASE)[0].strip()
        content.append(clean)

    return "\n\n".join(content)

def process_pdfs_in_batches(
    input_dir,
    output_dir,
    batch_size=5,
    max_retries=3,
    restart_every=5
):
    tei_folder = os.path.join(output_dir, "articulos_tei")
    txt_folder = os.path.join(output_dir, "articulos_txt")
    os.makedirs(tei_folder, exist_ok=True)
    os.makedirs(txt_folder, exist_ok=True)

    client = GrobidClient(grobid_server="http://localhost:8070", timeout=300)
    pdfs = [f for f in os.listdir(input_dir) if f.lower().endswith('.pdf')]
    print(f"Se encontraron {len(pdfs)} PDFs.")

    start = time.time()
    for idx, start_idx in enumerate(range(0, len(pdfs), batch_size)):
        batch = pdfs[start_idx:start_idx + batch_size]
        lote_num = idx + 1
        print(f"\nProcesando lote {lote_num} con {len(batch)} archivos...")

        if not is_grobid_alive():
            print("GROBID no disponible. Deteniendo.")
            break

        tmp_input = "/content/tmp_pdfs"
        tmp_output = "/content/tmp_tei"
        os.makedirs(tmp_input, exist_ok=True)
        os.makedirs(tmp_output, exist_ok=True)

        for fname in batch:
            shutil.copy(os.path.join(input_dir, fname), tmp_input)

        for attempt in range(1, max_retries + 1):
            try:
                client.process(
                    input_path=tmp_input,
                    output=tmp_output,
                    service="processFulltextDocument",
                    consolidate_citations=False
                )
                break
            except Exception as e:
                print(f"Error en lote (intento {attempt}): {e}")
                if attempt == max_retries:
                    print("Saltando lote.")
                else:
                    time.sleep(30)

        for fname in tqdm(os.listdir(tmp_output), desc="Guardando resultados"):
            if fname.endswith(".tei.xml"):
                xml_path = os.path.join(tmp_output, fname)
                shutil.copy(xml_path, os.path.join(tei_folder, fname))
                try:
                    txt = extract_clean_text(xml_path)
                    with open(os.path.join(txt_folder, fname.replace(".tei.xml", ".txt")), "w", encoding="utf-8") as f:
                        f.write(txt)
                except Exception as e:
                    print(f"Error procesando {fname}: {e}")

        shutil.rmtree(tmp_input, ignore_errors=True)
        shutil.rmtree(tmp_output, ignore_errors=True)
        gc.collect()
        time.sleep(60)

        if lote_num % restart_every == 0:
            print("¡Sugerencia! Reiniciá GROBID si notás cuelgues.")

    elapsed = time.time() - start
    print(f"\n¡Listo! Procesados {len(pdfs)} PDFs en {elapsed/60:.2f} minutos.")
