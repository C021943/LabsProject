#Se importan las librerias necesarias
import os
import re
import requests
import zipfile
from io import BytesIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin

#PARAMETROS:

#lINK Base
BASE_URL = "https://www.pwc.com/us/en/careers/university-relations/data-and-analytics-case-studies-files.html"
#Carpeta de descarga
DOWNLOAD_DIR = "./datalake/prelanding"
# Valores permitidos para la variable 'carga'
valores_validos = ["-", "Sales", "PurchasesFINAL", "InvoicePurchases", "PurchasePrices", "BegInv", "EndInv"]
#Tipo de carga
carga = "-" 

#vALIDAMOS que la variable carga tome el valor correcto# Validación
if carga not in valores_validos:
    raise ValueError(f"Valor inválido en 'carga': {carga}. Valores permitidos: {valores_validos}")

#Crea una carpeta en donde le estamos especificando
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

#Cabecera HTTP
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
}
# Limpio el nombre del archivo
# Es decir, si tiene caracteres que no permitidos o problematicos, los reemplazamos por guin bajo
def sanitize_filename(filename):
    """Limpia caracteres especiales en nombres de archivo"""
    return re.sub(r'[\\/*?:"<>|]', '_', filename).strip()


#Conexion a la pagina principal
try:
    print("Conexion a la pag principal")
    session = requests.Session()
    response = session.get(BASE_URL, headers=HEADERS, timeout=10)
    response.raise_for_status()
except Exception as e:
    print(f"Error al cargar la pag: {str(e)}")
    exit()

#Tomamos el contenido de la pagina
#lo convertimos al html crudo en objetos
#lo guardamos en una variable y luego creamos una lista para guardar los links
soup = BeautifulSoup(response.content, 'html.parser')
zip_links = []

# recorremos los links
# si la variable cargga es un guion, se trae todos los archivos. sino, se trae el archivo seleccionado

for link in soup.find_all('a', href=True):
    href = link['href'].lower()
    
    if carga == "-" or carga.lower() in href:
        if '.zip' in href:
            full_url = urljoin(BASE_URL, link['href'])
            zip_links.append(full_url)

print(f"Numero de archivos encontrados: {len(zip_links)}")

# Descargamos Zips
#Los abrimos en momeria y les extraemos el csv
#Lo guardamos en carpeta correspondiente
total_csv = 0
for url in zip_links:
    try:
        print(f"Se procesa : {url.split('/')[-1]}")
        r = session.get(url, headers=HEADERS, stream=True)
        r.raise_for_status()
        
        # ZIP en memoria
        with zipfile.ZipFile(BytesIO(r.content)) as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
            
            if not csv_files:
                print(" No contiene archivos CSV")
                continue
                
            for csv_file in csv_files:
                # limpiamos el nombre si es necesario y descargamos
                clean_name = sanitize_filename(os.path.basename(csv_file))
                if carga == "-":
                # detectamos en que categoria esta
                 subfolder = next((cat for cat in valores_validos if cat != "-" and cat.lower() in csv_file.lower()), "Otros")
                else:
                 subfolder = carga
                 # se crea la subcarpeta si no esta

                output_dir = os.path.join(DOWNLOAD_DIR, subfolder)
                os.makedirs(output_dir, exist_ok=True) 
                # Ruta final 
                dest_path = os.path.join(output_dir, clean_name)


                
                with zip_ref.open(csv_file) as source, open(dest_path, 'wb') as dest:
                    dest.write(source.read())
                
                print(f"CSV guardado: {clean_name}")
                total_csv += 1
                
    except Exception as e:
        print(f"Error : {str(e)}")

print(f"Total de CSVs descargados: {total_csv}")
print(f"Carpeta destino: {os.path.abspath(DOWNLOAD_DIR)}")