#Import of dependencies
import os
import re
import requests
import zipfile
from io import BytesIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin

#Parameters:

#Base Link
BASE_URL = "https://www.pwc.com/us/en/careers/university-relations/data-and-analytics-case-studies-files.html"
#Download folder
DOWNLOAD_DIR = "./datalake/prelanding"
# allowed values for carga variable
valores_validos = ["-", "Sales", "PurchasesFINAL", "InvoicePurchases", "PurchasePrices", "BegInv", "EndInv"]
#Tipo de carga
carga = "-" 

#Let's validate that the variable carga is taking the right value
if carga not in valores_validos:
    raise ValueError(f"Valor inválido en 'carga': {carga}. Valores permitidos: {valores_validos}")

#It creates a folder 
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# HTTP header
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
}
# Let's clean the files
# In case they have problematic characters
def sanitize_filename(filename):
    """Limpia caracteres especiales en nombres de archivo"""
    return re.sub(r'[\\/*?:"<>|]', '_', filename).strip()


#Conection to the web site
try:
    print("Conexion a la pag principal")
    session = requests.Session()
    response = session.get(BASE_URL, headers=HEADERS, timeout=10)
    response.raise_for_status()
except Exception as e:
    print(f"Error al cargar la pag: {str(e)}")
    exit()

# Let's take the content of the website
#Let's conver raw html to objects
#Lets save it into a variable and the create a list to sace the values
soup = BeautifulSoup(response.content, 'html.parser')
zip_links = []

# Lets loop the links
# If carga is '-', then it bring the whole data. Otherwise, it only bring the specified data.

for link in soup.find_all('a', href=True):
    href = link['href'].lower()
    
    if carga == "-" or carga.lower() in href:
        if '.zip' in href:
            full_url = urljoin(BASE_URL, link['href'])
            zip_links.append(full_url)

print(f"Number of files found: {len(zip_links)}")

# Lets download Zips
#Let's open then in memory and then extract the csvs
#Lets save them into the right folder
total_csv = 0
for url in zip_links:
    try:
        print(f"Procesing : {url.split('/')[-1]}")
        r = session.get(url, headers=HEADERS, stream=True)
        r.raise_for_status()
        
        # ZIP en memoria
        with zipfile.ZipFile(BytesIO(r.content)) as zip_ref:
            csv_files = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
            
            if not csv_files:
                print(" No CSVs found")
                continue
                
            for csv_file in csv_files:
                # Let;s clean it and download it
                clean_name = sanitize_filename(os.path.basename(csv_file))
                if carga == "-":
                # Lets detect the category
                 subfolder = next((cat for cat in valores_validos if cat != "-" and cat.lower() in csv_file.lower()), "Otros")
                else:
                 subfolder = carga
                 # Subfolder is created if it doesnt exist

                output_dir = os.path.join(DOWNLOAD_DIR, subfolder)
                os.makedirs(output_dir, exist_ok=True) 
                #  final  path
                dest_path = os.path.join(output_dir, clean_name)


                
                with zip_ref.open(csv_file) as source, open(dest_path, 'wb') as dest:
                    dest.write(source.read())
                
                print(f"CSV saved: {clean_name}")
                total_csv += 1
                
    except Exception as e:
        print(f"Error : {str(e)}")

print(f"Total of downloaded CSVs: {total_csv}")
print(f"Destiny folder: {os.path.abspath(DOWNLOAD_DIR)}")
