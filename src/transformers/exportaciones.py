from datetime import datetime
import pandas as pd
import os

# Variables globales
# * Ruta de la carpeta donde se encuentran los archivos CSV
folder_path= os.path.join(os.getcwd(), 'data', 'bienes', 'exportaciones')

# * Diccionario para mapear los tipos de mes a números
tipo_mes = {
    'E': 1, # Enero
    'EN': 2, # Enero
    'EF': 3, # Enero - Febrero
    'EMZ': 4, # Enero - Marzo
    'EAB': 5, # Enero - Abril
    'EA': 6, # Enero - Abril
    'EMY': 7, # Enero - Mayo
    'EMJ': 8, # Enero - Junio
    'EJL': 9, # Enero - Julio
    'EAG': 10, # Enero - Agosto
    'ES': 11, # Enero - Septiembre
    'EOC': 12, # Enero - Octubre
    'ENO': 13, # Enero - Noviembre
}

def exportaciones_por_mes():
    for file in os.listdir(folder_path):
        if file.endswith('.xlsb'):
            file_path = os.path.join(folder_path, file)

            # Leer específicamente la hoja 'TABLA'
            df = pd.read_excel(file_path, sheet_name='TABLA', engine='pyxlsb')
            
            print(f"Archivo: {file}")
            print(f"Usando la hoja: TABLA")
            print(df.head())
            
            # Mostrar resultados
            print(f"Resultados para el archivo: {file}")
            print(df)

def exportaciones_por_mes_hoja_especifica(nombre_hoja='TABLA'):
    """
    Función para procesar archivos Excel especificando qué hoja usar
    
    Args:
        nombre_hoja (str): Nombre de la hoja a usar (por defecto 'TABLA')
    """
    for file in os.listdir(folder_path):
        if file.endswith('.xlsb'):
            file_path = os.path.join(folder_path, file)

            # Verificar qué hojas están disponibles
            excel_file = pd.ExcelFile(file_path, engine='pyxlsb')
            sheet_names = excel_file.sheet_names
            
            print(f"Archivo: {file}")
            print(f"Hojas disponibles: {sheet_names}")
            
            # Verificar si la hoja especificada existe
            if nombre_hoja in sheet_names:
                df = pd.read_excel(file_path, sheet_name=nombre_hoja, engine='pyxlsb')
                print(f"Usando la hoja: {nombre_hoja}")
                print(df.head())
                print(f"Resultados para el archivo: {file}")
                print(df)
            else:
                print(f"La hoja '{nombre_hoja}' no existe en el archivo {file}")
                print(f"Hojas disponibles: {sheet_names}")
            
            print("-" * 50)  # Separador entre archivos

def obtener_nombre_hoja_por_archivo(archivo):
    """
    Construye el nombre de la hoja basándose en el patrón X20_25TIPO_MES
    
    Args:
        archivo (str): Nombre del archivo Excel
    
    Returns:
        str: Nombre de la hoja construido
    """
    # Extraer el tipo de mes del nombre del archivo
    # Por ejemplo: OEE_MA_Exportaciones_2020_mar_2025.xlsb -> 'mar'
    partes = archivo.split('_')
    
    # Buscar la parte que contiene el mes
    mes_abrev = None
    for parte in partes:
        if parte.lower() in ['mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic']:
            mes_abrev = parte.lower()
            break
    
    # Mapear abreviaciones de mes a códigos del tipo_mes
    mes_a_codigo = {
        'mar': 'EMZ',  # Enero - Marzo
        'abr': 'EAB',  # Enero - Abril  
        'may': 'EMY',  # Enero - Mayo
        'jun': 'EMJ',  # Enero - Junio
        'jul': 'EJL',  # Enero - Julio
        'ago': 'EAG',  # Enero - Agosto
        'sep': 'ES',   # Enero - Septiembre
        'oct': 'EOC',  # Enero - Octubre
        'nov': 'ENO',  # Enero - Noviembre
    }
    
    if mes_abrev and mes_abrev in mes_a_codigo:
        tipo_mes_codigo = mes_a_codigo[mes_abrev]
        # Construir el nombre de la hoja: X20_25TIPO_MES
        nombre_hoja = f"X20_25{tipo_mes_codigo}"
        return nombre_hoja
    
    return None

def exportaciones_con_hoja_automatica():
    """
    Función que automáticamente determina qué hoja usar basándose en el nombre del archivo
    """
    for file in os.listdir(folder_path):
        if file.endswith('.xlsb'):
            file_path = os.path.join(folder_path, file)

            # Verificar qué hojas están disponibles
            excel_file = pd.ExcelFile(file_path, engine='pyxlsb')
            sheet_names = excel_file.sheet_names
            
            print(f"Archivo: {file}")
            print(f"Hojas disponibles: {sheet_names}")
            
            # Intentar obtener el nombre de la hoja automáticamente
            nombre_hoja_automatica = obtener_nombre_hoja_por_archivo(file)
            
            if nombre_hoja_automatica and nombre_hoja_automatica in sheet_names:
                df = pd.read_excel(file_path, sheet_name=nombre_hoja_automatica, engine='pyxlsb')
                print(f"Usando la hoja automática: {nombre_hoja_automatica}")
                print(df.head())
                print(f"Resultados para el archivo: {file}")
                print(df)
            else:
                # Si no se puede determinar automáticamente, usar 'TABLA'
                df = pd.read_excel(file_path, sheet_name='TABLA', engine='pyxlsb')
                print(f"Usando la hoja por defecto: TABLA")
                print(df.head())
                print(f"Resultados para el archivo: {file}")
                print(df)
            
            print("-" * 50)  # Separador entre archivos