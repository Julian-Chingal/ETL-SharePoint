"""
Script de configuración inicial para el ETL de SharePoint
"""
import os
import shutil

def create_env_file():
    """Crea archivo .env desde el template"""
    if not os.path.exists('.env') and os.path.exists('.env.example'):
        shutil.copy('.env.example', '.env')
        print("✓ Archivo .env creado desde .env.example")
        print("⚠️  IMPORTANTE: Edita el archivo .env con tus credenciales reales")
    elif os.path.exists('.env'):
        print("✓ Archivo .env ya existe")
    else:
        print("⚠️  No se encontró .env.example")

def create_directories():
    """Crea los directorios necesarios"""
    directories = [
        'data/raw',
        'data/processed', 
        'data/output',
        'logs'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        print(f"✓ Directorio creado: {directory}")

def main():
    print("=== CONFIGURACIÓN INICIAL DEL ETL ===")
    
    # Crear directorios
    create_directories()
    
    # Crear archivo .env
    create_env_file()
    
    print("\n=== PRÓXIMOS PASOS ===")
    print("1. Edita el archivo .env con tus credenciales de SharePoint y base de datos")
    print("2. Ejecuta: python src/main.py")
    print("3. Revisa los logs en: logs/etl_process.log")
    
    print("\n=== CONFIGURACIÓN DE .ENV ===")
    print("Necesitas configurar las siguientes variables:")
    print("- SHAREPOINT_SITE_URL: URL de tu sitio de SharePoint")
    print("- SHAREPOINT_USERNAME: Tu usuario de SharePoint")
    print("- SHAREPOINT_PASSWORD: Tu contraseña de SharePoint")
    print("- DATABASE_URL: Cadena de conexión a tu base de datos")

if __name__ == "__main__":
    main()
