# ETL SharePoint - Sistema de Automatización de Datos Comerciales

Este proyecto implementa un sistema ETL (Extract, Transform, Load) completo para extraer archivos Excel desde SharePoint, transformar los datos y cargarlos en una base de datos.

## 🚀 Características

- **Extracción**: Conexión automática a SharePoint para descargar archivos Excel
- **Transformación**: Limpieza, estandarización y estructuración de datos
- **Carga**: Almacenamiento en base de datos
- **Logging**: Sistema completo de logs para monitoreo
- **Configuración**: Gestión de credenciales mediante variables de entorno
- **Robustez**: Manejo de errores y reintentos automáticos

## 📁 Estructura del Proyecto

```
automatizacion/
├── src/
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py          # Configuraciones generales
│   ├── extractors/
│   │   ├── __init__.py
│   │   └── sharepoint_extractor.py  # Extractor de SharePoint
│   ├── transformers/
│   │   ├── __init__.py
│   │   ├── excel_transformer.py     # Transformador de Excel
│   │   └── exportaciones.py
│   ├── loaders/
│   │   ├── __init__.py
│   │   └── data_loader.py           # Cargador de datos
│   ├── utils/
│   │   ├── __init__.py
│   │   └── helpers.py               # Utilidades y logging
│   └── main.py                      # Script principal
├── data/
│   ├── raw/                         # Datos extraídos
│   ├── processed/                   # Datos procesados
│   └── output/                      # Datos finales
├── logs/                            # Archivos de log
├── .env.example                     # Template de configuración
├── requirements.txt                 # Dependencias Python
└── setup.py                        # Script de configuración inicial
```

## 🛠️ Instalación

### 1. Clonar/Descargar el proyecto

### 2. Crear entorno virtual (recomendado)

```bash
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Configuración inicial

```bash
python setup.py
```

### 5. Configurar credenciales

Edita el archivo `.env` con tus credenciales reales:

```env
# SharePoint Configuration
SHAREPOINT_SITE_URL=https://tuorganizacion.sharepoint.com/sites/tusite
SHAREPOINT_USERNAME=tu_usuario@tuorganizacion.com
SHAREPOINT_PASSWORD=tu_contraseña
SHAREPOINT_FOLDER_PATH=/sites/tusite/Shared Documents/TuCarpeta

# Database Configuration
DATABASE_URL=mssql+pyodbc://usuario:contraseña@servidor:puerto/basededatos?driver=ODBC+Driver+17+for+SQL+Server

# Logging Configuration
LOGGING_LEVEL=INFO
```

## 🔧 Configuración de Base de Datos

### SQL Server
```env
DATABASE_URL=mssql+pyodbc://usuario:contraseña@servidor:puerto/basededatos?driver=ODBC+Driver+17+for+SQL+Server
```

### PostgreSQL
```env
DATABASE_URL=postgresql://usuario:contraseña@servidor:puerto/basededatos
```

### SQLite (para pruebas)
```env
DATABASE_URL=sqlite:///data/database.db
```

## 🚀 Uso

### Ejecución completa del ETL

```bash
python src/main.py
```

### Prueba con archivos locales

```bash
python test_local.py
```

### Solo probar conexiones

Edita `src/main.py` y descomenta la línea `test_connections()`:

```python
if __name__ == "__main__":
    test_connections()  # Descomenta esta línea
    # main()            # Comenta esta línea
```

## 📊 Tipos de Datos Soportados

El sistema está configurado para procesar:

- **Exportaciones**: Archivos con datos de exportaciones comerciales
- **Importaciones**: Archivos con datos de importaciones
- **Servicios**: Datos de comercio de servicios
- **Turismo**: Estadísticas de turismo
- **Inversión**: Datos de inversión extranjera

## 🔍 Monitoreo y Logs

Los logs se almacenan en:
- `logs/etl_process.log`: Log detallado del proceso
- Consola: Output en tiempo real

Niveles de log configurables:
- `DEBUG`: Información muy detallada
- `INFO`: Información general del proceso
- `WARNING`: Advertencias
- `ERROR`: Errores

## 🛡️ Seguridad

- Las credenciales se almacenan en variables de entorno
- Archivo `.env` incluido en `.gitignore`
- Conexiones seguras a SharePoint y base de datos
- Validación de configuración antes de ejecución

## 🔄 Flujo del ETL

### 1. Extracción (Extract)
- Conexión a SharePoint usando Office365
- Listado de archivos Excel en carpetas específicas
- Descarga automática de archivos nuevos/modificados
- Almacenamiento temporal en `data/raw/`

### 2. Transformación (Transform)
- Lectura de archivos Excel (.xlsx, .xlsb, .xls)
- Limpieza de datos (filas/columnas vacías)
- Estandarización de nombres de columnas
- Conversión de tipos de datos
- Agregación de metadatos (fecha de extracción, archivo fuente)
- Transformaciones específicas por tipo de dato

### 3. Carga (Load)
- Validación de conexión a base de datos
- Carga de datos con control de errores
- Backup automático en archivos CSV/Excel
- Verificación de integridad de datos

## 🐛 Solución de Problemas

### Error de conexión a SharePoint
```
Error al conectar con SharePoint: Authentication failed
```
**Solución**: Verificar credenciales en `.env` y permisos de usuario

### Error de conexión a base de datos
```
Error de conexión a base de datos
```
**Solución**: 
1. Verificar cadena de conexión en `DATABASE_URL`
2. Instalar drivers necesarios (ODBC para SQL Server)
3. Verificar conectividad de red

### Archivos Excel no se pueden leer
```
Error al leer archivo: No engine
```
**Solución**: Verificar que están instaladas las librerías:
```bash
pip install openpyxl pyxlsb xlrd
```

### Memoria insuficiente
```
MemoryError
```
**Solución**: Configurar `CHUNK_SIZE` más pequeño en `.env`

## 📈 Optimización

### Para mejorar rendimiento:

1. **Chunk Size**: Ajustar `CHUNK_SIZE` en configuración
2. **Filtros**: Implementar filtros por fecha en extracción
3. **Paralelización**: Procesar archivos en paralelo
4. **Indexación**: Crear índices en base de datos

## 🤝 Contribución

Para contribuir al proyecto:

1. Fork del repositorio
2. Crear rama para nueva característica
3. Commit de cambios
4. Push a la rama
5. Crear Pull Request

## 📄 Licencia

Este proyecto está bajo la licencia MIT.

## 📞 Soporte

Para soporte técnico:
- Revisar logs en `logs/etl_process.log`
- Ejecutar `test_local.py` para diagnóstico
- Verificar configuración con `python setup.py`

## 🔮 Próximas Características

- [ ] Interfaz web para monitoreo
- [ ] Programación automática (scheduler)
- [ ] Notificaciones por email
- [ ] API REST para consultas
- [ ] Dashboard de métricas
- [ ] Versionado de datos
