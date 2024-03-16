# Usa la imagen base de Python
FROM python:3.10

# Establece directorio de trabajo 
WORKDIR /app

# Copia el script Python al contenedor
COPY requirements.txt .
COPY scripts/main.py .

# Instala las dependencias del script (si las hay)
RUN pip install --no-cache-dir -r requirements.txt

# Comando por defecto para ejecutar el script cuando el contenedor se inicie
CMD ["python", "main.py"]
