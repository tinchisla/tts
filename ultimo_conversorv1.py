import boto3
import hashlib
from bs4 import BeautifulSoup
import feedparser
import requests
import aiohttp
import asyncio
import urllib.parse
import mysql.connector
from datetime import datetime
import time
import re
from unidecode import unidecode

max_length = 3000

def limpiar_texto_polly(texto):
    # Convertir "%20" en espacios normales
    texto = urllib.parse.unquote(texto)

    # Lista de caracteres no deseados que serán reemplazados por guiones bajos
    caracteres_no_deseados = ['/', '\\', '?', '%', '*', ':', '|', '"', '<', '>', '.', '!', '@', '#', '$', '%', '^', '&', '*', '(', ')', "'", '"']

    # Reemplazar caracteres no deseados por guiones bajos
    for c in caracteres_no_deseados:
        texto = texto.replace(c, '_')

    # Reemplazar espacios por guiones bajos
    texto = texto.replace(' ', '_')

    # Eliminar las tildes de las vocales
    texto_sin_tildes = unidecode(texto)

    # Convertir todo el texto a minúsculas
    texto_en_minusculas = texto_sin_tildes.lower()

    # Eliminar cualquier carácter que no sea alfanumérico o guion bajo
    texto_limpio = re.sub(r'[^a-zA-Z0-9_]', '', texto_en_minusculas)

    # Eliminar dobles guiones bajos consecutivos
    texto_limpio = texto_limpio.replace('__', '_')

    return texto_limpio[:240]  # Limitar la longitud del nombre del archivo a 240 caracteres
    
# 1. Funciones de extracción de datos desde el RSS feed
async def obtener_contenido_articulo(articulo_link):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(articulo_link) as response:
                response.raise_for_status()
                html = await response.text()

        soup = BeautifulSoup(html, "html.parser")

        # Obtener el nombre del autor
        author_name_element = soup.find('span', class_='author-name')
        author_name = author_name_element.get_text() if author_name_element else "Nombre de autor no encontrado"

        # Obtener el título y copete del artículo si están disponibles
        title_element = soup.find('h1', class_='article-headline left')
        copete_element = soup.find('h2', class_='article-subheadline left')

        # Obtener los párrafos del cuerpo del artículo
        paragraphs = []
        for p in soup.find_all('p', class_='paragraph'):
            # Excluir las etiquetas <i> dentro del párrafo
            for tag in p.find_all('i'):
                tag.decompose()

            # Obtener el texto del párrafo después de excluir las etiquetas <i>
            paragraph_text = p.get_text()
            paragraphs.append(paragraph_text)

        title = title_element.get_text() if title_element else "Título no encontrado"
        copete = copete_element.get_text() if copete_element else "Copete no encontrado"

        return author_name, title, copete, paragraphs
    except aiohttp.ClientError as client_error:
        print(f"Error al obtener el contenido del artículo {articulo_link}: {client_error}")
    except Exception as e:
        print(f"Error inesperado al obtener el contenido del artículo {articulo_link}: {e}")

    return None, None, None, None
    
def insertar_pausa(texto, duracion):
    # Agregar una pausa con la duración especificada entre párrafos usando SSML
    pausa_ssml = f"<break time='{duracion}s'/>"
    return f"<speak>{texto} {pausa_ssml}</speak>"
    
def eliminar_caracteres_no_deseados(texto):
    # Expresión regular para eliminar los caracteres no deseados entre párrafos
    # Esto incluye barras "/", signos de menor "<", signos de mayor ">", y cualquier carácter no alfanumérico.
    return re.sub(r"[\n\/<>]+", " ", texto)
    
def eliminar_caracteres_ssml(texto):
    # Caracteres no válidos para SSML según la documentación de AWS Polly
    caracteres_prohibidos = ['<', '>', '&', '\'', '\"', '`']
    for char in caracteres_prohibidos:
        texto = texto.replace(char, "")


    return texto

def convertir_a_audio_por_fragmento(texto, voz="Penelope", idioma="es-US", max_length=3000):
    polly_client = boto3.client("polly", region_name="us-east-1")

    # Dividir el texto en sus componentes (autor, título, copete y párrafos)
    componentes = texto.split("\n\n")

    # Eliminar caracteres no válidos para SSML en cada componente
    componentes_limpio = [eliminar_caracteres_ssml(comp) for comp in componentes]

    # Convertir autor a audio
    autor_audio_data = convertir_texto_a_audio(polly_client, componentes_limpio[0], voz, idioma)

    # Convertir título a audio
    titulo_audio_data = convertir_texto_a_audio(polly_client, componentes_limpio[1], voz, idioma)

    # Convertir copete a audio si está presente
    copete_audio_data = b""
    if len(componentes_limpio) > 2:
        copete_audio_data = convertir_texto_a_audio(polly_client, componentes_limpio[2], voz, idioma)

    # Aplicar el filtro solo a los párrafos
    parrafos_filtrados = [eliminar_caracteres_no_deseados(p) for p in componentes_limpio[3:]]

    # Convertir párrafos a audio
    parrafos_audio_data = b""
    for parrafo in parrafos_filtrados:
        parrafo_audio = convertir_texto_a_audio(polly_client, parrafo, voz, idioma)
        if parrafo_audio:
            parrafos_audio_data += parrafo_audio

    # Combinar los audios de autor, título, copete y párrafos
    audio_data = b"".join([autor_audio_data, titulo_audio_data, copete_audio_data, parrafos_audio_data])

    return audio_data

    
def convertir_texto_a_audio(polly_client, texto, voz, idioma):
    # Eliminar caracteres especiales que pueden interferir con el formato SSML
    texto_limpio = eliminar_caracteres_ssml(texto)

    # Agregar una pausa con la duración especificada al final del texto
    texto_con_pausas = insertar_pausa(texto_limpio, 0.5)
    texto_fragmentado = [texto_con_pausas[i:i + max_length] for i in range(0, len(texto_con_pausas), max_length)]
    audio_data = b""

    for i, fragmento in enumerate(texto_fragmentado, 1):
        try:
            response = polly_client.synthesize_speech(
                Text=fragmento,
                VoiceId=voz,
                LanguageCode=idioma,
                OutputFormat="mp3",
                TextType="ssml"# Especificar que el texto es SSML
                #Engine="neural"
            )
            audio_data += response["AudioStream"].read()
            print(f"Fragmento {i}/{len(texto_fragmentado)} sintetizado correctamente.")
            # Agregar una pausa breve para evitar exceder los límites de solicitud de Polly
            time.sleep(0.5)
        except Exception as e:
            print(f"Error al sintetizar el fragmento {i}/{len(texto_fragmentado)}: {e}")
            return None

    return audio_data

def eliminar_caracteres_ssml(texto):
    # Caracteres no válidos para SSML según la documentación de AWS Polly
    caracteres_prohibidos = ['<', '>', '&', '\'', '\"', '`']
    for char in caracteres_prohibidos:
        texto = texto.replace(char, "")


    return texto

def guardar_audio_en_archivo(audio_data, nombre_archivo):
    with open(nombre_archivo, "wb") as f:
        f.write(audio_data)


def crear_carpeta_en_s3(s3_client, bucket, carpeta):
    # Verificar si la carpeta existe en S3
    try:
        s3_client.head_object(Bucket=bucket, Key=carpeta)
    except:
        # Si la carpeta no existe, crearla
        s3_client.put_object(Bucket=bucket, Key=carpeta)

def guardar_audio_en_s3(audio_data, bucket, nombre_archivo, prefijo_articulo):
    # Crear el cliente de S3
    s3_client = boto3.client("s3")

    # Crear la carpeta "infobae_test" si no existe
    crear_carpeta_en_s3(s3_client, bucket, "infobae_test")

    # Transformar el nombre del archivo
    nombre_archivo_limpio = limpiar_texto_polly(nombre_archivo)
    nombre_archivo_mp3 = f"infobae_test/{nombre_archivo_limpio}.mp3"

    # Guardar el archivo de audio en el bucket de S3 dentro de la carpeta "infobae_test"
    s3_client.put_object(Bucket=bucket, Key=nombre_archivo_mp3, Body=audio_data)

    return nombre_archivo_mp3
    
def verificar_existencia_archivo_en_s3(bucket, nombre_archivo):
    # Código para verificar si el archivo existe en el bucket de S3
    s3_client = boto3.client("s3")
    try:
        s3_client.head_object(Bucket=bucket, Key=nombre_archivo)
        return True
    except:
        return False


def conectar_base_de_datos():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="root",
            password="playerv1",
            database="audioombu_db"
        )
        return conn
    except mysql.connector.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None
        
def convertir_fecha(fecha_rss):
    # Define el formato del feed RSS y el formato deseado para la base de datos
    formato_rss = "%a, %d %b %Y %H:%M:%S %z"
    formato_bd = "%Y-%m-%d"

    # Convierte la fecha del formato del feed RSS al formato de la base de datos
    fecha_objeto = datetime.strptime(fecha_rss, formato_rss)
    fecha_convertida = fecha_objeto.strftime(formato_bd)
    return fecha_convertida

def insertar_articulo_en_bd(conn, titulo, link, contenido, fecha_publicacion):
    try:
        cursor = conn.cursor()
        insert_query = "INSERT INTO Articulos (titulo, link, contenido, fecha_publicacion) VALUES (%s, %s, %s, %s)"
        
        # Verifica si la fecha de publicación está presente en el feed RSS
        if fecha_publicacion:
            # Convierte la fecha de publicación al formato compatible con la base de datos (YYYY-MM-DD)
            fecha_publicacion_bd = convertir_fecha(fecha_publicacion)
        else:
            # Si la fecha de publicación no está presente, usa la fecha actual como valor predeterminado
            fecha_publicacion_bd = datetime.now().strftime("%Y-%m-%d")

        data = (titulo, link, contenido, fecha_publicacion_bd)
        cursor.execute(insert_query, data)
        conn.commit()
        return cursor.lastrowid
    except mysql.connector.Error as e:
        print(f"Error al insertar el artículo en la base de datos: {e}")
        return None
        
def insertar_audio_en_bd(conn, nombre_archivo, id_articulo, url_articulo):
    try:
        cursor = conn.cursor()
        insert_query = "INSERT INTO Audios (nombre_archivo, id_articulo, url_articulo) VALUES (%s, %s, %s)"
        data = (nombre_archivo, id_articulo, url_articulo)
        cursor.execute(insert_query, data)
        conn.commit()
        return cursor.lastrowid
    except mysql.connector.Error as e:
        print(f"Error al insertar el registro en la tabla 'Audios': {e}")
        return None

def buscar_articulo_por_link(conn, link):
    try:
        cursor = conn.cursor()
        select_query = "SELECT * FROM Articulos WHERE link = %s"
        cursor.execute(select_query, (link,))
        return cursor.fetchone()
    except mysql.connector.Error as e:
        print(f"Error al buscar el artículo en la base de datos: {e}")
        return None


# 3. Función principal
def main():
    url_rss_feed = "https://www.infobae.com/feeds/rss/"
    bucket_name = "testpruebaaudioinfobaeombudigital"
    
    # Conectar a la base de datos
    conn = conectar_base_de_datos()
    if not conn:
        print("Error: No se pudo conectar a la base de datos.")
        return

    # Obtener el contenido del RSS feed y extraer los enlaces a los artículos
    rss_content = feedparser.parse(url_rss_feed)
    if not rss_content.entries:
        print("Error: No se pudo obtener el contenido del RSS feed.")
        return

    print("Contenido del RSS feed:")

    # Extraer los artículos del feed
    articulos = []
    for entry in rss_content.entries:
        link = entry.link.strip()
        titulo = entry.title.strip()

        # Utilizamos la 'summary' del feed como contenido del artículo
        contenido = entry.summary.strip()

        if link and titulo and contenido:
            articulo = {
                "titulo": titulo,
                "link": link,
                "contenido": contenido,
            }
            articulos.append(articulo)

    print(f"Total de artículos encontrados en el RSS feed: {len(articulos)}")

    if not articulos:
        print("Error: No se encontraron artículos en el RSS feed.")
        return
   
   # Procesar cada artículo del RSS feed
    for i, articulo in enumerate(articulos, 1):
        titulo_articulo = articulo["titulo"]
        link_articulo = articulo["link"]
        contenido_articulo = articulo["contenido"]
        copete_articulo = articulo.get("copete", "")

        print(f"Procesando artículo {i}/{len(articulos)} - Título: {titulo_articulo}")

        # Verificar si el artículo ya ha sido procesado previamente (deduplicación)
        identificador_unico = hashlib.md5(contenido_articulo.encode()).hexdigest()
        nombre_archivo_mp3 = f"{identificador_unico}.mp3"

        # Agregar el prefijo del título del artículo al nombre del archivo
        prefijo_articulo = urllib.parse.quote(titulo_articulo, safe="")
        nombre_archivo_mp3_con_prefijo = f"{prefijo_articulo}"

        # Si el archivo no existe en el bucket, procesar el texto y guardarlo en S3
        if not verificar_existencia_archivo_en_s3(bucket_name, nombre_archivo_mp3_con_prefijo):
            print(f"Procesando contenido del artículo: {titulo_articulo}")

            # Obtener el contenido completo del artículo desde el enlace
                        # Obtener el contenido completo del artículo desde el enlace
            loop = asyncio.get_event_loop()
            autor_articulo, titulo, copete, parrafos_articulo = loop.run_until_complete(obtener_contenido_articulo(link_articulo))
            
            # Verificar si se pudo obtener el contenido del artículo
            if not parrafos_articulo:
                print(f"Error: No se pudo obtener el contenido completo del artículo: {titulo_articulo}")
                continue
            
            # Unir los componentes en una sola cadena de texto
            contenido_articulo_completo = "{}\n{}\n{}\n\n{}".format(autor_articulo, titulo, copete, '\n\n'.join(parrafos_articulo))
             # Convertir el contenido del artículo a audio
            audio_data = convertir_a_audio_por_fragmento(contenido_articulo_completo)

            if not audio_data:
                print(f"Error: No se pudo convertir el contenido del artículo a audio por fragmentos: {titulo_articulo}")
                continue

            # Guardar el archivo de audio en S3 con el prefijo del título del artículo
            guardar_audio_en_s3(audio_data, bucket_name, nombre_archivo_mp3_con_prefijo, prefijo_articulo)

            print(f"Audio del artículo {titulo_articulo} guardado en S3.")
    
            # Obtener la fecha de publicación del artículo desde el feed
            fecha_publicacion = articulo.get("published", "")
    
            # Insertar el artículo en la base de datos
            id_articulo = insertar_articulo_en_bd(conn, titulo_articulo, link_articulo, contenido_articulo, fecha_publicacion)
            if not id_articulo:
                print(f"Error: No se pudo insertar el artículo en la base de datos: {titulo_articulo}")
                continue

            # Insertar el registro en la tabla "Audios"
            insertar_audio_en_bd(conn, nombre_archivo_mp3, id_articulo, link_articulo)
    
            print(f"Artículo {titulo_articulo} insertado en la base de datos.")
    
        else:
            print(f"El artículo {titulo_articulo} ya ha sido procesado previamente.")

    conn.close()

if __name__ == "__main__":
    main()