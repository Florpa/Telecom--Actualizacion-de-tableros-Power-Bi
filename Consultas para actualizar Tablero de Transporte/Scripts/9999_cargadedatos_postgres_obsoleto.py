# Txt to postgres

# Importa las librerias
import datetime
import psycopg2
import os

# Abre la coneccion al postgres
connection = psycopg2.connect(user="postgres@godatos",
                              password="datoscimmit1234$",
                              host="godatos.postgres.database.azure.com",
                              port="5432",
                              database="telecom")
cur = connection.cursor()

# Cambia el directorio a la carpeta que contiene el input TRANSITO.TXT
os.chdir('F:/repos_flor/Actualizacion-Tableros/querys_tablero_telecom/query_tablero_telecom_new/import_to_postgres')

# Crea la tabla historica con todos los campos que figuran en el txt, sumando geometria y pk (primary key with serial)
cur.execute("create table if not exists datos.historico_telecom (dia text,time text,uli_sitiorm_latitud text,uli_sitiorm_longitud text,lineas text,cdl_provincia text,cdl_localidad text,sitiorm_agrupador_zona_de_servic text,BARRIO text,hora text,DAY text, ID int,geom geometry (point,4326),pk serial PRIMARY KEY)")

# Consulta a la bd cual fue la ultima fecha, en el caso que la bd este vacia, asigna al valor fecha 1999-9-09 solo para asignar un valor.
cur.execute("select max(day::date)::text from datos.historico_telecom")
result = cur.fetchall()
fecha = str(result[0][0])
if fecha == 'None':
    fecha = '1999-9-09'
fecha = datetime.datetime.strptime(str(fecha), '%Y-%m-%d')


# Carga en una variable el insert objetivo
postgres_insert_query = """INSERT INTO datos.historico_telecom (dia, "time", uli_sitiorm_latitud, uli_sitiorm_longitud, lineas, cdl_provincia, cdl_localidad, sitiorm_agrupador_zona_de_servic, barrio, hora, day, id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

# Abre el archivo Trafico y compara las fechas en el archivo con las que se encuentran en el servidor. Si la fecha es mayor realiza el insert
print('Importando')
with open('TRAFICO.txt') as input:
    next(input)
    for lineas in input:
        try:
            linea = lineas.replace('\n', '').split(';')
            dia = linea[10]
            dia = datetime.datetime.strptime(dia, '%d%b%Y')
            if dia > fecha:  # Compara la fecha nueva con la fecha en el servidor
                record_to_insert = linea
                cur.execute(postgres_insert_query, record_to_insert)
                connection.commit()
        except:
            print('error en la importacion')

print('Importacion finalizada')
# Actualiza las geometrias en la tabla para los valores nulos

cur.execute("update datos.historico_telecom set geom = st_setsrid(st_makepoint(uli_sitiorm_longitud::double precision,uli_sitiorm_latitud::double precision),4326) where geom is null;")
connection.commit()

# Cierra las conexion.
cur.close()
connection.close()
