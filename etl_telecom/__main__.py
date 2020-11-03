# -*- coding: UTF-8 -*-

'''

etl_telecom

Tomo datos de una tabla del postgres y muevo cosas hasta tener actualizada la tabla que actualiza el tablero.

'''

import psycopg2
# import sys
import datetime
from .config import *
from .utils import get_max_date, increment_staged_data, get_distinct_new_dates, increment_antenas

RETURN_EXITOSO = True

def main():

    print("--- Inicio del script ---")

    print("pg user:", godatos_pg_user)
    print("pg pw:", godatos_pg_password)
    print("pg host:", godatos_pg_host)
    print("pg port:", godatos_pg_port)
    print("pg db name:", godatos_pg_db_name)

    print("--- Inicio del procesamiento ---")

    # Abre la coneccion y la request
    con = psycopg2.connect(user = godatos_pg_user,
                            password = godatos_pg_password,
                            host = godatos_pg_host,
                            port = godatos_pg_port,
                            database = godatos_pg_db_name)

    cur = con.cursor()

    print("Conectado.")

    print(datetime.datetime.today())

    source_table = "raw_data.sample_table"
    print("source table:", source_table)
    
    sink_staged_table = "staged_data.sample_table"
    print("sink table:", sink_staged_table)

    # check_holes_in_new_data("raw_data.sample_table", cur)
    # acá puede ir función que toma tabla, hace fecha max, min, serie y valida que haya todos los días intermedios
    # podría hacerse lo mismo a nivel hora, que no haya horas faltantes en cada día
    # print("Resultado de chequeo de fechas: ", "tenemos un 3312")

    max_date = get_max_date(sink_staged_table, cur)
    # si tabla vacía devuelve una lista con un elemento [(None,)]

    increment_staged_data(source_table,
                            max_date,
                            sink_staged_table,
                            cur)
    
    print("Staged-data incrementada OK.")

    distinct_new_dates = get_distinct_new_dates(
        max_date, 
        sink_staged_table,
        cur
    )

    print(distinct_new_dates)

    # incremento antenas, defino tabla de destino
    sink_antenas_table = "staged_data.antenas_sample_table"
    print(sink_antenas_table)

    increment_grilla_antenas(
        sink_staged_table,
        sink_antenas_table,
        distinct_new_dates,
        cur
    )
    # 

    print("Data de antenas incrementada OK.")

    """
    for fecha in fechas:

        fecha = (str(fecha[0]))
        
        for hora in horas:

            hora = (str(hora[0]))
            
            cur.execute("with antenas as (select st_transform(geom,5347) as geom, day, sum(cant_lineas)*3.3 AS lineas,hora from telecom.dispositivos_0711 a where  hora::time = '" + hora + "' and day = '"+fecha+"' group by  geom, day,hora order by hora),calendario as( select pk_fecha,fecha,semana,dia_semana,dia,mes,dia_semana_nombre,feriado from telecom.calendario_2020),voro as (select x.geom, b.lineas as moviles, day, hora from (SELECT (ST_DUMP(ST_VoronoiPolygons(ST_Collect(geom)))).geom as geom from antenas)x inner join antenas b on st_within (b.geom, x.geom) ),caba as ( Select st_transform(st_union(geom),5347) as geom from flor.radios_caba),vorointer as (select st_intersection(b.geom,a.geom) as geom, moviles, day, hora,st_area(st_intersection(b.geom,a.geom)) as tarea  from voro a inner join caba b on st_intersects(a.geom, b.geom)), fraccion as (select st_transform(geom,5347) as geom,id as fraccion from  general.cuadrado_150),vorofrac as (select ST_Intersection(a.geom, b.geom) as geom, a.fraccion, moviles, tarea,day, hora, st_area(ST_Intersection(a.geom, b.geom)) as  farea from fraccion a inner join vorointer  b on st_intersects(a.geom, b.geom)) ,combi as ( select  GEOM, FRACCION, MOVILES, tarea, ((farea *100)/tarea) as porarea, ((farea*moviles)/tarea) as fantena,day, hora  from vorofrac),presalida as (select st_union(geom) as geom, fraccion, round(sum(fantena)) as moviles, day, hora from combi group by fraccion,day, hora),salida as (select st_transform(geom,4326) as geom,fraccion,moviles,moviles/(st_area(geom)/1000000) as densidad,day,hora from presalida),insertar as (insert into telecom.proyecto (fraccion, fecha, hora, moviles, semana)select fraccion,b.fecha,hora,moviles,b.semana from salida a join calendario b on a.day=b.fecha)select count(*) from telecom.proyecto")
            
            con.commit()
        
        print('Finalizado:', fecha)
    """
    
    # debo truncar tabla inicial al terminar el script? al inicio?
    # qué sucede al día siguiente?

    con.commit()
    
    cur.close()

    con.close()

    return RETURN_EXITOSO

if __name__ == '__main__':
    main()


# en algún lugar debo crear índices
# indexar por fecha, porque uso mucho el campo día
# indexar espacialmente para acelerar operaciones