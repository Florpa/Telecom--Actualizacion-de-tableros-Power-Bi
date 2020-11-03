'''Este script lo que hace es 
*   insertar en la tabla auxiliar datos.tablero_transporte_auxiliarauxiliar los resultados del procesamiento anterior
*   Actualizar para la semana actual la cantidad de dispositivos registrados
*   Actualizar los valores de los campos de relacionados a la precuarentena y a la semana anterior para compararlos luego
*   Hacer un insert final en la tabla que actualiza los valores del tablero Concentacion de persona 
La unica variable que hay que harcodear es la fecha de actualizacion, que correponde al viernes de la semana a a ctualizar y  el periodo
que comprende las fechas del lunes y viernes de la semana a actualizar'''

import psycopg2
import sys
import datetime


# Abre la coneccion y la request
connection = psycopg2.connect(user="postgres@godatos",
                              password="datoscimmit1234$",
                              host="godatos.postgres.database.azure.com",
                              port="5432",
                              database="telecom")

cur = connection.cursor()
print("conectado")

# FECHA DEL VIERNES EN LA QUE SE REALIZA LA ACTUALIZACION 
actualizacion = '30/10/2020'
#PERIODO DE LA SEMANA HABIL A ACTUALIZAR
periodo= '26/10 a 30/10'  

print(datetime.datetime.today())
#busco cual es la ultima semana que tengo registrada en mi tabla
cur.execute('SELECT  max (num_semana_actual)FROM datos.tablero_transporte_auxiliar')
resultado = cur.fetchall()

#VALOR DE LA ULTIMA SEMANA DISPONIBLE EN LA TABLA QUE ALIMENTA AL TABLERO
semana_anterior = (resultado[0][0])
#LE SUMA 1 A LA ULTIMA SEMANA PARA SABER CUAL ES ACTUAL A INSERTAR
semana_actual = (resultado[0][0])+1



#0001 primer insert
cur.execute("INSERT INTO datos.tablero_transporte_auxiliar(id_cuadricula, num_semana_actual,cant_lineas_actual) select fraccion, a.semana, round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana NOT IN (6,7,8)  and a.semana= " +
            str(semana_actual)+" and hora between '10:00' and '17:00' group by fraccion,a.semana")
connection.commit()
print(datetime.datetime.today())
print('Termine de insertar la ultima semana')


print('Voy a comenzar a actualizar los distintos campos de la tabla. Primero la semana de la PRECUARENTENA')
#002 updates semana precuarentena
cur.execute("update datos.tablero_transporte_auxiliar a set num_semana_precuarentena = 2 where num_semana_precuarentena is null and num_semana_actual = "+str(semana_actual))
connection.commit()
cur.execute("update  datos.tablero_transporte_auxiliar a set cant_lineas_precuarentena= b.n_lineas from (select fraccion, a.semana, round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana NOT IN (5,6,7,8)  and hora between '10:00' and '17:00' and  c.semana= 2 group by fraccion,a.semana)b where a.id_cuadricula=b.fraccion	 and num_semana_actual = "+str(semana_actual)+" and  a.cant_lineas_precuarentena is null")
connection.commit()
print(datetime.datetime.today())
print('Termine de actualizar los valores de la semana Precuarentena')

#003 Updates semana anterior
print('Comienzo a actualizar los valores correspondientes a la SEMANA ANTERIOR')
cur.execute("update datos.tablero_transporte_auxiliar a set num_semana_anterior = "+str(semana_anterior)+" where semana_anterior is null and num_semana_actual = "+str(semana_actual))
connection.commit()
cur.execute("update datos.tablero_transporte_auxiliar a set cant_lineas_semana_anterior = b.n_lineas from (select fraccion, a.semana, round (median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha)where c.dia_semana NOT IN (5,6,7,8) and a.semana="+str(semana_anterior)+" and hora between '10:00' and '17:00' group by fraccion,a.semana) b where  fraccion= id_cuadricula and a.num_semana_anterior= '"+str(semana_anterior)+"' and num_semana_actual = "+str(semana_actual))
connection.commit()
print(datetime.datetime.today())
print('Termine de actualizar los valores de la semana anterior')

#004 Update de poblacion
print('Comienzo a actualizar los valores de Poblacion, correspondiente a la semana')
cur.execute("update  datos.tablero_transporte_auxiliar a set poblacion_caba= b.n_lineas from (select fraccion, a.semana,round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana NOT IN (5,6,7,8) and a.semana= '"+str(semana_actual)+"' and hora ='4:00'group by fraccion,a.semana)b where a.poblacion_caba is null and a.id_cuadricula=b.fraccion and b.semana="+str(semana_actual))
connection.commit()
print(datetime.datetime.today())
print('Termine de actualizar los valores de la "poblacion" de esa semana')

#005 Update de fechas y valores en null
print('Actualizo la fecha y el periodo correspondiente a la semana')
cur.execute("update datos.tablero_transporte_auxiliar  a set actualizacion ='"+actualizacion+"',descripcion='"+periodo+"' where num_semana_actual="+str(semana_actual))
connection.commit()
print(datetime.datetime.today())
print('Termine de actualizar la fecha y el periodo correspondiente a la semana')

print('Actualizo los valores en null o en 0 ')
cur.execute("update datos.tablero_transporte_auxiliar a set cant_lineas_semana_anterior = '1', cant_lineas_actual = '1'where cant_lineas_semana_anterior is null or cant_lineas_actual is null ")
connection.commit()
print(datetime.datetime.today())
print('Termine de actualizar los valores en nulls')

#006 Insert Final a la Tbla que alimenta el tablero
print('Ultimo insert a la tabla que alimenta powerbi')
cur.execute ("insert into datos.tablero_transporte SELECT  id, id_cuadricula, geom, poblacion_caba,semana_pre_cuarentena, num_semana_precuarentena, cant_lineas_precuarentena,semana_anterior, num_semana_anterior, cant_lineas_semana_anterior, semana_actual,num_semana_actual, cant_lineas_actual,case when b.tipo is null then 'N' else b.tipo end tipo_area,case when b.tipo = 'Zona Trasbordo' then concat ('CT- ',b.nombre) when b.tipo != 'Zona Trasbordo' then initcap (b.nombre) when b.tipo is null then 'N'else nombre end area_interes, promedio, actualizacion, descripcion FROM datos.tablero_transporte_auxiliar a LEFT JOIN aux.area_interes_grilla b on b.id_grilla=a.id_cuadricula where num_semana_actual= "+str(semana_actual))
connection.commit()
print(datetime.datetime.today())
print('Termine con el proceso, puedo refrescar el tablero')


cur.close()
connection.close()

