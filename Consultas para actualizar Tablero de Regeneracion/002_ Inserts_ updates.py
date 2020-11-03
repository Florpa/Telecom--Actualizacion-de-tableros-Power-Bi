
import psycopg2
import sys
import datetime


# Abre la coneccion y la request
# Abre la coneccion y la request
connection = psycopg2.connect(user="postgres@godatos",
                              password="datoscimmit1234$",
                              host="godatos.postgres.database.azure.com",
                              port="5432",
                              database="telecom")

cur = connection.cursor()
print("conectado")

print(datetime.datetime.today())
#busco cual es la ultima semana que tengo registrada en mi tabla
cur.execute('SELECT  max (semana)FROM tablero_regeneracion_urbana.data_')
resultado = cur.fetchall()


#VALOR DE LA ULTIMa.semana DISPONIBLE EN LA TABLA QUE ALIMENTA AL TABLERO
semana_anterior = (resultado[0][0])
#LE SUMA 1 A LA ULTIMa.semana PARA SABER CUAL ES ACTUAL A INSERTAR
semana_actual = (resultado[0][0])+1

# FECHA DEL DOMINGO ANTERIOR A LA ACTUALIZACION 
actualizacion = '25/10/2020'
#PERIODO  DE LA SEMANA QUE SE ACTUALIZA, LUNE A DOMINGO
descripcion = '19/10 a 25/10'  

#001 PRIMER INSERT Y ACTUALIZACION DE VALORES PARA MEDIODIA SEMANA ACTUAL
print('VAMOS A ACTUALIZAR POR FRANJAS HORARIAS, PRIMERO TODO LOS VALORES REFERIDOS AL MEDIODIA')
print('INSERTO LOS REGISTROS PARA LA FRANJA DEL MEDIODIA. ACTUALIZO LOS VALORES PARA ESA SEMANA')
cur.execute("INSERT INTO tablero_regeneracion_urbana.data_ (id_cuadricula, semana,franja,  dispositivos_semana)select fraccion, a.semana, '1-Mediodia',round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha)where c.dia_semana NOT IN (5,6,7,8) AND a.semana = "+str(semana_actual)+" and hora between '11:00' and '15:00'   group by fraccion,a.semana;")
connection.commit()
print('TERMINE DE INSERTAR Y ACTUALIZAR LOS VALORES DEL MEDIODIA DE LA SEMANA ACTUAL')


#1A UPDATE MEDIODIA FIN DE SEMANA ACTUAL
print('ACTUALIZACION VALORES PARA EL MEDIODIA DEL FIN DE SEMANA ACTUAL')

cur.execute("UPDATE tablero_regeneracion_urbana.data_ a SET findsemana = b.semana, franja_finde= '1-Mediodia',dispositivos_finde= b.n_lineas from (select fraccion, a.semana,round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana IN (5,6,7,8) AND a.semana = "+str(semana_actual)+" and hora between '11:00' and '15:00'group by fraccion,a.semana)b where a.id_cuadricula= b.fraccion  and b.semana::text= a.semana::text and franja= '1-Mediodia'and dispositivos_finde is null")
connection.commit()
print('TERMINE DE ACTUALIZAR LOS VALORES DEL FIN DE SEMANA')


#1B UPDATES MEDIODIA SEMANA ANTERIOR
print('ASIGNO LOS VALORES CALCULADOS PRA EL MEDIODIA PARA LA SEMANA ANTERIOR')
cur.execute("UPDATE tablero_regeneracion_urbana.data_ a SET semana_anterior = b.semana,franja_semaant= '1-Mediodia',dispositivos_seman_ante= b.n_lineas from (select fraccion, a.semana, round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha)where c.dia_semana not IN (5,6,7,8) AND a.semana = "+str(semana_anterior)+" and hora between '11:00' and '15:00' group by fraccion,a.semana)b where a.id_cuadricula= b.fraccion and dispositivos_seman_ante is null and a.semana = "+str(semana_actual))
connection.commit()
print('TERMINE DE ACTUALIZAR LOS VALORES DEL MEDIODIA DE SEMANA ANTERIOR')


#1C UPDATES MEDIDIA FIN DE SEMANA ANTERIOR
print('ASIGNO LOS VALORES CALCULADOS PARA EL FIN DE SEMANA ANTERIOR')
cur.execute("UPDATE tablero_regeneracion_urbana.data_ a SET find_anterior = b.semana, franja_findeante= '1-Mediodia',dispositivos_findeanter= b.n_lineas from (select fraccion, a.semana, round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana IN (5,6,7,8) AND a.semana = "+str(semana_anterior)+"and  hora between '11:00' and '15:00' group by fraccion,a.semana)b where a.id_cuadricula= b.fraccion  and dispositivos_findeanter is null and a.semana = "+str(semana_actual))

connection.commit()
print(datetime.datetime.today())
print('TERMINE  DE ACTUALIZAR TODOS LOS VALORES PARA  LA FRANJA DEL MEDIODIA, COMENZARE CON LA FRANJA DE LA TARDE')

#------------------------------------------COMIENZA EL BLOQUE DE LA TARDE:D--------------------------------------------------

#2 PRIMER INSERT Y ACTUALIZACION DE VALORES PARA LA TARDE DE LA SEMANA ACTUAL
print('VAMOS A ACTUALIZAR POR FRANJAS HORARIAS, PRIMERO TODO LOS VALORES REFERIDOS A LA TARDE')

print('INSERTO LOS REGISTROS PARA LA FRANJA DEL TARDE. ACTUALIZO LOS VALORES PARA ESA SEMANA')
cur.execute("INSERT INTO tablero_regeneracion_urbana.data_ (id_cuadricula, semana,franja,  dispositivos_semana)select fraccion, a.semana, '2-Tarde',round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha)where c.dia_semana NOT IN (5,6,7,8) AND a.semana = "+str(semana_actual)+" and hora between '15:30' and '17:00'   group by fraccion,a.semana;")
connection.commit()
print('TERMINE DE INSERTAR Y ACTUALIZAR LOS VALORES DE LA TARDE DE LA SEMANA ACTUAL')
print(datetime.datetime.today())

#2A UPDATE TARDE FIN DE SEMANA ACTUAL
print('ACTUALIZACION VALORES PARA LA TARDE DEL FIN DE SEMANA ACTUAL')

cur.execute("UPDATE tablero_regeneracion_urbana.data_ a SET findsemana = b.semana, franja_finde= '2-Tarde',dispositivos_finde= b.n_lineas from (select fraccion, a.semana,round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana IN (5,6,7,8) AND a.semana = "+str(semana_actual)+" and hora between '15:30' and '17:00'group by fraccion,a.semana)b where a.id_cuadricula= b.fraccion  and b.semana::text= a.semana::text and franja= '2-Tarde'and dispositivos_finde is null")
connection.commit()
print('TERMINE DE ACTUALIZAR LOS VALORES DEL FIN DE SEMANA')
print(datetime.datetime.today())

#2B UPDATES TARDE SEMANA ANTERIOR
print('ASIGNO LOS VALORES CALCULADOS PARA EL TARDE DE LA SEMANA ANTERIOR')
cur.execute("UPDATE tablero_regeneracion_urbana.data_ a SET semana_anterior = b.semana,franja_semaant= '2-Tarde',dispositivos_seman_ante= b.n_lineas from (select fraccion, a.semana, round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha)where c.dia_semana not IN (5,6,7,8) AND a.semana = "+str(semana_anterior)+" and hora between '15:30' and '17:00' group by fraccion,a.semana)b where a.id_cuadricula= b.fraccion and dispositivos_seman_ante is null and a.semana = "+str(semana_actual))
connection.commit()
print('Termine de insertar y actualizar los valores de la franja del TARDE de la semana anterior')
print(datetime.datetime.today())

#2C UPDATES TARDE FIN DE SEMANA ANTERIOR
print('ASIGNO LOS VALORES CALCULADOS PARA EL TARDE DEL FIN DE SEMANA ANTERIOR')
cur.execute("UPDATE tablero_regeneracion_urbana.data_ a SET find_anterior = b.semana, franja_findeante= '2-Tarde',dispositivos_findeanter= b.n_lineas from (select fraccion, a.semana, round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana IN (5,6,7,8) AND a.semana = "+str(semana_anterior)+"and  hora between '15:30' and '17:00' group by fraccion,a.semana)b where a.id_cuadricula= b.fraccion  and dispositivos_findeanter is null and a.semana = "+str(semana_actual))
connection.commit()

print('TERMINE  DE ACTUALIZAR TODOS LOS VALORES PARA  LA FRANJA DEL MEDIODIA, COMENZARE CON LA FRANJA DE LA NOCHE')



#-----------------------------------------COMIENZA EL BLOQUE DE LA NOCHE :D--------------------------------------------------


#003 PRIMER INSERT Y ACTUALIZACION DE VALORES PARA LA NOCHE DE LA SEMANA ACTUAL
print('VAMOS A ACTUALIZAR POR FRANJAS HORARIAS, PRIMERO TODO LOS VALORES REFERIDOS A LA NOCHE')
print('INSERTO LOS REGISTROS PARA LA FRANJA DEL NOCHE. ACTUALIZO LOS VALORES PARA ESA SEMANA')
cur.execute("INSERT INTO tablero_regeneracion_urbana.data_ (id_cuadricula, semana,franja,  dispositivos_semana)select fraccion, a.semana, '3-Noche',round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha)where c.dia_semana NOT IN (5,6,7,8) AND a.semana = "+str(semana_actual)+" and hora between '17:30' and '23:00'   group by fraccion,a.semana;")
connection.commit()
print('TERMINE DE INSERTAR Y ACTUALIZAR LOS VALORES DE LA NOCHE DE LA SEMANA ACTUAL')
print(datetime.datetime.today())

#3A UPDATE NOCHE FIN DE SEMANA ACTUAL
print('ACTUALIZACION VALORES PARA LA NOCHE DEL FIN DE SEMANA ACTUAL')

cur.execute("UPDATE tablero_regeneracion_urbana.data_ a SET findsemana = b.semana, franja_finde= '3-Noche',dispositivos_finde= b.n_lineas from (select fraccion, a.semana,round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana IN (5,6,7,8) AND a.semana = "+str(semana_actual)+" and hora between '17:30' and '23:00'group by fraccion,a.semana)b where a.id_cuadricula= b.fraccion  and b.semana::text= a.semana::text and franja= '3-Noche'and dispositivos_finde is null")
connection.commit()
print('TERMINE DE ACTUALIZAR LOS VALORES PARA LA NOCHE DEL FIN DE SEMANA')
print(datetime.datetime.today())

#3B UPDATES NOCHE SEMANA ANTERIOR
print('ASIGNO LOS VALORES CALCULADOS PARA LA NOCHE DE LA SEMANA ANTERIOR')
cur.execute("UPDATE tablero_regeneracion_urbana.data_ a SET semana_anterior = b.semana,franja_semaant= '3-Noche',dispositivos_seman_ante= b.n_lineas from (select fraccion, a.semana, round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha)where c.dia_semana not IN (5,6,7,8) AND a.semana = "+str(semana_anterior)+" and hora between '17:30' and '23:00' group by fraccion,a.semana)b where a.id_cuadricula= b.fraccion and dispositivos_seman_ante is null and a.semana = "+str(semana_actual))
connection.commit()
print('Termine de insertar y actualizar los valores de la franja del NOCHE de la semana anterior')
print(datetime.datetime.today())

#3C UPDATES NOCHE FIN DE SEMANA ANTERIOR

print('ASIGNO LOS VALORES CALCULADOS PARA LA NOCHE DEL FIN DE SEMANA ANTERIOR')
cur.execute("UPDATE tablero_regeneracion_urbana.data_ a SET find_anterior = b.semana, franja_findeante= '3-Noche',dispositivos_findeanter= b.n_lineas from (select fraccion, a.semana, round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana IN (5,6,7,8) AND a.semana = "+str(semana_anterior)+"and  hora between '17:30' and '23:00' group by fraccion,a.semana)b where a.id_cuadricula= b.fraccion  and dispositivos_findeanter is null and a.semana = "+str(semana_actual))
connection.commit()
print ('TERMINE DE ACTUALIZAR LA CANTIDAD DE DISPOSITIVOS POR FRANJA HORARIA')



#004 ACTUALIZAR LA POBLCAION DE ESA SEMANA CON LOS VALORES ACTUALIZADOS
print('ACTUALIZO DE POBLACION DE LA SEMANA ACTUAL')
cur.execute("UPDATE tablero_regeneracion_urbana.data_ a SET poblacion = n_lineas from (select a.semana, fraccion,a.semana as sem,round(median(a.moviles::int))as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana not IN (5,6,7,8) and a.semana= "+str(semana_actual)+" and hora = '4:00' group by a.semana,fraccion) b where a.id_cuadricula= b.fraccion and poblacion is null and a.semana= "+str(semana_actual))
connection.commit()
print ('TERMINE DE ACTUALIZAR LA POBLACION DE ESA SEMANA')


#005 ACTUALIZAR LOS CAMPOS DE LA SEMANA PRECUARENTENA
print('ACTUALIZO LOS VALORES DE DISPOSITIVOS PARA LA SEMANA DE LA PRECUARENTENA')

#5A PRIMERO LA POBLACION DE LA SEMANA PRECUARENTENA
print('VOY A ACTUALIZAR EL CAMPO DE POBLACION CON LOS VALORES DE LA PRECUARENTENA')

cur.execute("update	tablero_regeneracion_urbana.data_ a set poblacion_pre= b.n_lineas from (select fraccion, a.semana, round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana  IN (5,6,7,8) and hora = '4:00' and a.semana=2 group by fraccion,a.semana)b where a.id_cuadricula= b.fraccion and poblacion_pre is null and a.semana= "+str(semana_actual))
connection.commit()
print ('TERMINE DE ACTUALIZAR LA POBLACION DE ESA SEMANA')

#5B LUEGO VOY A ACTUALIZAR POR FRANJA HORARIA LOS VALORES  DE LA CANTIDAD DE DISPOSITIVOS QUE HUBO EN ESA SEMANA

print('ACTUALIZO LOS VALORES DE LA FRANJA DEL MEDIODIA PARA LA SEMANA DE LA PRECUARENTENA')
cur.execute("update tablero_regeneracion_urbana.data_ a set semanprecu =b.semana, franja_pre= '1-Mediodia',dispositivo_semaprec= n_lineas from (select fraccion, a.semana,  round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana NOT IN (5,6,7,8) and hora between '11:00' and '15:00' and a.semana =2 group by fraccion,a.semana) b where a.id_cuadricula=b.fraccion and  a.franja= '1-Mediodia'and a.semana= "+str(semana_actual))
connection.commit()
print ('TERMINE DE ACTUALIZAR LA FRANJA DEL MEDIODIA PARA LA SEMANA DE LA PRECUARENTENA')

print('ACTUALIZO LOS VALORES DE LA FRANJA DEL MEDIODIA PARA EL FIN DE SEMANA DE LA PRECUARENTENA')
cur.execute("update tablero_regeneracion_urbana.data_ a set prefindsemana = b.semana,franja_prefinde= '1-Mediodia',dispositivo_findprec= b.n_lineas from (select fraccion, a.semana,round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana IN (5,6,7,8) and hora between '11:00' and '15:00'and a.semana=2 group by fraccion,a.semana)b where a.id_cuadricula=b.fraccion  and a.franja= '1-Mediodia'and a.semana= "+str(semana_actual))
connection.commit()
print ('TERMINE DE ACTUALIZAR LA FRANJA DEL MEDIODIA PARA EL FIN DESEMANA DE LA PRECUARENTENA')

#5C ACTUALIZO LA FRANJA DE LA TARDE PARA LA SEMANA PRECUARENTENA
print('ACTUALIZO LOS VALORES DE LA FRANJA DE LA TARDE PARA LA SEMANA DE LA PRECUARENTENA')
cur.execute("update tablero_regeneracion_urbana.data_ a set semanprecu =b.semana, franja_pre= '2-Tarde',dispositivo_semaprec= n_lineas from (select fraccion, a.semana,  round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana NOT IN (5,6,7,8) and hora between '15:30' and '17:00' and a.semana =2 group by fraccion,a.semana) b where a.id_cuadricula=b.fraccion and  a.franja= '2-Tarde' and a.semana= "+str(semana_actual))
connection.commit()
print ('TERMINE DE ACTUALIZAR LA FRANJA DE LA TARDE PARA LA SEMANA DE LA PRECUARENTENA')

print('ACTUALIZO LOS VALORES DE LA FRANJA DE LA TARDE PARA EL FIN DE SEMANA DE LA PRECUARENTENA')
cur.execute("update tablero_regeneracion_urbana.data_ a set prefindsemana = b.semana,franja_prefinde= '2-Tarde',dispositivo_findprec= b.n_lineas from (select fraccion, a.semana,round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana IN (5,6,7,8) and hora between '15:30' and '17:00'and a.semana=2 group by fraccion,a.semana)b where a.id_cuadricula=b.fraccion  and a.franja= '2-Tarde'and a.semana= "+str(semana_actual))
connection.commit()
print ('TERMINE DE ACTUALIZAR LA FRANJA DE LA TARDE PARA EL FIN DESEMANA DE LA PRECUARENTENA')

#5B ACTUALIZO LA FRANJA DE LA TARDE PARA LA SEMANA PRECUARENTENA

print('ACTUALIZO LOS VALORES DE LA FRANJA DE LA NOCHE PARA LA SEMANA DE LA PRECUARENTENA')
cur.execute("update tablero_regeneracion_urbana.data_ a set semanprecu =b.semana, franja_pre= '3-Noche',dispositivo_semaprec= n_lineas from (select fraccion, a.semana,  round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana NOT IN (5,6,7,8) and hora between '17:30' and '23:00' and a.semana =2 group by fraccion,a.semana) b where a.id_cuadricula=b.fraccion and  a.franja= '3-Noche' and a.semana= "+str(semana_actual))
connection.commit()
print ('TERMINE DE ACTUALIZAR LA FRANJA DE LA NOCHE PARA LA SEMANA DE LA PRECUARENTENA')

print('ACTUALIZO LOS VALORES DE LA FRANJA DE LA NOCHE PARA EL FIN DE SEMANA DE LA PRECUARENTENA')
cur.execute("update tablero_regeneracion_urbana.data_ a set prefindsemana = b.semana,franja_prefinde= '3-Noche',dispositivo_findprec= b.n_lineas from (select fraccion, a.semana,round(median (a.moviles::int)) as n_lineas from datos.dispositivos_por_grilla a left  join aux.calendario_2020 c using (fecha) where c.dia_semana IN (5,6,7,8) and hora between '17:30' and '23:00'and a.semana=2 group by fraccion,a.semana)b where a.id_cuadricula=b.fraccion  and a.franja= '3-Noche'and a.semana= "+str(semana_actual))
connection.commit()
print ('TERMINE DE ACTUALIZAR LA FRANJA DE LA NOCHE PARA EL FIN DESEMANA DE LA PRECUARENTENA')

#5C
#005 Update de fechas y valores en null
print('Actualizo la fecha y el periodo correspondiente a la semana')
cur.execute("update tablero_regeneracion_urbana.data_  a set actualizacion ='"+actualizacion+"',descripcion='"+descripcion+"' where semana="+str(semana_actual))
connection.commit()
print(datetime.datetime.today())
print('Termine de actualizar la fecha y el periodo correspondiente a la semana')



#6 INSERTO LOS RESULTADO EN LA TABLA QUE ALIMENTA EL TABLERO

print('INSERTO LOS DATOS EN LA TABLA QUE ALIMENTA EL TABLERO CON TODOS LOS DATOS ACTUALIZADOS')
cur.execute("insert into tablero_regeneracion_urbana.data select id_cuadricula, poblacion, semana, franja, dispositivos_semana, findsemana, franja_finde, dispositivos_finde, semana_anterior, franja_semaant, dispositivos_seman_ante, find_anterior, franja_findeante, dispositivos_findeanter,semanprecu, franja_pre, dispositivo_semaprec, prefindsemana,  franja_prefinde, dispositivo_findprec, poblacion_pre,case when b.nombre is not null then b.nombre when b.nombre is null then 'N' else 'other' end zona_interes,actualizacion, descripcion FROM tablero_regeneracion_urbana.data_ a LEFT JOIN regeneracion.zonas b on  b.id= a.id_cuadricula where semana="+str(semana_actual))
connection.commit()

print('YA PODES REFRESCAR EL TABLERO')


cur.close()
connection.close()

