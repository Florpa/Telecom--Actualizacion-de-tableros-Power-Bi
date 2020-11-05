/*Puede acontecer que la conecxion al Postgres este lenta y los updates e inserts tarden mas de lo debido
o incluso no se ejecuten. Esto no tieene que ver con un bugs en la orden o sintaxis, asi que te sugiero no cambies la estructura de las querys
Lo que si te recomiendo es que primero, conserves la calma, luego leas con atencion esto y si lo anterior no funciona que me escribas :D.*/

/*Consideraciones: La consulta 002_ no tarda mas de 3 min en ejecutarse. Si identificas que se "trabo" en un update te sugiero sigas estas recomendaciones*/


/*1)
La siguiente consulta permite identificar que campos estan completos y 
la cantidad de dispositivos que se actualizaron por cada franja horaria,
Te servira para identificar si el update que se esta ejecutando y demora por el caudal de registros
o si no esta actualizando nada */
SELECT DISTINCT (franja),semana,sum (poblacion)as poblacion,sum (poblacion_pre::int)pobl_pre,
		sum (dispositivos_semana)as conteo_semana, sum(dispositivos_finde) as conteo_finde,
		sum (dispositivos_seman_ante)as semana_anterior,sum (dispositivos_findeanter)as finde_anterior,
		sum (dispositivo_semaprec)prec,sum (dispositivos_finde) as dispositivos_finde, 
		SUM (dispositivo_findprec)AS dispositivo_findprec, count (*),semanprecu,prefindsemana
	from  tablero_regeneracion_urbana.data_auxiliar where semana in (34,35)
	group by  franja,semana,semanprecu, prefindsemana
	order by semana desc

/*2) Si identificas que el update no esta actualizando nada te recomiendo, primero que canceles el script,
luego que abras una ventana de consulta en postgres y ejecutes la siguiente consulta
*/

--Esta consulta te va a mostrar que procesos estan ejecutandose en la DDBB. Si ya cancelaste el update y 
--aun asi figura como en ejecucion, debes cancelarlo.
select query,pid 
from pg_stat_activity
where datname = 'telecom'

--Para cancelar consultas que estan en cola o en ejecucion, deben ingresar entre () el pid del proceso.
--Una vez que el proceso este cancelado, te devolvera true como respuesta.
SELECT pg_terminate_backend(39636);
/*3)Debes borrar los registros que se insertaron. Todo lo correspondiente a la semana que estabas actualizando debe ser borrado
para que puedas iniciar nuevamente la ejecucion. Caso contrario vas a tener mas registros de lo debido para esa semana,
y las medianas te van a dar numeros muy altos. NUNCA JAMAS EN LA VIDA DEJES DE PONERLE PARAMETROS A UN DELETE. SINO VAS A ESTAR EN PROBLEMAS*/

delete  from  tablero_regeneracion_urbana.data_auxiliar where semana = (numero de la semana que estabas insertando. este campo es un int)

delete  from  tablero_regeneracion_urbana.data_auxiliar where semana = 35

/*En la tabla data_auxiliar es donde debes operar siempre. Esto es asi porque a menos 
que el procedimiento se haya finalizado ok, no va a realizar ningun insert en la tabla que alimenta el tablero
De todas maneras si queres chequear que no se haya insertado ejecuta esta consulta*/

SELECT DISTINCT (franja),semana,sum (poblacion)as poblacion,sum (poblacion_pre::int)pobl_pre,
		sum (dispositivos_semana)as conteo_semana, sum(dispositivos_finde) as conteo_finde,
		sum (dispositivos_seman_ante)as semana_anterior,sum (dispositivos_findeanter)as finde_anterior,
		sum (dispositivo_semaprec)prec,sum (dispositivos_finde) as dispositivos_finde, 
		SUM (dispositivo_findprec)AS dispositivo_findprec, count (*),semanprecu,prefindsemana
	from  tablero_regeneracion_urbana.data where semana in (35)
	group by  franja,semana,semanprecu, prefindsemana
	order by semana desc



