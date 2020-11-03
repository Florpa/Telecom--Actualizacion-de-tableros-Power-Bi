-- Luego de haber corrido el Notebook que inserta los nuevos registros en la tabla datos.dispositivos_caba.
--es aconsejable  hacer unos chequeos antes de poner a procesar la informacion. Para evitar perdidas de tiempo

--Estas consulta deben ejecutarse en Postgres
-- Chequeo el valor de la mediana para los dias. Y cuento la cantidad de horas que se registraron
select date,count (distinct (hora))as horas, 
round(median (n_lineas))as n_lineas 
from 
	(select distinct (date),hora, sum (n_lineas)*3.3 as n_lineas
	from public.dispositivos_caba
	group by date,hora
	order by 1)c
group by date
order by date

--Luego de ejecutar las actualizaciones que se generan con el script
-- ejecuto esta consulta para chequear que se hayan efectuado los cambios

	SELECT  count(id_cuadricula)as cantidad_de_registros,num_semana_actual,
			sum (poblacion_caba) as poblacion, 
			sum (cant_lineas_semana_anterior) valores_semana_anterior,
			sum(cant_lineas_actual)valores_semana_actual
	FROM datos.tablero_transporte
	group by num_semana_actual
	order by num_semana_actual desc
				 	