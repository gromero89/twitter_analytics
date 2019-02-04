#Librerias
import extract_data
import datetime
import time
import filtrar_seguidores
from tabulate import tabulate
import influencers_followers

#Extrae y guarda datos generales de las cuentas de las operadoras
operadoras = {'ClaroEcua','MovistarEC','CNT_EC'}
operadoras_data = extract_data.extract_data_operadora(operadoras,"operadoras_data")

#Extrae y guarda tweets del timeline
#CNT
extract_data.extract_status_operadora("CNT_EC","cnt_status")
#Claro
extract_data.extract_status_operadora("ClaroEcua","claro_status")
#Movistar
extract_data.extract_status_operadora("MovistarEC","movistar_status")

#Cargamos datos followers, friends de las operadoras
#---CLARO-------------------------------------------------------------------
claro_followers_ids = extract_data.extract_followers_ids("@ClaroEcua")
extract_data.guarda_data(claro_followers_ids, "claro_users_ids")

claro_friends_ids = extract_data.extract_friends_ids("@ClaroEcua")
extract_data.guarda_data(claro_friends_ids, "claro_users_ids")

claro_data = extract_data.twitter_user_data("claro_users_ids", "claro_data")
extract_data.twitter_user_status("claro_data", "claro_status_data")
#---CLARO-------------------------------------------------------------------
#---MOVISTAR----------------------------------------------------------------
movistar_followers_ids = extract_data.extract_followers_ids("@MovistarEC")
extract_data.guarda_data(movistar_followers_ids, "movistar_users_ids")

movistar_friends_ids = extract_data.extract_friends_ids("@MovistarEC")
extract_data.guarda_data(movistar_friends_ids, "movistar_users_ids")

movistar_data = extract_data.twitter_user_data("movistar_users_ids", "movistar_data")
extract_data.twitter_user_status("movistar_data", "movistar_status_data")
#---MOVISTAR----------------------------------------------------------------
#---CNT---------------------------------------------------------------------
cnt_followers_ids = extract_data.extract_followers_ids("@CNT_EC")
extract_data.guarda_data(cnt_followers_ids, "cnt_users_ids")

cnt_friends_ids = extract_data.extract_friends_ids("@CNT_EC")
extract_data.guarda_data(cnt_friends_ids, "cnt_users_ids")

cnt_data = extract_data.twitter_user_data("cnt_users_ids", "cnt_data")
extract_data.twitter_user_status("cnt_data", "cnt_status_data")
#---CNT----------------------------------------------------------------------

#ANALISIS 1
#filtrar_seguidores usuarios no relevantes
start_time = time.time()
#cargamos datos operadoras
claro_ids = extract_data.guardar_data("claro_users_ids")
claro_data = extract_data.guardar_data("claro_data")

movistar_ids = extract_data.guardar_data("movistar_users_ids")
movistar_data = extract_data.guardar_data("movistar_data")

cnt_ids = extract_data.guardar_data("cnt_users_ids")
cnt_data = extract_data.guardar_data("cnt_data")

#filtrar_seguidores seguidores con poca o nula actividad
claro_followers = filtrar_seguidores.filtrar_seguidores(followers = claro_data, tweets_mensual = 4, seguidores_minimo = 10)
movistar_followers = filtrar_seguidores.filtrar_seguidores(followers = movistar_data,  tweets_mensual = 4, seguidores_minimo = 10)
cnt_followers = filtrar_seguidores.filtrar_seguidores(followers = cnt_data, tweets_mensual = 4, seguidores_minimo = 10)

claro_quality = len(claro_followers) / len(claro_data)
movistar_quality = len(movistar_followers) / len(movistar_data)
cnt_quality = len(cnt_followers) / len(cnt_data)

#tabla de estadísticas
claro_pr = filtrar_seguidores.alcance(claro_followers)
movistar_pr = filtrar_seguidores.alcance(movistar_followers)
cnt_pr = filtrar_seguidores.alcance(cnt_followers)

info_claro = ["Claro", len(claro_data), len(claro_followers), len(claro_data) - len(claro_followers), claro_quality * 100, claro_pr]
info_movistar = ["Movistar", len(movistar_data), len(movistar_followers),len(movistar_data) - len(movistar_followers), movistar_quality * 100,movistar_pr]
info_cnt = ["CNT", len(cnt_data), len(cnt_followers),len(cnt_data) - len(cnt_followers), cnt_quality * 100,cnt_pr]
headers = ["Operadora", "Seguidores", "Seguidores filt.", "Seguidores Inactivos","Quality (%)","Potencial reach"]
print(tabulate([info_claro, info_movistar, info_cnt], headers, "orgtbl"))

#ANALISIS 2
#extraemos influyentes, calculamos sus tweets y los guardamos
#los datos resultan de la ejecucion anterior (Analisis 1)
start_time = time.time()
#cargamos datos operadora
claro_data = extract_data.guardar_data("claro_data")
movistar_data = extract_data.guardar_data("movistar_data")
cnt_data = extract_data.guardar_data("cnt_data")

claro_influyentes = filtrar_seguidores.filtrar_seguidores(followers = claro_data, tweets_mensual = 20, seguidores_minimo = 1584, quality_filter = 10)
movistar_influyentes = filtrar_seguidores.filtrar_seguidores(followers = movistar_data, tweets_mensual = 20, seguidores_minimo = 1584, quality_filter = 10)
cnt_influyentes = filtrar_seguidores.filtrar_seguidores(followers = cnt_data, tweets_mensual = 20, seguidores_minimo = 1584, quality_filter = 10)

extract_data.save_data(claro_influyentes, "claro_influyentes")
extract_data.save_data(movistar_influyentes, "movistar_influyentes")
extract_data.save_data(cnt_influyentes, "cnt_influyentes")

#se extraen tweets de los seguidores influyentes
data_admin.twitter_user_status("claro_influyentes", "claro_influyentes_status")
data_admin.twitter_user_status("movistar_influyentes", "movistar_influyentes_status")
data_admin.twitter_user_status("cnt_influyentes", "cnt_influyentes_status")

#ANALISIS 3
#extraen estadisticas de los seguidores influyentes
start_time = time.time()
#cargamos datos operadoras
claro_influyentes = extract_data.guardar_data("claro_influyentes_status")
movistar_influyentes = extract_data.guardar_data("movistar_influyentes_status")
cnt_influyentes = extract_data.guardar_data("cnt_influyentes_status")

claro_stats, claro_timeline, claro_top10 = influencers_followers.celebrity_statistics(claro_influyentes, "@ClaroEcua")
movistar_stats, movistar_timeline, movistar_top10 = influencers_followers.celebrity_statistics(movistar_influyentes, "@MovistarEC")
cnt_stats, cnt_timeline, cnt_top10 = influencers_followers.celebrity_statistics(cnt_influyentes, "@CNT_EC")

#tabla de estadísticas
claro_influyentes_density = str(round(len(claro_influyentes) / len(claro_followers) * 100, 2))
movistar_influyentes_density = str(round(len(movistar_influyentes) / len(movistar_followers) * 100, 2))
cnt_influyentes_density = str(round(len(cnt_influyentes) / len(cnt_followers) * 100, 2))

aux = 200 * len(claro_influyentes)
claro_m = str(round(claro_stats[2] / aux * 100, 2))
aux = 200 * len(movistar_influyentes)
movistar_m = str(round(movistar_stats[2] / aux * 100, 2))
aux = 200 * len(cnt_influyentes)
cnt_m = str(round(cnt_stats[2] / aux * 100, 2))

info_claro = ["Claro", str(len(claro_influyentes)) + " (" + claro_influyentes_density + ")", str(claro_stats[0]), str(claro_stats[1]), str(claro_stats[2]) + " (" + claro_m + ")"]
info_movistar = ["Movistar", str(len(movistar_influyentes)) + " (" + movistar_influyentes_density + ")", str(movistar_stats[0]), str(movistar_stats[1]), str(movistar_stats[2]) + " (" + movistar_m + ")"]
info_cnt = ["Cnt", str(len(cnt_influyentes)) + " (" + cnt_influyentes_density + ")", str(cnt_stats[0]), str(cnt_stats[1]), str(cnt_stats[2]) + " (" + cnt_m + ")"]
headers = ["Operadora", "influyentes (%)", "Prom. activity", "Prom. followers", "Menciones (%)"]
print("SEGUIDORES INFLUYENTES")
print(tabulate([info_claro, info_movistar, info_cnt], headers, "orgtbl"))
print("* Menciones en los ultimos 200 tweets")
