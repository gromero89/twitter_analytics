import random as rand
import extract_data
from blist import blist
from datetime import date

# Filtra usuarios de acuerdo a los parametros de entrada
def filtrar_seguidores(followers, tweets_mensual, seguidores_minimo,filtro_rff=True, limit=False):
    """
    Extrae de 'followers' aquellos usuarios que no cumplan con los parámetros deseados:
    tweets_mensual, seguidores_minimo, filtro_rff
    """
    if type(followers) == str:
        followers = extract_data.load_data(followers)
        
    filtered_followers = blist([])
    
    if limit:
        followers = rand.sample(followers, limit)
        
    #para cada seguidor
    for follower in followers:
        
        #cargamos filtro actividad
        statuses_filter = (1 + date.today().year / int(follower["created_at"][-4:])) * 12 * tweets_mensual
        
        #filtramos por número de seguidores y por actividad
        #si seguidores_minimo=0: no se aplica el filtro de minimo de seguidores
        if seguidores_minimo  == 0: 
            if follower["statuses_count"] > statuses_filter:
                #filtramos por calidad según rate
                if filtro_rff:
                    friends = follower["friends_count"]
                    if follower["friends_count"] == 0:                    
                        friends = 1
                    rate = follower["followers_count"] / friends
                    #if rate >= filtro_rff:#debe se mayor que 1 para identificar potenciales
                    if rate > filtro_rff:#debe se mayor que 1 para identificar potenciales
                        filtered_followers.append(follower)
                else:

                    filtered_followers.append(follower)
            
        else:
            if follower["followers_count"] > seguidores_minimo and follower["statuses_count"] > statuses_filter:

                #filtramos por calidad según rate
                if filtro_rff:
                    friends = follower["friends_count"]
                    if follower["friends_count"] == 0:                    
                        friends = 1
                    rate = follower["followers_count"] / friends
                    #if rate >= filtro_rff:#debe se mayor que 1 para identificar potenciales
                    if rate > filtro_rff:#debe se mayor que 1 para identificar potenciales
                        filtered_followers.append(follower)
                else:

                    filtered_followers.append(follower)

    
    return list(filtered_followers)
    
    
def alcance(followers):
    """
    Calcula el alcance potencial de la cuenta a partir de los seguidores de los
    followers.
    """
    pot = 0
    for follower in followers:
        pot += follower["followers_count"]
    return pot + len(followers)
