"""
ANÁLISIS DE LOS SEGUIDORES POTENCIALES
Extrae información de los seguidores para analizar su contribución a la marca.
"""
# Librerías
from datetime import datetime

# Extrae estadísticas de los followers potenciales
def influencer_statistics(influencers, mention):
    #inicializamos variables
    actividad_cant, seguidores_cant, menciones_cant = 0, 0, 0
    statuses_counts = {}
    top10influencers = []
    #para cada influencer
    for influencer in influencers:
        user_count_mentions = 0
        #para cada tweet o retweet del influencer
        for status in influencer["status"]:
            if type(status) == dict and mention in status["text"]:
                user_count_mentions += 1
                month = status["created_at"][4:7]
                year = status["created_at"][-4:]
                key = " ".join([month, year])
                #contabilizamos según fecha del tweet
                if key in statuses_counts:
                    statuses_counts[key] += 1
                else:
                    statuses_counts[key] = 1
        
        top10influencers.append({"influencer": influencer["name"] + " (" + influencer["screen_name"] + ")", "mentions": user_count_mentions, "followers": influencer["followers_count"]})
        actividad_cant += influencer["statuses_count"]
        seguidores_cant += influencer["followers_count"]
        
    #menciones totales
    menciones_cant = sum(statuses_counts.values())
    total_influencers = len(influencers)
    #media del número de tweets de cada influencer
    av_activity = actividad_cant / total_influencers
    #media del número de followers de cada influencer
    av_follower = seguidores_cant / total_influencers
    statistics = [av_activity, av_follower, menciones_cant]
    #preparamos timeline
    statuses_counts = sorted(statuses_counts.items(), key = lambda w:datetime.strptime(w[0],"%b %Y"))
    timeline_dates = [t[0] for t in statuses_counts]
    timeline = [statuses_counts, timeline_dates]
    #ordenamos y sacamos 10
    top10influencers = sorted(top10influencers, key = lambda k: k["mentions"], reverse = True)[0:10]
    return (statistics, timeline, top10influencers)