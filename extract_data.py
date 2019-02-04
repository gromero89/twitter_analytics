import json
import time
import numpy as np
from blist import blist
import tweepy           # Para consumir la API de Tweeter
from collections import Counter 
import datetime
from credentials import * 

##Fichero para extraccion de datos

# Variables globales
APP_KEY = CONSUMER_KEY
APP_SECRET = CONSUMER_SECRET

DIR_PATH = "D:/Master_GR/TFM/Desarrolo_memoria/datos_followers/"

# Guardar datos en json ########################################################
def guardar_data(data, file_name):
    #crea/abre fichero
    jsonfile = open(DIR_PATH + file_name, "w")
    #guarda los datos en el fichero
    json.dump(data, jsonfile, default = myconverter)
    #cierra el fichero
    jsonfile.close()

# Cargar datos desde json ######################################################
def cargar_data(file_name):
    #abre fichero
    jsonfile = open(DIR_PATH + file_name, "r")
    #guarda los datos del fichero en data
    data = json.load(jsonfile)
    #cierra el fichero
    jsonfile.close()
    return data
    
# Extrae las variables deseadas de los datos del json ##########################
def shape_data(data, extract_keys):
    #Extrae unicamente los key especificos en extract_keys
    X = []
    
    for row in data:
        #únicamente extrae los datosque hay en extract_keys
        new_row = [value for key, value in row.items() if key in extract_keys]
        X.append(new_row)
    #transforma datos en array
    return np.array(X)

#convertir valor fecha
def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()
    
# Acceso a la api de twitter
def twitter_setup():
    """
    Función de utilidad para configurar la API de Twitter
    con nuestras claves de acceso.
    """

    # Autenticación y acceso usando claves:
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

    # Retornar API con autenticación:
    api = tweepy.API(auth)
    return api

#Carga los datos generales de las cuentas de las operadoras
#recibe como parametro cuenta de donde se extraen los datos y nombre del arhivo
def extract_data_operadora(operadoras , save_file):
    
    # Creamos un objeto extractor:
    extractor = twitter_setup()
    #operadoras = {'ClaroEcua','MovistarEC','CNT_EC'}

    data = blist([])
    date  = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for operadora in operadoras:
        
        user = extractor.get_user(operadora)
          
        #guardamos únicamente las variables deseadas
        user_data = {}
        user_data["id"] = user.id
        user_data["screen_name"] = user.screen_name #[hashtag["text"] for hashtag in status["entities"]["hashtags"]]
        user_data["name"] = user.name
        user_data["followers_count"] = user.followers_count
        user_data["friends_count"] = user.friends_count
        user_data["statuses_count"] = user.statuses_count
        user_data["created_at"] = user.created_at
        data.append(user_data)
          
        
    guardar_data(list(data), save_file)

#Carga tweets de las cuentas
def extract_status_operadora(screen_name,save_file):
    
    # Creamos un objeto extractor:
    extractor = twitter_setup()
    
    alltweets = [] 
    
    new_tweets = extractor.user_timeline(screen_name=screen_name, count=200) 
    alltweets.extend(new_tweets) 
    oldest = alltweets[-1].id - 1 
    while len(new_tweets) > 0: 
        new_tweets = extractor.user_timeline(screen_name=screen_name, count=200, max_id=oldest) 
        alltweets.extend(new_tweets) 
        oldest = alltweets[-1].id - 1
    
    data = blist([])
    
    for tweet in alltweets : 
        #tweets_operadora.insert_one(tweet._json) 
        #guardamos únicamente las variables deseadas
        tweet_dict = {}
        tweet_dict["id"] = tweet.id
        tweet_dict["text"] = tweet.text
        tweet_dict["created_at"] = tweet.created_at
        tweet_dict["coordinates"] = tweet.coordinates
        tweet_dict["in_reply_to_screen_name"] = tweet.in_reply_to_screen_name
        tweet_dict["retweet_count"]=tweet.retweet_count
        tweet_dict["favorite_count"]=tweet.favorite_count
        tweet_dict["screen_name"] = tweet.user.screen_name#[user.screen_name for user in tweet.user.screen_name]
        
        if 'media' in tweet.entities:
            tweet_dict["media"] = "Contenido multimedia"
        else:
            tweet_dict["media"] =  "No multimedia" 
       
            
             
        data.append(tweet_dict)
        
    guardar_data(list(data), save_file)
    
        
    

# Carga los ids de los friends de las cuentas
def extract_friends_ids(account_name, limit = 5000):
    
    #Carga friends_id
    
    #inicializa cursor
    cursor = -1
    extract_friends_ids = blist([])
    
    # Creamos un objeto extractor:
    extractor = twitter_setup()
    
    while cursor != 0:
        #método extractor
        query = extractor.get_friends_ids(screen_name = account_name, cursor = cursor, count = limit)
        
        extract_friends_ids.extend(query.ids)
        #actualizamos paginación
        cursor = query["next_cursor"]
        #pausa de 60 segundos por la api
        time.sleep(60)
    
    return list(extract_friends_ids)
    
# Carga los ids de los followers de las cuentas
def extract_followers_ids(account_name, limit = 5000):
   
    #inicializa cursor
    cursor = -1
    extract_followers_ids = blist([])
    
    # Creamos un objeto extractor:
    extractor = twitter_setup()
    
    while cursor != 0:
        #método extractor
        query = extractor.get_followers_ids(screen_name = account_name, cursor = cursor, count = limit)
        
        extract_followers_ids.extend(query.ids)
        #paginacion
        cursor = query["next_cursor"]
        #pause 60sg por la API
        time.sleep(60)
    
    return list(extract_followers_ids)

# guarda datos de perfil de cada seguidor
def extract_user_data(file_ids, save_file):
    """
    'save_file': archivo donde se guardan los datos
    'file_ids': ids de los usuarios
    """
    # Creamos un objeto extractor:
    extractor = twitter_setup()
    
    data = blist([])
    #carga los datos
    total_uids = load_data(file_ids)
    
    try:
        #datos guardados previamente
        prev_data = load_data(save_file)
    except:
        
        #continua guardando
        prev_data = []
        prev_data_ids = [user["id"] for user in prev_data]
        seguidor_ids = list(set(total_uids) - set(prev_data_ids))
        grupo_seguidor_ids = [seguidor_ids[n : n + 100] for n in range(0, len(seguidor_ids), 100)]
        
        #para cada grupo de ids
        for grupo_ids in grupo_seguidor_ids:
                #calcula los datos de usuario
                seguidor_obj =  extractor.lookup_user(user_id = grupo_ids, include_entities = False)
                #para cada usuario
                for user in seguidor_obj:
                    #únicamente guardamos las características deseadas
                    seguidor = {}
                    seguidor["id"] = user.id
                    seguidor["name"] = user.name
                    seguidor["screen_name"] = user.screen_name
                    seguidor["created_at"] = user.created_at
                    seguidor["description"] = user.description
                    seguidor["url"] = user.url
                    seguidor["favourites_count"] = user.favourites_count
                    seguidor["followers_count"] = user.followers_count
                    seguidor["friends_count"] = user.friends_count
                    seguidor["language"] = user.lang
                    seguidor["location"] = user.location
                    seguidor["protected"] = user.protected
                    seguidor["statuses_count"] = user.statuses_count
                    data.append(seguidor)
          
                time.sleep(15)
                #guardamos datos
                guardar_data(prev_data + list(data), save_file)
    return data

    
# Carga los topics de cada usuario #############################################
def extract_user_status(file_data, save_file):
    """
    Carga en 'save_file' los status (tweets y retweets) de los usuarios en 'file_data'.
    """
    # Creamos un objeto extractor:
    extractor = twitter_setup()
    data = blist([])
    #según los datos ya calculados sigue calculando
    total_data = load_data(file_data)
    try:
        prev_data = load_data(save_file)
    except:
        prev_data = []
        prev_data_ids = [user["id"] for user in prev_data]
        followers_obj = [user for user in total_data if user["id"] not in prev_data_ids]
        #para cada usuario
        for follower in followers_obj:
            follower["status"] = []
            #si todavía existe y no está protegido
            if not follower["protected"] and follower["statuses_count"] > 0:
                try:
                    #calcula con la api los últimos 200 tweets y retweets
                    user_timeline =  extractor.get_user_timeline(user_id = follower["id"], count = 200)
                    #para cada tweet/retweet
                    for status in user_timeline:
                        #guardamos únicamente las variables deseadas
                        user_status_dict = {}
                        user_status_dict["text"] = status.text
                        user_status_dict["hashtags"] = status.entities.hashtags.text
                        user_status_dict["created_at"] = status.created_at
                        user_status_dict["coordinates"] = status.coordinates
                    follower["status"].append(user_status_dict)
                except Exception as e:
                    if e.error_code == 401:
                        print("error: ", e.error_code) 
                        follower["status"].append("Error 401: protected user")
                    else:
                        print(str(e)) 
                        print(e.error_code) 
                    break
                data.append(follower)
                #pausa de 3 segundos por la api
                time.sleep(3)
                #guardamos datos calculados
                guardar_data(prev_data + list(data), save_file)
    return data

#devuelve la cantidad de followers en comun
def commonElement(ar1,ar2,ar3): 
     # first convert lists into dictionary 
     ar1 = Counter(ar1) 
     ar2 = Counter(ar2) 
     ar3 = Counter(ar3) 
     
     # perform intersection operation 
     resultDict = dict(ar1.items() & ar2.items() & ar3.items()) 
     common = [] 
      
     # iterate through resultant dictionary 
     # and collect common elements 
     for (key,val) in resultDict.items(): 
          for i in range(0,val): 
               common.append(key) 
  
     return common 

