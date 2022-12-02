#The Restaurantour - Data Wrangling

#Step 1. Use the Yelp API to collect LA restaurant info 
#Step 2. Use the Foursquare API to collect additional info for each restaurant 
#Step 3. Localize the restaurants into neighborhoods 
#Step 4. Use the Foursquare API to collect information about other places in each neighborhood

#load packages
import requests
import numpy as np
import pandas as pd
from copy import deepcopy
import pickle
from shapely.geometry import Point, Polygon, MultiPolygon
from collections import defaultdict

#load map data:
with open('df_map.pkl','rb') as f:
    df_map = pickle.load(f)

#define functions
def get_yelp_rests(URL,headers,location,offset,cat):
    params = {
        'term': 'restaurants',
        'location': location, #'Los Angeles'
        'limit': 50,
        'offset': offset,
        'sort_by': 'rating',
        'categories': cat
    }
    return requests.get(URL, params=params, headers=headers)

def get_yelp_details(rest_url,headers,cuisine,rest_name):
    response = requests.get(rest_url, headers=headers)
    if response.status_code != 200:
        print(cuisine, rest_name, 'bad status')
        return []
    else:
        return response.json()

def get_yelp_reviews(reviews_url,headers,cuisine,rest_name):
    response = requests.get(reviews_url, headers=headers)
    if response.status_code != 200:
        print(cuisine, rest_name, 'bad status')
        return []
    else:
        return response.json()
    
def get_fsq_id(fs_url,fs_headers,cuisine,rest_name,lat,long):
    params = {
        "name": rest_name,
        "ll":  str(lat) + ',' +  str(long)
    }
    response = requests.get(fs_url, params=params, headers=fs_headers)
    if response.status_code != 200:
        print(cuisine, rest_name, 'bad status')
        return []
    else:
        return response.json()
    
def get_fsq_fields(fs_url,fsq_id,fs_headers,fields,cuisine,rest_name):
    params = {
        'fields': fields
    }
    response = requests.get(fs_url + fsq_id, params=params, headers=fs_headers)
    if response.status_code != 200:
        print(cuisine, rest_name, 'bad status')
        return []
    else:
        return response.json()
    
def add_rest_to_df(rest,df_rests):
    df_rests.loc[loc,'yelp_id'] = rest['id']
    if 'fsq_id' in rest:
        df_rests.loc[loc,'fsq_id'] = rest['fsq_id']
    df_rests.loc[loc,'yelp_name'] = rest['name']
    cats = []
    for c in rest['categories']:
        cats.append(c['title'])
    df_rests.loc[loc,'yelp_categories'] = cats
    df_rests.loc[loc,'neighborhood'] = rest['neighborhood']
    df_rests.loc[loc,'latitude'] = rest['coordinates']['latitude']
    df_rests.loc[loc,'longitude'] = rest['coordinates']['longitude']
    df_rests.loc[loc,'address1'] = rest['location']['address1']
    df_rests.loc[loc,'address2'] = rest['location']['address2']
    df_rests.loc[loc,'city'] = rest['location']['city']
    df_rests.loc[loc,'state'] = rest['location']['state']
    df_rests.loc[loc,'zip_code'] = rest['location']['zip_code']
    df_rests.loc[loc,'display_address'] = rest['location']['display_address']
    df_rests.loc[loc,'display_phone'] = rest['display_phone']
    if 'hours' in rest:
        df_rests.loc[loc,'hours_type'] = rest['hours'][0]['hours_type']
        df_rests.loc[loc,'hours_open'] = rest['hours'][0]['open']
    if 'is_claimed' in rest:
        df_rests.loc[loc,'yelp_is_claimed'] = rest['is_claimed']
    if 'price' in rest:
        df_rests.loc[loc,'yelp_price'] = rest['price']
    df_rests.loc[loc,'yelp_rating'] = rest['rating']
    df_rests.loc[loc,'yelp_review_count'] = rest['review_count']

    if 'four square data' in rest:
        if rest['four square data']:
            fsrest = rest['four square data'] #used below for readability
            df_rests.loc[loc,'fsq_name'] = fsrest['name']
            cats = []
            for c in fsrest['categories']:
                cats.append(c['name'])
            df_rests.loc[loc,'fsq_categories'] = cats
            df_rests.loc[loc,'census_block'] = fsrest['location']['census_block']
            df_rests.loc[loc,'fsq_address'] = fsrest['location']['formatted_address']

            if 'hours_popular' in fsrest:
                df_rests.loc[loc,'fsq_hours_popular'] = fsrest['hours_popular']
            if 'display' in fsrest['hours']:
                df_rests.loc[loc,'fsq_hours_display'] = fsrest['hours']['display']
            if fsrest['chains']:
                df_rests.loc[loc,'is_chain'] = 1
            else:
                df_rests.loc[loc,'is_chain'] = 0
            df_rests.loc[loc,'fsq_verified'] = fsrest['verified']
            if 'popularity' in fsrest:
                df_rests.loc[loc,'fsq_popularity'] = fsrest['popularity']
            if 'price' in fsrest:
                df_rests.loc[loc,'fsq_price'] = fsrest['price']
            if 'rating' in fsrest:
                df_rests.loc[loc,'fsq_rating'] = fsrest['rating']
            if 'stats' in fsrest:
                if 'total_photos' in fsrest['stats']:
                    df_rests.loc[loc,'fsq_total_photos'] = fsrest['stats']['total_photos']
                if 'total_ratings' in fsrest['stats']:
                    df_rests.loc[loc,'fsq_total_ratings'] = fsrest['stats']['total_ratings']
                if 'total_tips' in fsrest['stats']:
                    df_rests.loc[loc,'fsq_total_tips'] = fsrest['stats']['total_tips']
            if 'tastes' in fsrest:
                df_rests.loc[loc,'fsq_tastes'] = fsrest['tastes']
            if 'features' in fsrest:
                df_rests.loc[loc,'fsq_features'] = [fsrest['features']]
            if 'website' in fsrest:
                df_rests.loc[loc,'website'] = fsrest['website']
    return df_rests

#Step 1. Use the Yelp API to collect LA restaurant info
URL = 'https://api.yelp.com/v3/businesses/search'
api_key = 'private' #replace with real api key
headers = {'Authorization': f'Bearer {api_key}'}

#API can return top 1000 results, in sets of 50:
offsets = np.arange(0,1000,50)

#Yelp restaurant categories
categories = [
    'afghani',
    'african',
    'newamerican',
    'tradamerican',
    'arabian',
    'argentine',
    'armenian',
    'asianfusion',
    'australian',
    'austrian',
    'bangladeshi',
    'bbq',
    'basque',
    'belgian',
    'brasseries',
    'brazilian',
    'british',
    'buffets',
    'bulgarian',
    'burgers',
    'burmese',
    'cafes',
    'cajun',
    'cambodian',
    'caribbean',
    'catalan',
    'chinese',
    'comfortfood',
    'creperies',
    'cuban',
    'delis',
    'diners',
    'dinnertheater',
    'eritrean',
    'ethiopian',
    'filipino',
    'fishnchips',
    'fondue',
    'french',
    'gamemeat',
    'gastropubs',
    'georgian',
    'german',
    'gluten_free',
    'greek',
    'guamanian',
    'halal',
    'hawaiian',
    'himalayan',
    'honduran',
    'hkcafe',
    'hotpot',
    'hungarian',
    'iberian',
    'indpak',
    'indonesian',
    'irish',
    'italian',
    'japanese',
    'kebab',
    'korean',
    'kosher',
    'laotian',
    'latin',
    'raw_food',
    'malaysian',
    'mediterranean',
    'mexican',
    'mideastern',
    'modern_european',
    'mongolian',
    'moroccan',
    'newmexican',
    'nicaraguan',
    'noodles',
    'pakistani',
    'panasian',
    'persian',
    'peruvian',
    'pizza',
    'polish',
    'polynesian',
    'popuprestaurants',
    'portuguese',
    'poutineries',
    'russian',
    'salad',
    'sandwiches',
    'scandinavian',
    'seafood',
    'singaporean',
    'slovakian',
    'somali',
    'soulfood',
    'soup',
    'southern',
    'spanish',
    'srilankan',
    'steak',
    'supperclubs',
    'sushi',
    'syrian',
    'taiwanese',
    'tapasmallplates',
    'tex-mex',
    'thai',
    'turkish',
    'ukrainian',
    'uzbek',
    'vegan',
    'vegetarian',
    'vietnamese',
    'wraps'
    ]

#gather restaurants
city = 'Los Angeles'
rest_dict = {}
for cat in categories:
    cat_list = [] #to store the responses for each category
    for offset in offsets:
        response = get_yelp_rests(URL,headers,city,offset,cat)
        if response.json()['businesses']:
            cat_list.append(response)
        else:
            break
    rest_dict[cat] = cat_list
    
# remove extra fields and restaurants that are permanently closed
remove_keys = [
    'transactions'
    ]

cleaned_dict = {}
for cat in categories:
    cat_resps = rest_dict[cat]
    #create a new list of dicts for this category
    cleaned_dict[cat] = []
    for resp in cat_resps:
        rests = resp.json()['businesses']
        for rest in rests:
            # first, check to make sure it is still open:
            if not rest['is_closed']:
                    for k in remove_keys: #remove unneeded keys
                        rest.pop(k, None)
                    cleaned_dict[cat].append(rest)
                    
# add additional details for these restaurants from Yelp API
added_keys = [
    'is_claimed',
    'photos',
    'hours',
    'special_hours'
    ]

details_url = 'https://api.yelp.com/v3/businesses/'
for cuisine in cleaned_dict:
    for rest in cleaned_dict[cuisine]:
        if 'is_claimed' not in rest:
            resp_json = get_yelp_details(details_url + rest['id'],headers,cuisine,rest['name'])
            if resp_json:
                for key in added_keys:
                    if key in resp_json:
                        rest[key] = resp_json[key]
                        
# also add the 3 review samples from Yelp API
for cuisine in cleaned_dict:
    for rest in cleaned_dict[cuisine]:
        if 'reviews' not in rest:
            reviews_url = details_url + rest['id'] + '/reviews'
            resp_json = get_yelp_reviews(reviews_url,headers,cuisine,rest['name'])
            if 'reviews' in resp_json:
                rest['reviews'] = resp_json['reviews']
                
#Step 2. Use the Foursquare API to collect additional info for each restaurant 

fs_url = 'https://api.foursquare.com/v3/places/match'
fs_api_key = 'private' #replace with real api key
fs_headers = {
        "Accept": "application/json",
        "Authorization": fs_api_key
    }

# add Foursquare id to match Yelp info
for cuisine in cleaned_dict:
    for rest in cleaned_dict[cuisine]:
        if 'fsq_id' not in rest:
            resp_json = get_fsq_id(fs_url,fs_headers,cuisine,rest['name'],rest['coordinates']['latitude'],rest['coordinates']['longitude'])
            if 'fsq_id' in resp_json:
                rest['fsq_id'] = resp_json['fsq_id']
            elif 'fsq_id' in resp_json['place']:
                rest['fsq_id'] = resp_json['place']['fsq_id']
                
# add Foursquare field data
fs_url = 'https://api.foursquare.com/v3/places/'
fields = ('name,location,categories,chains,timezone,link,description,website,social_media,' +
          'verified,hours,hours_popular,rating,stats,popularity,price,menu,date_closed,photos,' +
          'tips,tastes,features')

for cuisine in cleaned_dict:
    for rest in cleaned_dict[cuisine]:
        if 'four square data' not in rest:
            if 'fsq_id' in rest:
                resp_json = get_fsq_fields(fs_url,rest['fsq_id'],fs_headers,fields,cuisine,rest['name'])
                if 'name' in resp_json:
                    rest['four square data'] = resp_json
                    
# identify mismatched restaurants between Yelp vs. Foursquare info
# also flag for removal restaurants without an address
#create copy so do not overwrite in case there is an issue
new_cleaned_dict = deepcopy(cleaned_dict)

for cuisine in new_cleaned_dict:
    for rest in new_cleaned_dict[cuisine]:
        if type(rest['location']['address1']) == None.__class__: #likely a food truck
            #add a tag to ignore this entry
            rest['remove'] = 1
        elif rest['location']['address1'] == '': #another way it can be empty
            rest['remove'] = 1
        else:
            rest['remove'] = 0
            if 'four square data' in rest:
                if rest['name'] != rest['four square data']['name']:
                    if 'location' in rest['four square data']:
                        if 'address' in rest['four square data']['location']:
                            #split the address to compare just the number:
                            yelp_address = rest['location']['address1'].split()
                            fs_address = rest['four square data']['location']['address'].split()
                            if yelp_address[0] != fs_address[0]:
                                rest['four square data'] = {} #remove
                                rest['fsq_id'] = 'mismatch'
                                
#Step 3. Localize the restaurants into neighborhoods 
#use the polygon boundary data from USC, found in df_map

num_rests_neighborhood = defaultdict(int)
poly_list = list(df_map['polygon'])

# locations found during Yelp search that are not within LA county
outside_cities = ['Lake Forest','Stanton','Anaheim','Westminster','Newport Beach',
                  'Costa Mesa','Chino','San Clemente','Laguna Beach','Chino Hills',
                  'Garden Grove','Orange','Huntington Beach','Upland','Fullerton',
                  'Daly City','Citrus Heights','Gilroy','Santa Rosa','London',
                  'Thousand Oaks','San Diego']

# boundaries are not perfectly drawn - need to add jitter to the coordinates to localize properly
jitter = 0.00005

for cuisine in new_cleaned_dict:
    c = -1
    for rest in new_cleaned_dict[cuisine]:
        c += 1 #to keep track of the restaurant loc
        #do some additional cleaning based on state and city:
        if rest['location']['state']:
            if rest['location']['state'] != 'CA':
                rest['remove'] = 1
        if rest['location']['city']:
            if rest['location']['city'] in outside_cities:
                rest['remove'] = 1

        #special cases:
        #tradamerican Sea Salt Fish & Chips loc 274 count 2 - location is actually closed
        if cuisine == 'tradamerican' and c == 274:
            rest['remove'] = 1

        if not rest['remove']: #if not already flagged for removal
            #create a point for the lat/long:
            if 'coordinates' in rest:
                rest['neighborhood'] = []
                if rest['coordinates']['latitude'] and rest['coordinates']['longitude']:
                    #create multiple versions of the loc_points (original and with jitter = 5 total)
                    loc_points = []
                    loc_points.append(Point(rest['coordinates']['longitude'],rest['coordinates']['latitude']))
                    loc_points.append(Point(rest['coordinates']['longitude']+jitter,rest['coordinates']['latitude']))
                    loc_points.append(Point(rest['coordinates']['longitude']-jitter,rest['coordinates']['latitude']))
                    loc_points.append(Point(rest['coordinates']['longitude'],rest['coordinates']['latitude']+jitter))
                    loc_points.append(Point(rest['coordinates']['longitude'],rest['coordinates']['latitude']-jitter))

                    for loc_point in loc_points:
                        #now check to see if the point is within any neighborhood:
                        for i, poly in enumerate(poly_list):
                            if loc_point.within(poly):
                                if df_map['name'][i] not in rest['neighborhood']:
                                    rest['neighborhood'].append(df_map['name'][i])
                                    num_rests_neighborhood[df_map['name'][i] + '_' + cuisine] += 1
                #another special case:
                if cuisine == 'mediterranean' and c == 427:
                    #mediterranean Gyro Spot Los Angeles loc 427 is supposed to be in West LA
                    rest['neighborhood'].append('West Los Angeles')
                    num_rests_neighborhood['West Los Angeles' + '_' + cuisine] += 1
            else:
                print(cuisine, rest['name'], 'no coords') #never prints, every location had a lat/long
                
#calculate all restaurants in a neighborhood (since there could be overlap with the cuisine types)
all_rests_neigh = {}

#initialize for all neighborhoods:
neighs = list(df_map['name'])
for neigh in neighs:
    all_rests_neigh[neigh] = 0

yelp_ids = {} #keep track of yelp ids, so do not double count restaurants

for cuisine in new_cleaned_dict:
    for rest in new_cleaned_dict[cuisine]:
        if not rest['remove']:
            if rest['id'] not in yelp_ids:
                yelp_ids[rest['id']] = 1
            else:
                yelp_ids[rest['id']] += 1

            if yelp_ids[rest['id']] == 1: #only count the first time it is encountered
                for neigh in rest['neighborhood']:
                    all_rests_neigh[neigh] += 1

# discovered: not all neighborhoods were found in the Los Angeles Yelp search
# add in restaurants from these missing cities
missing_cities = [
    'Agoura Hills',
    'Arcadia',
    'Avocado Heights',
    'Azusa',
    'Baldwin Park',
    'Calabasas',
    'Canoga Park',
    'Chatsworth',
    'Cerritos',
    'Charter Oak',
    'Chatsworth Reservoir',
    'Citrus',
    'La Mirada',
    'Covina',
    'Diamond Bar',
    'Duarte',
    'East La Mirada',
    'East Pasadena',
    'East San Gabriel',
    'El Monte',
    'Hacienda Heights',
    'Hansen Dam',
    'Harbor City',
    'Hawaiian Gardens',
    'Hidden Hills',
    'Irwindale',
    'Industry',
    'La Habra Heights',
    'Lake View Terrace',
    'Lakewood',
    'La Puente',
    'La Verne',
    'Lomita',
    'Lopez/Kagel Canyons',
    'Malibu',
    'Mayflower Village',
    'Monrovia',
    'Mount Washington',
    'North El Monte',
    'North Whittier',
    'Norwalk',
    'Pacoima',
    'Palos Verdes Estates',
    'West Carson',
    'Pico Rivera',
    'Pomona',
    'Porter Ranch',
    'Ramona',
    'Rancho Dominguez',
    'Rolling Hills Estates',
    'Rancho Palos Verdes',
    'Rolling Hills',
    'Rosemead',
    'Rowland Heights',
    'Santa Fe Springs',
    'San Dimas',
    'San Pasqual',
    'San Pedro',
    'Sierra Madre',
    'South Diamond Bar',
    'South El Monte',
    'South San Gabriel',
    'South San Jose Hills',
    'South Whittier',
    'Sylmar',
    'Sunland',
    'Temple City',
    'Topanga',
    'Tujunga',
    'Valinda',
    'Vincent',
    'Walnut',
    'West Covina',
    'Westlake Village',
    'West Puente Valley',
    'West San Dimas',
    'West Whittier-Los Nietos',
    'Whittier Narrows',
    'Wilmington',
    'Winnetka',
    'Woodland Hills'
    ]

missing_dict = {}
for neigh in missing_cities:
    if neigh not in missing_dict:
        missing_dict[neigh] = {}
        for cat in categories:
            cat_list = []
            for offset in offsets:
                location = neigh + ', CA'
                response = get_yelp_rests(URL,headers,location,offset,cat)
                if response.status_code != 200:
                    print(neigh, cat, 'bad status')
                else:
                    if response.json()['businesses']:
                        cat_list.append(response)
                    else:
                        break
            missing_dict[neigh][cat] = cat_list
            
#sort through the missing_dict to organize data based on category instead of area
#do not allow duplicates
missing_dict_cat = defaultdict(list)

for neigh, val in missing_dict.items():
    for cat,responses in val.items():
        for resp in responses:
            rests = resp.json()['businesses']
            for rest in rests:
                # first, check to make sure it is still open:
                if not rest['is_closed']:
                    rest_ids = [] #determine which restaurants already in the category
                    for already in missing_dict_cat[cat]:
                        rest_ids.append(already['id'])

                    if rest['id'] not in rest_ids:
                        #remove unneeded keys
                        for k in remove_keys:
                            rest.pop(k, None)
                        missing_dict_cat[cat].append(rest)
                        
# remove restaurant entries that were already present in the new_cleaned_dict
all_yelp_ids = {} #including removed ids

for cat, val in new_cleaned_dict.items():
    for rest in val:
        if rest['id'] not in all_yelp_ids:
            all_yelp_ids[rest['id']] = 1

cleaned_missing_dict = defaultdict(list)

for cat, val in missing_dict_cat.items():
    for rest in val:
        if rest['id'] not in all_yelp_ids:
            cleaned_missing_dict[cat].append(rest)
            
#add in the yelp details and reviews to the cleaned_missing_dict
for cuisine in cleaned_missing_dict:
    for rest in cleaned_missing_dict[cuisine]:
        if 'is_claimed' not in rest:
            resp_json = get_yelp_details(details_url + rest['id'],headers,cuisine,rest['name'])
            if resp_json:
                for key in added_keys:
                    if key in resp_json:
                        rest[key] = resp_json[key]
        if 'reviews' not in rest:
            reviews_url = details_url + rest['id'] + '/reviews'
            resp_json = get_yelp_reviews(reviews_url,headers,cuisine,rest['name'])
            if 'reviews' in resp_json:
                rest['reviews'] = resp_json['reviews']
                
#add the fsq data to the cleaned_missing_dict
for cuisine in cleaned_missing_dict:
    for rest in cleaned_missing_dict[cuisine]:
        if 'fsq_id' not in rest:
            resp_json = get_fsq_id(fs_url,fs_headers,cuisine,rest['name'],rest['coordinates']['latitude'],rest['coordinates']['longitude'])
            if 'fsq_id' in resp_json:
                rest['fsq_id'] = resp_json['fsq_id']
            elif 'fsq_id' in resp_json['place']:
                rest['fsq_id'] = resp_json['place']['fsq_id']
        if 'four square data' not in rest:
            if 'fsq_id' in rest:
                resp_json = get_fsq_fields(fs_url,rest['fsq_id'],fs_headers,fields,cuisine,rest['name'])
                if 'name' in resp_json:
                    rest['four square data'] = resp_json
                    
#flag for removal rests without locations in cleaned_missing_dict, and check for yelp/fsq mistmatch
for cuisine in cleaned_missing_dict:
    for rest in cleaned_missing_dict[cuisine]:
        if type(rest['location']['address1']) == None.__class__: #likely a food truck
            #add a tag to ignore this entry
            rest['remove'] = 1
        elif rest['location']['address1'] == '': #another way it can be empty
            rest['remove'] = 1
        else:
            rest['remove'] = 0
            if 'four square data' in rest:
                if rest['name'] != rest['four square data']['name']:
                    if 'location' in rest['four square data']:
                        if 'address' in rest['four square data']['location']:
                            #split the address to compare just the number:
                            yelp_address = rest['location']['address1'].split()
                            fs_address = rest['four square data']['location']['address'].split()
                            if yelp_address[0] != fs_address[0]:
                                rest['four square data'] = {} #remove
                                rest['fsq_id'] = 'mismatch'
                                
#add neighborhood data to cleaned_missing_dict
for cuisine in cleaned_missing_dict:
    c = -1
    for rest in cleaned_missing_dict[cuisine]:
        c += 1  #to keep track of the restaurant loc
        #do some additional cleaning based on state and city:
        if rest['location']['state']:
            if rest['location']['state'] != 'CA':
                rest['remove'] = 1
        if rest['location']['city']:
            if rest['location']['city'] in outside_cities:
                rest['remove'] = 1

        if not rest['remove']:  #if not already flagged for removal
            #create a point for the lat/long:
            if 'coordinates' in rest:
                rest['neighborhood'] = []
                if rest['coordinates']['latitude'] and rest['coordinates']['longitude']:
                    #create multiple versions of the loc_points (original and with jitter - 5 total)
                    loc_points = []
                    loc_points.append(Point(rest['coordinates']['longitude'], rest['coordinates']['latitude']))
                    loc_points.append(Point(rest['coordinates']['longitude'] + jitter, rest['coordinates']['latitude']))
                    loc_points.append(Point(rest['coordinates']['longitude'] - jitter, rest['coordinates']['latitude']))
                    loc_points.append(Point(rest['coordinates']['longitude'], rest['coordinates']['latitude'] + jitter))
                    loc_points.append(Point(rest['coordinates']['longitude'], rest['coordinates']['latitude'] - jitter))

                    for loc_point in loc_points:
                        #now check to see if the point is within any neighborhood:
                        for i, poly in enumerate(poly_list):
                            if loc_point.within(poly):
                                if df_map['name'][i] not in rest['neighborhood']:
                                    rest['neighborhood'].append(df_map['name'][i])
                                    num_rests_neighborhood[df_map['name'][i] + '_' + cuisine] += 1
            else:
                print(cuisine, rest['name'], 'no coords')  #never prints, every location had a lat/long
                
# add missing rests to the total number of restaurants in each neighborhood:
for cuisine in cleaned_missing_dict:
    for rest in cleaned_missing_dict[cuisine]:
        if not rest['remove']:
            if rest['id'] not in yelp_ids:
                yelp_ids[rest['id']] = 1
            else:
                yelp_ids[rest['id']] += 1

            if yelp_ids[rest['id']] == 1: #only count the first time it is encountered
                for neigh in rest['neighborhood']:
                    all_rests_neigh[neigh] += 1
                    
#gather all restaurants together into a df
df_columns = ['yelp_id',
              'fsq_id',
              'yelp_name',
              'fsq_name',
              'yelp_categories',
              'fsq_categories',
              'neighborhood',
              'latitude',
              'longitude',
              'census_block',
              'address1',
              'address2',
              'city',
              'state',
              'zip_code',
              'display_address',
              'fsq_address',
              'display_phone',
              'hours_type',
              'hours_open',
              'fsq_hours_popular',
              'fsq_hours_display',
              'is_chain',
              'yelp_is_claimed',
              'yelp_price',
              'yelp_rating',
              'yelp_review_count',
              'fsq_verified',
              'fsq_popularity',
              'fsq_price',
              'fsq_rating',
              'fsq_total_photos',
              'fsq_total_ratings',
              'fsq_total_tips',
              'fsq_tastes',
              'fsq_features',
              'website'
              ]

#the very first entry is not actually a restaurant - remove it:
new_cleaned_dict['afghani'][1]['remove'] = 1 #family meat market

df_rests = pd.DataFrame(columns = df_columns)

loc = 0
for cuisine in new_cleaned_dict:
    for rest in new_cleaned_dict[cuisine]:
        if not rest['remove']:
            #check to see if a row exists for this restaurant:
            if rest['id'] not in df_rests['yelp_id'].values:
                df_rests = add_rest_to_df(rest,df_rests)
                loc += 1
                
#above ends at loc = 11283; start from there to add missing rests
loc = 11283 #the ending location of df_rests
for cuisine in cleaned_missing_dict:
    for rest in cleaned_missing_dict[cuisine]:
        if not rest['remove']:
            #check to see if a row exists for this restaurant:
            if rest['id'] not in df_rests['yelp_id'].values:
                df_rests = add_rest_to_df(rest,df_rests)
                loc += 1
                
#save results
with open('df_rests_all.pkl','wb') as f:
    pickle.dump(df_rests, f)
    
# Remove the northern neighborhoods from df_map (not processing restaurants in these less visited areas)
remove_neighborhoods = ['Lancaster',
                        'Angeles Crest',
                        'Val Verde',
                        'Unincorporated Catalina Island',
                        'Quartz Hill',
                        'Littlerock',
                        'Lake Hughes',
                        'Unincorporated Santa Susana Mountains',
                        'Stevenson Ranch',
                        'Northwest Palmdale',
                        'Acton',
                        'Desert View Highlands',
                        'Unincorporated Santa Monica Mountains',
                        'Leona Valley',
                        'Ridge Route',
                        'Bradbury',
                        'Hasley Canyon',
                        'Castaic Canyons',
                        'Green Valley',
                        'Northeast Antelope Valley',
                        'Elizabeth Lake',
                        'Palmdale',
                        'Agua Dulce',
                        'Sun Village',
                        'Castaic',
                        'Southeast Antelope Valley',
                        'Avalon',
                        'Tujunga Canyons',
                        'Northwest Antelope Valley',
                        'Lake Los Angeles']

#add a column to keep or remove to df_map:
for ind in df_map.index:
    if df_map['name'][ind] in remove_neighborhoods:
        df_map.loc[ind,'remove'] = 1
    else:
        df_map.loc[ind,'remove'] = 0

df_map_removed = df_map.query('remove == 0')

#save the removed map:
with open('df_map_removed.pkl','wb') as f:
    pickle.dump(df_map_removed, f)
    
#Step 4. Use the Foursquare API to collect information about other places in each neighborhood

# Add in places of interest data
fs_url = "https://api.foursquare.com/v3/places/search"

#create a reduced list of categories: (previous version used more categories)
fsq_categories_dict_reduced = {
    '10004': 'Arts and Entertainment > Art Gallery',
    '10028': 'Arts and Entertainment > Museum > Art Museum',
    '10032': 'Arts and Entertainment > Night Club',
    '10037': 'Arts and Entertainment > Performing Arts Venue > Concert Hall',
    '10039': 'Arts and Entertainment > Performing Arts Venue > Music Venue',
    '10043': 'Arts and Entertainment > Performing Arts Venue > Theater',
    '10052': 'Arts and Entertainment > Strip Club',
    '11001': 'Business and Professional Services > Advertising Agency',
    '11046': 'Business and Professional Services > Financial Service > Banking and Finance',
    '12013': 'Community and Government > Education > College and University',
    '12066': 'Community and Government > Government Building > City Hall',
    '13003': 'Dining and Drinking > Bar',
    '13035': 'Dining and Drinking > Cafes, Coffee, and Tea Houses > Coffee Shop',
    '13040': 'Dining and Drinking > Dessert Shop',
    '16053': 'Landmarks and Outdoors > Waterfront',
    '17020': 'Retail > Boutique',
    '17034': 'Retail > Discount Store',
    '17068': 'Retail > Food and Beverage Retail > Gourmet Store',
    '17070': 'Retail > Food and Beverage Retail > Grocery Store / Supermarket > Organic Grocery',
    '17080': 'Retail > Food and Beverage Retail > Wine Store',
    '17108': 'Retail > Pawn Shop',
    '17116': 'Retail > Souvenir Store',
    '17138': 'Retail > Vintage and Thrift Store',
    '19014': 'Travel and Transportation > Lodging > Hotel',
    '19021': 'Travel and Transportation > Pier',
    '19031': 'Travel and Transportation > Transport Hub > Airport'
    }

neighborhood_places_dict = {}

for i, poly in enumerate(poly_list):
    sw = str(poly.bounds[1]) + ',' + str(poly.bounds[0])
    ne = str(poly.bounds[3]) + ',' + str(poly.bounds[2])

    if df_map['name'][i] not in neighborhood_places_dict:
        neighborhood_places_dict[df_map['name'][i]] = {}
    for key,val in fsq_categories_dict_reduced.items():
        if val not in neighborhood_places_dict[df_map['name'][i]]:
            params = {
                "categories": key,
                "sw": sw,
                'ne': ne,
                "limit": 50
            }
            response = requests.get(fs_url, params=params, headers=fs_headers)
            if response.status_code != 200:
                print(i, key, val, 'bad status')
            else:
                neighborhood_places_dict[df_map['name'][i]][val] = response.json()
                
#Aggregating the other places data
#key is the name of the neighborhood, for each entry go through and make sure it
#is actually located in the correct poly. If it is, then add it to the count for
#that category in the neighborhood

#keep track of places fsq_ids to not double count
fsq_ids = {}

hood_loc = -1
for hood, cats in neighborhood_places_dict.items():
    hood_loc += 1
    if hood not in remove_neighborhoods: #do not process the northern neighborhoods
        for cat in cats:
            if neighborhood_places_dict[hood][cat]['results']:
                neighborhood_places_dict[hood][cat]['count'] = 0
                if cat == 'Dining and Drinking > Cafes, Coffee, and Tea Houses > Coffee Shop': #split chains
                    neighborhood_places_dict[hood][cat]['count_chains'] = 0
                elif cat == 'Business and Professional Services > Financial Service > Banking and Finance': #split ATMs
                    neighborhood_places_dict[hood][cat]['count_atms'] = 0

                for result in neighborhood_places_dict[hood][cat]['results']:
                    if result['fsq_id'] not in fsq_ids:
                        fsq_ids[result['fsq_id']] = True
                        if 'geocodes' in result:
                            loc_points = []
                            loc_points.append(Point(result['geocodes']['main']['longitude'],result['geocodes']['main']['latitude']))
                            loc_points.append(Point(result['geocodes']['main']['longitude']+jitter,result['geocodes']['main']['latitude']))
                            loc_points.append(Point(result['geocodes']['main']['longitude']-jitter,result['geocodes']['main']['latitude']))
                            loc_points.append(Point(result['geocodes']['main']['longitude'],result['geocodes']['main']['latitude']+jitter))
                            loc_points.append(Point(result['geocodes']['main']['longitude'],result['geocodes']['main']['latitude']-jitter))

                            for loc_point in loc_points:
                                #now check to see if the point is within the neighborhood:
                                if loc_point.within(poly_list[hood_loc]):
                                    if 'count' not in neighborhood_places_dict[hood][cat]:
                                        neighborhood_places_dict[hood][cat]['count'] = 1
                                    else:
                                        neighborhood_places_dict[hood][cat]['count'] += 1

                                    if cat == 'Dining and Drinking > Cafes, Coffee, and Tea Houses > Coffee Shop':
                                        if result['chains']:
                                            neighborhood_places_dict[hood][cat]['count_chains'] += 1

                                    if cat == 'Business and Professional Services > Financial Service > Banking and Finance':
                                        if 'ATM' in result['name']:
                                            neighborhood_places_dict[hood][cat]['count_atms'] += 1
                                            
# store places info as a dataframe
df_density = pd.DataFrame()

#pulling out the bar and dessert cats so can combine for the first 5 entries (ran extra requests to Foursquare API)
bar_cats = ['	Dining and Drinking > Bar > Beach Bar', #fix this later
            'Dining and Drinking > Bar > Beer Bar',
            'Dining and Drinking > Bar > Beer Garden',
            'Dining and Drinking > Bar > Champagne Bar',
            'Dining and Drinking > Bar > Cocktail Bar',
            'Dining and Drinking > Bar > Dive Bar',
            'Dining and Drinking > Bar > Gay Bar',
            'Dining and Drinking > Bar > Hookah Bar',
            'Dining and Drinking > Bar > Hotel Bar',
            'Dining and Drinking > Bar > Ice Bar',
            'Dining and Drinking > Bar > Karaoke Bar',
            'Dining and Drinking > Bar > Lounge',
            'Dining and Drinking > Bar > Piano Bar',
            'Dining and Drinking > Bar > Pub',
            'Dining and Drinking > Bar > Rooftop Bar',
            'Dining and Drinking > Bar > Sake Bar',
            'Dining and Drinking > Bar > Speakeasy',
            'Dining and Drinking > Bar > Sports Bar',
            'Dining and Drinking > Bar > Tiki Bar',
            'Dining and Drinking > Bar > Whisky Bar',
            'Dining and Drinking > Bar > Wine Bar']

dessert_cats = ['Dining and Drinking > Dessert Shop > Creperie', 
                'Dining and Drinking > Dessert Shop > Cupcake Shop',
                'Dining and Drinking > Dessert Shop > Donut Shop',
                'Dining and Drinking > Dessert Shop > Frozen Yogurt Shop',
                'Dining and Drinking > Dessert Shop > Gelato Shop',
                'Dining and Drinking > Dessert Shop > Ice Cream Parlor',
                'Dining and Drinking > Dessert Shop > Pastry Shop',
                'Dining and Drinking > Dessert Shop > Pie Shop']

count = -1
for hood, cat in neighborhood_places_dict.items():
    if hood not in remove_neighborhoods:
        count += 1
        df_density.loc[count,'name'] = hood
        #add in the fsq density for other than rests:
        for key in cat:
            #special cases for the first 5 cities
            if len(cat) == 162: #need to combine the bar and dessert categories
                df_density.loc[count,'Bar'] = 0
                df_density.loc[count,'Dessert Shop'] = 0
                for bcat in bar_cats: #for the Bar categories
                    if 'count' in cat[bcat]:
                        df_density.loc[count,'Bar'] = df_density.loc[count,'Bar'] + cat[bcat]['count']

                for dcat in dessert_cats: #for the dessert categories
                    if 'count' in cat[dcat]:
                        df_density.loc[count,'Dessert Shop'] = df_density.loc[count,'Dessert Shop'] + cat[dcat]['count']

            #now for the rest of the categories:
            if key in list(fsq_categories_dict_reduced.values()):
                string_list = key.split('>')
                cat_name = string_list[-1].strip()
                if 'count' in cat[key]:
                    df_density.loc[count,cat_name] = cat[key]['count']

                    if key == 'Dining and Drinking > Cafes, Coffee, and Tea Houses > Coffee Shop':
                        df_density.loc[count,cat_name + '_chains'] = cat[key]['count_chains']
                        df_density.loc[count,cat_name + '_indie'] = cat[key]['count'] - cat[key]['count_chains']

                    if key == 'Business and Professional Services > Financial Service > Banking and Finance':
                        df_density.loc[count,cat_name + '_atms'] = cat[key]['count_atms']
                        df_density.loc[count,cat_name + '_banks'] = cat[key]['count'] - cat[key]['count_atms']
                else:
                    #there are none, set to 0
                    df_density.loc[count,cat_name] = 0
                    if key == 'Dining and Drinking > Cafes, Coffee, and Tea Houses > Coffee Shop':
                        df_density.loc[count,cat_name + '_chains'] = 0
                        df_density.loc[count,cat_name + '_indie'] = 0
                    if key == 'Business and Professional Services > Financial Service > Banking and Finance':
                        df_density.loc[count,cat_name + '_atms'] = 0
                        df_density.loc[count,cat_name + '_banks'] = 0

        #now, also add in the total number of restaurants, and the num restaurants for each cuisine
        for cuisine in categories:
            if hood + '_' + cuisine not in num_rests_neighborhood:
                df_density.loc[count,'Restaurant_' + cuisine] = 0
            else:
                df_density.loc[count,'Restaurant_' + cuisine] = num_rests_neighborhood[hood + '_' + cuisine]
        #total number of restaurants
        if hood not in all_rests_neigh:
            df_density.loc[count,'Restaurant_total'] = 0
        else:
            df_density.loc[count,'Restaurant_total'] = all_rests_neigh[hood]
            
#save df_density
with open('df_density.pkl','wb') as f:
    pickle.dump(df_density, f)