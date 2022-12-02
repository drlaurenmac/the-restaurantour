import streamlit as st
import pandas as pd
import numpy as np
import pickle
from streamlit_folium import folium_static
import folium
import branca
import json
   
#load data:
#rest_neigh_dict contains all restaurant information sorted by each neighborhood:
with open('rest_neigh_dict.pkl','rb') as f:
    rest_neigh_dict = pickle.load(f)
    
#rest_cuisine_dict contains all restaurant information sorted by each cuisine:
with open('rest_cuisine_dict.pkl','rb') as f:
    rest_cuisine_dict = pickle.load(f)
    
#rest_price_dict contains all restaurant information sorted by each price:
with open('rest_price_dict.pkl','rb') as f:
    rest_price_dict = pickle.load(f) 

#clusters_dict contains results from clustering analysis:
with open('clusters_dict.pkl','rb') as f:
    clusters_dict = pickle.load(f)

#rename East Whittier to East La Mirada:
clusters_dict['cluster']['East La Mirada'] = clusters_dict['cluster'].pop('East Whittier')

#convert clusters_dict to df:    
df_clusters = pd.DataFrame(clusters_dict)
df_clusters.loc[:,'name'] = df_clusters.index
#sort by name:
df_clusters = df_clusters.sort_values(by = 'name')
df_clusters = df_clusters.reset_index().drop('index',axis=1)

#clusters_label_dict contains the assigned label from the clustering analysis:
with open('clusters_label_dict.pkl','rb') as f:
    clusters_label_dict = pickle.load(f)

#geojson info for la neighborhoods:
with open('la_neighborhoods_gj.json','r') as f:
    gj = json.load(f)
    
#geojson info including cluster labels:
with open('clusters_gj.json','r') as f:
    gj_clusters = json.load(f)

#neigh_cuisines_dict has all cuisines by neighborhood
#cuisines = neigh_cuisines_dict['All']
with open('neigh_cuisines_dict.pkl','rb') as f:
    neigh_cuisines_dict = pickle.load(f)
    
#neigh_prices_dict has all prices by neighborhood
#prices = neigh_prices_dict['All']
with open('neigh_prices_dict.pkl','rb') as f:
    neigh_prices_dict = pickle.load(f)
    
#cuisine_prices_dict has all prices by cuisine
with open('cuisine_prices_dict.pkl','rb') as f:
    cuisine_prices_dict = pickle.load(f)
    
#neigh_cuisine_prices_dict has all prices by neigh and cuisine (keys = neigh_cuisine)
with open('neigh_cuisine_prices_dict.pkl','rb') as f:
    neigh_cuisine_prices_dict = pickle.load(f)
    
#FUNCTIONS:
#create functions to plot the restaurant markers onto the folium map

def create_base_map(avg_lat,avg_long, gj, df_clusters,zoom_level):
    m = folium.Map(location=[avg_lat, avg_long],zoom_start=zoom_level)
    folium.TileLayer('cartodbpositron').add_to(m)
    c = folium.Choropleth(
        geo_data=gj,
        name="choropleth",
        data=df_clusters,
        columns=["name", "cluster"],
        key_on="properties.name",
        fill_color="RdYlBu",
        fill_opacity=0.7,
        line_opacity=0.2,
        bins = len(df_clusters['cluster'].unique())
    )
    #remove the legend
    for key in c._children:
        if key.startswith('color_map'):
            del(c._children[key])
    c.add_to(m)
  
    return m

def add_rest_marker(m,text,lat,long):    
    pp = folium.Html(text, script=True)
    iframe = branca.element.IFrame(pp, width=430, height=180)
    popup = folium.Popup(iframe, max_width=2650, parse_html=True)    
    folium.Marker(
    location=[lat, long],
    popup=popup,
    icon=folium.Icon(color="lightblue", icon='glyphicon-cutlery')
    ).add_to(m)
    
    return m

#find the top 3 restaurants given a neighborhood or category:
def find_top_rests(neigh_choice,cuisine_choice,price_choice,rest_dict):
    if neigh_choice != '':
        rests = pd.DataFrame(rest_dict[neigh_choice])
        if cuisine_choice != '':
            rests = find_by_cuisine(cuisine_choice,rests)
        if price_choice != '':
            rests = find_by_price(price_choice,rests)        
    elif cuisine_choice != '':
        rests = pd.DataFrame(rest_dict[cuisine_choice])
        if price_choice != '':
            rests = find_by_price(price_choice,rests)
    elif price_choice != '':
        rests = pd.DataFrame(rest_dict[price_choice])
        
    top_3 = rests.sort_values(by=['bayes_yelp_rating','yelp_review_count'],axis=0,ascending=False)[:3]
    top_3 = add_hover_text(top_3) 
    return top_3, top_3['lat'].mean(), top_3['long'].mean()

def add_hover_text(top_3):
    hover_text = []
    for index, row in top_3.iterrows():
        hover_text.append(('<b>{name}</b><br><br>'+
                           '<b>Neighborhood</b>: {neighborhood}<br>'+
                           '<b>Categories</b>: {categories}<br>'+
                           '<b>Price</b>: {price}<br>'+
                           '<b>Address</b>: {address}<br>'+
                           '<b>Phone</b>: {phone}<br>'+
                           '<b>Website</b>: <a href={website} target="_blank"> {website} </a><br>'
                           ).format(
            name=row['name'],
            neighborhood=', '.join([str(elem) for elem in row['neighborhood']]),
            categories=', '.join([str(elem) for elem in row['categories']]),
            price= row['price'] if row['price'] in neigh_prices_dict['All'] else 'not in database',
            address= ', '.join([str(elem) for elem in row['display_address']]),
            phone=row['display_phone'],
            website= row['website'] if str(row['website']) != 'nan' else 'not in database'
        ))
    top_3['text'] = hover_text
    return top_3

def find_by_cuisine(cuisine_choice,rests):
    df_holder = pd.DataFrame(columns=rests.columns)
    loc = 0
    for row in rests.index:
        if cuisine_choice in rests.loc[row,'categories']:
            df_holder.loc[loc,] = rests.loc[row,]
            loc += 1
    return df_holder

def find_by_price(price_choice,rests):
    df_holder = pd.DataFrame(columns=rests.columns)
    price_dict = rests.groupby('price').groups
    if price_choice in price_dict:
        loc = 0
        for ind in price_dict[price_choice]:
            df_holder.loc[loc,] = rests.loc[ind,]
            loc += 1
    return df_holder

def plot_recs(neigh_choice,cuisine_choice,price_choice,rest_dict,gj,df_clusters,zoom_level):
    top_3, avg_lat, avg_long = find_top_rests(neigh_choice,cuisine_choice,price_choice, rest_dict)
    m = create_base_map(avg_lat,avg_long,gj,df_clusters,zoom_level)
    for index, row in top_3.iterrows():
        m = add_rest_marker(m,row['text'],row['lat'],row['long'])
    return folium_static(m)

#Create the app:
#User selects neighborhood, cuisine, and/or price from drop-down menu

#user selects options in the sidebar:
with st.sidebar:
    st.title('The Restaurantour')
    
    st.markdown('**Select your desired criteria below and receive up to the top 3 restaurant choices suited to your tastes!**')


    by_neighborhood = st.checkbox('Neighborhood')
    by_cuisine = st.checkbox('Cuisine')
    by_price = st.checkbox('Price')
    neigh_choice = ''
    cuisine_choice = ''
    price_choice = ''

    if by_neighborhood:
        zoom_level = 13
        rest_dict = rest_neigh_dict    
        neigh_choice = st.selectbox('Please select a neighborhood:', [''] + sorted(list(rest_neigh_dict.keys())))
        if neigh_choice != '':
            #add here what the neighborhood cluster tag is
            for label, neighs in clusters_label_dict.items():
                if neigh_choice in neighs:
                    cluster_label = label
                    break
            st.write('Neighborhood type:', cluster_label)

        if by_cuisine:
            if neigh_choice != '':
                cuisine_choice = st.selectbox('Please select a cuisine:', [''] + neigh_cuisines_dict[neigh_choice])
        if by_price:
            if by_cuisine:
                if neigh_choice != '' and cuisine_choice != '':
                    price_choice = st.selectbox('Please select a price:', [''] + neigh_cuisine_prices_dict[neigh_choice + '_' + cuisine_choice]) 
            else:
                if neigh_choice != '':
                    price_choice = st.selectbox('Please select a price:', [''] + neigh_prices_dict[neigh_choice])

    elif by_cuisine:
        zoom_level = 10
        rest_dict = rest_cuisine_dict
        cuisine_choice = st.selectbox('Please select a cuisine:', [''] + neigh_cuisines_dict['All'])
        if by_price:
            if cuisine_choice != '':
                price_choice = st.selectbox('Please select a price:', [''] + cuisine_prices_dict[cuisine_choice])

    elif by_price:
        zoom_level = 10
        rest_dict = rest_price_dict
        price_choice = st.selectbox('Please select a price:', [''] + neigh_prices_dict['All'])

#shown in the main container:
st.header('The Restaurantour is an intelligent restaurant recommendation app designed for travelers.')

st.subheader('Recommendations currently provided for Los Angeles, CA.')

st.subheader('Please use the sidebar to choose search parameters.') 

#show the map with clusters when no choices are made:
if neigh_choice == '' and cuisine_choice == '' and price_choice == '':
    st.markdown('Explore LA neighborhoods in the map below. Warmer colors show areas with more restaurant "hotspots".')
    
    cluster_map = folium.Map(
        location=[34.04386826477363,-118.25840454347254],
        tiles='cartodbpositron',
        zoom_start=10,
        control_scale=True
        )
    folium.TileLayer('cartodbpositron').add_to(cluster_map)
    folium.Choropleth(
            geo_data=gj,
            name="choropleth",
            data=df_clusters,
            columns=["name", "cluster"],
            key_on="properties.name",
            fill_color="RdYlBu",
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name="Neighborhood Type",
            bins = len(df_clusters['cluster'].unique())
        ).add_to(cluster_map)
    folium.GeoJson(
        gj_clusters,
        name='Clusters',
        show=True,
        highlight_function=lambda x: {
            'fillOpacity':1
        },
        tooltip=folium.features.GeoJsonTooltip(
            fields=['name','cluster'],
            aliases=['Neighborhood','Type'],
        ),
    ).add_to(cluster_map)

    folium_static(cluster_map)
else:
    st.markdown('After making all selections, restaurant recommendations will be shown in map below.')
    st.markdown('Click on the restaurant icon to learn more details about each recommendation.')
    
    
#display top 3 restaurants given user selected choices:    
if by_neighborhood and by_cuisine and by_price:    
    if neigh_choice != '' and cuisine_choice != '' and price_choice != '':
            plot_recs(neigh_choice,cuisine_choice,price_choice,rest_dict,gj,df_clusters,zoom_level)
    
elif by_neighborhood and by_cuisine:
    if neigh_choice != '' and cuisine_choice != '':
        plot_recs(neigh_choice,cuisine_choice,price_choice,rest_dict,gj,df_clusters,zoom_level)
    
elif by_neighborhood and by_price:
    if neigh_choice != '' and price_choice != '':
        plot_recs(neigh_choice,cuisine_choice,price_choice,rest_dict,gj,df_clusters,zoom_level)

elif by_cuisine and by_price:
    if cuisine_choice != '' and price_choice != '':
        plot_recs(neigh_choice,cuisine_choice,price_choice,rest_dict,gj,df_clusters,zoom_level)

elif by_neighborhood:
    if neigh_choice != '':
        plot_recs(neigh_choice,cuisine_choice,price_choice,rest_dict,gj,df_clusters,zoom_level)
    
elif by_cuisine:
    if cuisine_choice != '':
        plot_recs(neigh_choice,cuisine_choice,price_choice,rest_dict,gj,df_clusters,zoom_level)

elif by_price:
    if price_choice != '':
        plot_recs(neigh_choice,cuisine_choice,price_choice,rest_dict,gj,df_clusters,zoom_level)
