#This file contains helper functions used within the notebook. Often these are plotting and data manipulations which are used in several places 
#throughout the notebook. 
import pandas as pd 
import geopandas as gpd 
import fiona
import contextily as ctx
import datetime as dt

def concater(temp_dir):
    """Takes the collected purpleair data and converts it into a pandas dataframe
    
    ===
    Inputs:
    temp_dir = a list of paths pickled purpleair data with nomenclature legacy_month_day_year_time.pkl
    
    Returns:
    A single dataframe which combines all entires into a large pandas dataframe. 
    ===
    """
    data = []
    for dirs in temp_dir:
        temp_df = pd.read_pickle(dirs)
        
        #Data stored as pickled files. Need to read out the legacy entries. 
        if 'legacy' in dirs:
            temp_df['Time']= dirs.split('legacy_')[1].split('.pkl')[0]
            
        data.append(temp_df)

    return(pd.concat(data))

def geoplot(geo_df, ax, title):
    """A plotting function for geographic areas
    
    ===
    Inputs:
    geo_df: A geodataframe with geometry column for plotting. For added background map, please ensure EPSG is set to 3857
    ax: The axis for plotting
    title: the title of the plot
    
    Returns:
    none
    ===
    """    
    #Plot
    geo_df.plot(ax=ax,color = 'k',markersize = 20,marker='o')
    ax.set_title(title)

    #Plot a background map if the EPSG is configured correctly
    if geo_df.crs == 3857:
        ctx.add_basemap(ax)
    
def utah_border_plot(axis):
    """Plot the boundary of Utah. Assumes you have a copy of the Utah bounding geometry from https://gis.utah.gov/data/boundaries/citycountystate/ within your working directory. 

    ===
    Inputs:
    axis = axis for the plot of interest
    
    Returns:
    plot with Utah shape added
    ===
    """
    #Plot the Boundary of the State
    state =fiona.open('Utah.gdb')

    # Build the GeoDataFrame from Fiona Collection
    state_gdf = gpd.GeoDataFrame.from_features([feature for feature in state], crs=state.crs)
    # Get the order of the fields in the Fiona Collection; add geometry to the end
    columns = list(state.meta["schema"]["properties"]) + ["geometry"]
    # Re-order columns in the correct order
    state_gdf = state_gdf[columns]
    #Recast the mapping environment
    state_gdf = state_gdf.to_crs('EPSG:3857')
    state_gdf[state_gdf['STATE']=='Utah'].boundary.plot(ax=axis,color='k')

    return 

def hour_rounder(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +dt.timedelta(hours=t.minute//30))