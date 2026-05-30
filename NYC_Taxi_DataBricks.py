# Generated from: NYC_Taxi_DataBricks.ipynb
# Converted at: 2026-05-30T20:25:19.596Z
# Next step (optional): refactor into modules & generate tests with RunCell
# Quick start: pip install runcell

# # I. DATA INGESTION & CLEANING


# ## •	Loading the dataset with Pandas


import pandas as pd
import numpy as np

df = spark.read.table("workspace.default.nyc").toPandas()

df.head(5)

df.info(memory_usage = "deep")

# ## •	Handle missing values and invalid coordinates


df.isna().sum()

# _**No missing values**_
# <hr>


# _**To validate the longtitide and latitude values we need to check the column values. Latitude need to be between -90 to 90 and Longtitude from -180 to 180.**_


df_filtered_columns = df[["pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude"]]
df_filtered = df_filtered_columns.query("pickup_latitude >= -90 and pickup_latitude <= 90 &"
                                        "dropoff_latitude >= -90 and dropoff_latitude <= 90 &"
                                        "pickup_longitude >= -180 and pickup_longitude <= 180 &"
                                        "dropoff_longitude >= -180 and dropoff_longitude <= 180")
df_filtered_columns.info()

# _**We can see from the df_filtered_columns.info() result that the longtitude and latitude filtered column rows are the exact same number as the initial data set(1458644 entries).Also, null values are checked above with isna.sum(). Than means all the values are valid.**_
# <hr>


# ## • Filter or remove extreme outliers


# _**For identifing outliers, first we need to be searching the appropriate columns and setting the approriate metrics.**_
# 
# _**1. Passengers: Passengers need to be more than 0 and less than or equal to 6 <br>
# 2. Trip Duration: Trips below 60 seconds are not entirely valid and we set an upper limit of 12 hours (43,200 seconds). <br>
# 3. Datetimes: Drop off times can't be less pickup times for obvious reasons.**_


df.describe().round(2)

# **_1. Passengers: Passengers need to be more than 0 and less than or equal to 6_**


passenger_zero = df[["passenger_count"]].query("passenger_count == 0 or passenger_count > 6")
passenger_zero.count()


# _**We currently have 65 rows with ouliers. 60 equal to 0, 5 greater than 6**_ <hr>


# **_2. Trip Duration: Trips below to 60 seconds are not entirely valid and we set an upper limit of 12 hours (43,200 seconds)._**


invalid_trips = df[['trip_duration']].query("trip_duration == 0 or trip_duration <= 60 or trip_duration > 43200")
invalid_trips.count()

# **_We have 10770 trips below 60 and above 43,200 seconds._** <hr>


# _**3. Datetimes: Drop off times can't be less pickup times for obvious reasons.**_


invalid_times = df[['pickup_datetime','dropoff_datetime']].query("pickup_datetime > dropoff_datetime")
invalid_times.count()

# _**No anomalies in those columns**_ <hr>


# _**Finally we combine the querries to clean the data in a new data frame**_



df_clean = df.query("passenger_count >= 1 and passenger_count <= 6 and "
                     "trip_duration > 60 and trip_duration <= 43200 and "
                     "pickup_datetime <= dropoff_datetime")


print(f"Original dataset: {len(df):,} rows")
print(f"Cleaned dataset: {len(df_clean):,} rows")
print(f"Removed: {len(df) - len(df_clean):,} rows ({(len(df) - len(df_clean))/len(df)*100:.2f}%)")

# ## •	Feature engineering: <br>
# ##Extract pickup_hour, pickup_day, pickup_month from pickup_datetime
# 


df_clean = df_clean.assign( pickup_hour = df_clean["pickup_datetime"].dt.hour,
                            pickup_day = df_clean["pickup_datetime"].dt.day_name().str.slice(stop=3),
                            pickup_month = df_clean["pickup_datetime"].dt.month_name().str.slice(stop=3)
)
                 
df_clean.head(5)

# - ## EXTRA! <br>
# ## Memory optimization


# _**First we check which data we can downcast**_


df_clean.info(memory_usage="deep")

# _**We can see that the initial 250.4MB of memory have turned to 408.7MB due to the addition of the pickup_datetime data.**_


# _**'We can safely downcast all integers according to theie max values, and objects to Category/String**_
# 
# * id --> _'object'_
# * vendor_id --> _'int64'_
# * Passenger_count --> _'int64'_
# * store_and_fwd_flag --> _'object'_
# * trip_duration --> _'int64'_
# * pickup_hour  --> _'int32'_        
# * pickup_day  -->  _'object'_    
# * pickup_month  --> _'object'_


df_clean.describe().loc[["max"]].T.round(2)

# **_• Vendor_id, passenger_count and pickup_hour are well within the range of 8bits (-128 to 127), with a lot of space for data base expansion_ <br>**
# **_• Trip_duration went from 3,526,282 to 43,177 as max value, since we trimmed ouliers but still is cast as int64. Int16 ranges from (-32,768 to 32,767), so we will use int32 instead._**


df_clean= df_clean.astype({"vendor_id":"int8",
                "passenger_count":"int8",
                "trip_duration":"int32",
                "pickup_hour":"int8"})
 

df_clean.info(memory_usage="deep")

df_clean[["id"]].nunique()/len(df)*100

# _**The 'id' column is 99.2% unique so we will use it a string (pyarrow) type to save some memory space**_


df_clean[["store_and_fwd_flag"]].nunique()/len(df)*100

df_clean[["store_and_fwd_flag"]].nunique()

# **_'store_and_fwd_flag' has only 2 unique values in all rows so we can cast it as a category type._**


df_clean[["pickup_day"]].nunique()/len(df)*100

df_clean[["pickup_month"]].nunique()/len(df)*100

# **_We know for a fact that pickup_day and pickup_month columns, have 6 and 12 unique values respectively, out of the 1447855 total entries in the dataset. Those will be cast as categories too._**


df_clean= df_clean.astype({"id":"string[pyarrow]",
                          "store_and_fwd_flag":"category",
                          "pickup_day": "object",
                          "pickup_month": "object"})

df_clean.info(memory_usage="deep")

# 


# _**By downcasting the data types we went from 408.7MB of memory to 255.4MB**_


# #II. DATA VISUALIZATION & EXPLORATORY ANALYSIS


# ## ⚠️ The graph colors are built in Light Theme. Dark themes will invert the colors
# <hr>
# 


import plotly.express as px
import plotly.graph_objects as go

# ## Trip Distribution Analysis


# - ###  Trips per hour


df_clean

trips_per_hour = df_clean.groupby(["pickup_hour"])[["id"]].count().reset_index(names = "hours")
trips_per_hour

fig = px.bar(trips_per_hour,
              x= "hours",
              y="id",
              labels=dict(hours="<b>Hours</b>", id="<b>Trips Count</b>"),
              text="id",
              title="<b><i>Tripes per Hour</b></i>",
             
              color_discrete_sequence=["#ffae1b"])

fig.update_traces(base=dict(width = 3),
                    marker_line_color="#b300b3",
                    marker_line_width=1.75,
                    textfont_size=13)
        
               
    
fig.update_layout(plot_bgcolor="#fafafa",
                  xaxis_title_font=dict(size=16),
                  yaxis_title_font=dict(size=16),
                  xaxis=dict(tickfont=dict(size=16)),
                  yaxis=dict(tickfont=dict(size=16)))
                  #width=1000,
                  #height=600)

fig.update_layout(xaxis=dict(showgrid=True, gridcolor="#e6e6e6"),
                  yaxis=dict(showgrid=True, gridcolor="#e6e6e6"),
                  plot_bgcolor="#f9f9f9")
fig.show()


# - ### Trips per day


trips_per_day = df_clean.groupby(["pickup_day"])[["id"]].count().reset_index(names = "day")

# We need to create a custom category to sort the day of the week, else they are going to be sorted aplheticaly, since we have cast the pickup_day as category type

day_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]   
trips_per_day["day"] = pd.Categorical(trips_per_day["day"], categories=day_order, ordered=True) 
trips_per_day = trips_per_day.sort_values("day") 
trips_per_day

fig = px.bar(trips_per_day,
              x= "day",
              y="id",
              labels=dict(day="<b>Day</b>", id="<b>Trips Count</b>"),
              text="id",
              title="<b><i>Daily Trips</b></i>",
             
              color_discrete_sequence=["#ffae1b"])

fig.update_traces(base=dict(width = 3),
                    marker_line_color="#b300b3",
                    marker_line_width=1.75,
                    textfont_size=13)
        
               
    
fig.update_layout(plot_bgcolor="#fafafa",
                  xaxis_title_font=dict(size=16),
                  yaxis_title_font=dict(size=16),
                  xaxis=dict(tickfont=dict(size=16)),
                  yaxis=dict(tickfont=dict(size=16)))


fig.update_layout(xaxis=dict(showgrid=True, gridcolor="#e6e6e6"),
                  yaxis=dict(showgrid=True, gridcolor="#e6e6e6"),
                  plot_bgcolor="#f9f9f9")
fig.show()


# - ###  Passenger count distribution


passenger_dist = df_clean.groupby(["passenger_count"])[["id"]].count().reset_index(names = "passenger_count")
passenger_dist



fig = px.bar(passenger_dist,
              x= "passenger_count",
              y="id",
              labels=dict(passenger_count="<b>Passenger Count</b>", id="<b>Trips Count</b>"),
              text="id",
              title="<b><i>Passenger Distribution</b></i>",
             
              color_discrete_sequence=["#ffae1b"])


fig.update_traces(marker_line_color="#b300b3",
                    marker_line_width=1.75,
                    textfont_size=13,
                    textposition='auto')
        
               
    
fig.update_layout(plot_bgcolor="#fafafa",
                  xaxis_title_font=dict(size=16),
                  yaxis_title_font=dict(size=16),
                  xaxis=dict(tickfont=dict(size=16)),
                  yaxis=dict(tickfont=dict(size=16)))


fig.update_layout(xaxis=dict(showgrid=True, gridcolor="#e6e6e6"),
                  yaxis=dict(showgrid=True, gridcolor="#e6e6e6"),
                  plot_bgcolor="#f9f9f9")
fig.show()

# ## Trip Duration vs Passenger Count


# - ### Scatter plot with optional regression line


x = df_clean["passenger_count"]
y = df_clean["trip_duration"]
slope, intercept = np.polyfit(x, y, 1)
print(f"Regression equation: Trip duration = {intercept:.2f} + {slope:.2f} * Passenger count")

df_sample = df_clean.sample(n=10000, random_state=42)
x_sample = df_sample["passenger_count"]
y_sample = df_sample["trip_duration"]
slope, intercept = np.polyfit(x_sample, y_sample, 1)
print(f"Regression equation: Trip duration = {intercept:.2f} + {slope:.2f} * Passenger count")

%pip install statsmodels

# Manual 'jitter' to spread the values 
df_sample["passenger_jitter"] = df_sample["passenger_count"] + np.random.uniform(-0.5, 0.5, len(df_sample))

fig = px.scatter(df_sample, 
                 x="passenger_jitter", 
                 y="trip_duration",
                 color="passenger_count",
                 color_continuous_scale="Rainbow",
                 trendline="ols",
                 trendline_color_override="black", 
                 labels=dict(passenger_jitter="<b>Passenger Count</b>", 
                             trip_duration="<b>Duration (s)</b>"),
                 title="<b><i>Passenger Count vs Trip Duration</b></i> (Jittered)",
                 opacity=0.5)

fig.update_traces(line=dict(width=3), selector=dict(mode='lines'))

# Regression line
fig.add_annotation(
    x=df_sample["passenger_count"].median(),
    y=df_sample["trip_duration"].mean(),
    text="<b>Regression Line</b>",
    showarrow=False,
    yshift=15,             # Pulls text slightly above the line
    font=dict(color="black", size=18))

# Force the axis to normal count
fig.update_layout(
    xaxis = dict(
        tickmode = "array",
        tickvals = [1, 2, 3, 4, 5, 6],
        ticktext = ["1", "2", "3", "4", "5", "6"],
        showgrid=True, 
        gridcolor="#e6e6e6"),
    yaxis=dict(showgrid=True, gridcolor="#e6e6e6"),
    plot_bgcolor="#f9f9f9")

fig.show()

# ## ⚠️ Due to the volume of values html isn't able to put all the entries in graphics. instead, we use a sample of 10,000 values 
# <hr>
# 


%pip install statsmodels

# Manual 'jitter' to spread the values 
df_sample["passenger_jitter"] = df_sample["passenger_count"] + np.random.uniform(-0.5, 0.5, len(df_sample))

fig = px.scatter(df_sample, 
                 x="passenger_jitter", 
                 y="trip_duration",
                 color="passenger_count",
                 color_continuous_scale="Rainbow",
                 trendline="ols",
                 trendline_color_override="black", 
                 labels=dict(passenger_jitter="<b>Passenger Count</b>", 
                             trip_duration="<b>Duration (s)</b>"),
                 title="<b><i>Passenger Count vs Trip Duration</b></i> (Jittered)",
                 opacity=0.5)

fig.update_traces(line=dict(width=3), selector=dict(mode='lines'))

# Regression line
fig.add_annotation(
    x=df_sample["passenger_count"].median(),
    y=df_sample["trip_duration"].mean(),
    text="<b>Regression Line</b>",
    showarrow=False,
    yshift=15,             # Pulls text slightly above the line
    font=dict(color="black", size=18))

# Force the axis to normal count
fig.update_layout(
    xaxis = dict(
        tickmode = "array",
        tickvals = [1, 2, 3, 4, 5, 6],
        ticktext = ["1", "2", "3", "4", "5", "6"],
        showgrid=True, 
        gridcolor="#e6e6e6"),
    yaxis=dict(showgrid=True, gridcolor="#e6e6e6"),
    plot_bgcolor="#f9f9f9")

fig.show()

# _**We use 'jitter' to spread the values along the x axis, otherwise all y values would get 'stacked' on a straight line along the x axis values**_


# - ### Explain qualitative correlation between trip duration and passenger count


correlation = df_clean["passenger_count"].corr(df_clean["trip_duration"])
print(f"Correlation coefficient (r): {correlation:.4f}")

# **_Here, we use the original data frame for the correlation although numbers are really close with the df_sample coefficient being at 0.0092_**


# ### A Correlation coefficient of 0.0134, shows that passenger count and trip duration have
# ###  a close to 0 relation. That means that our variables are reliable not to eachother. <hr>


# - ## Geospatial Analysis


# - ### Heatmap of pickup locations



fig = go.Figure(go.Densitymap(lat=df_sample["pickup_latitude"], 
                              lon=df_sample["pickup_longitude"],
                              radius=5,
                              colorscale="Viridis",
                              showscale=False,
                # Extra data on hover                                
                              customdata=df_sample[["passenger_count", "trip_duration"]], 
                              hovertemplate=(
                                    "<b>Location Info</b><br>" +
                                    "Latitude: %{lat:.4f}<br>" +
                                    "Longitude: %{lon:.4f}<br>" +
                                    "Passengers: %{customdata[0]}<br>" +
                                    "Duration: %{customdata[1]}s" +
                                    "<extra></extra>"))) #  Remove the trace box

fig.update_layout(map_style="open-street-map", map_zoom=10,
    map_center=dict(
        lat=df_sample["pickup_latitude"].mean(), 
        lon=df_sample["pickup_longitude"].mean()))
    
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

fig.show()

# - ### Heatmap of dropoff locations


fig = go.Figure(go.Densitymap(lat=df_sample["dropoff_latitude"],
                              lon=df_sample["dropoff_longitude"],
                              radius=5,
                              colorscale="Viridis",
                              showscale=False,
                    # Extra data on hover                                
                                customdata=df_sample[["passenger_count", "trip_duration"]], 
                                hovertemplate=(
                                    "<b>Location Info</b><br>" +
                                    "Latitude: %{lat:.4f}<br>" +
                                    "Longitude: %{lon:.4f}<br>" +
                                    "Passengers: %{customdata[0]}<br>" +
                                    "Duration: %{customdata[1]}s" +
                                    "<extra></extra>"))) #  Remove the trace box

fig.update_layout(map_style="open-street-map", map_zoom=10,
    map_center=dict(
        lat=df_sample["dropoff_latitude"].mean(), 
        lon=df_sample["dropoff_longitude"].mean()))
    
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()

# - ## Traffic Pattern Analysis


# - ### Compare peak hours vs off-peak hours



peak_hours = (df_clean
              .groupby("pickup_hour")
              .agg(id_count=("id", "count"))
              .reset_index())


peak_hours["peak_status"] = np.where(
    peak_hours['id_count'] > peak_hours['id_count'].mean(), "Peak", "Off-Peak")


peak_hours = (peak_hours
                .set_index(["pickup_hour", "peak_status"])
                .sort_values(by="id_count", ascending=False)
                .reset_index())
peak_hours

fig = px.scatter(peak_hours,
               x="id_count",
               y="pickup_hour",
               color="peak_status",
               labels=dict(pickup_hour= "<b>Hour</b>", id_count="<b>Trips</b>"),
               title="<b><i>Peak Hours</b></i>",
               color_discrete_map={
                   "Peak": "#b300b3" , 
                   "Off-Peak": "#ffae1b"
               })


fig.update_traces(marker=dict(symbol="diamond",size=8))
              

fig.update_layout(plot_bgcolor="#fafafa",
                   xaxis_title_font=dict(size=16),
                   yaxis_title_font=dict(size=16),
                   xaxis=dict(tickmode="linear", dtick=10000, tickfont=dict(size=16)),
                   yaxis=dict(dtick=4, tickfont=dict(size=16)),
                   legend_title_text="Peak Status")
                  

fig.update_layout(xaxis=dict(showgrid=True, gridcolor="#e6e6e6"),
                  yaxis=dict(showgrid=True, gridcolor="#e6e6e6"),
                  plot_bgcolor="#f9f9f9")

fig.show()

# - ### Compare weekday vs weekend traffic



# First we need to create the 'weekday' 'weekend' column
df_clean["weekend"] = np.where(df_clean['pickup_day'].isin(['Sat', 'Sun']), "Weekend", "Weekday")

# We create the custom category as above
day_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]   
df_clean["pickup_day"] = pd.Categorical(df_clean["pickup_day"], categories=day_order, ordered=True)


weekdays_weekends = (df_clean
                     .groupby(["weekend", "pickup_day"], observed=True)
                     .agg(trip_count=("id", "count"))
                     .sort_values(by=["weekend"]))

weekdays_weekends

plot_data = weekdays_weekends.reset_index()

fig = px.sunburst(plot_data, 
                  path=["weekend", "pickup_day"], 
                  values="trip_count",
                  title="<b>Trip Distribution: Weekday vs. Weekend</b>",
                  color="weekend", 
                  color_discrete_map={
                      "Weekday": "#ffae1b",  
                      "Weekend": "#b300b3"})

fig.update_traces(
    textinfo="label+value",
    texttemplate="<b>%{label}</b><br>%{value:,} trips")

fig.update_layout(width=700, height=700)
fig.show()

# # III. DESCRIPTIVE STATISTICS & ANALYTICS


# - ### 	What is the average trip duration per hour?


df_clean

trip_duration_ph = df_clean.groupby(["pickup_hour"])[["trip_duration"]].mean()
trip_duration_ph

# - ### What is the average trip duration per day of the week?


avg_trip_duration_pw = df_clean.groupby(["pickup_day"])[["trip_duration"]].mean()
avg_trip_duration_pw

# - ### Which pickup location has the most trips?


most_trips_location = (df_clean
                       .groupby(["pickup_longitude", "pickup_latitude"])[["id"]].count()
                       .sort_values(by=["id"], ascending=False)
                       .head())
most_trips_location

# - ### Which trips are the longest and shortest?


longest_trips = (df_clean
                       .groupby(["pickup_longitude", "pickup_latitude"])[["trip_duration"]].max()
                       .sort_values(by=["trip_duration"], ascending=False)
                       .head())
longest_trips


shortest_trips = (df_clean
                       .groupby(["pickup_longitude", "pickup_latitude"])[["trip_duration"]].min()
                       .sort_values(by=["trip_duration"], ascending=True)
                       .head(10))
shortest_trips


# - ### Which hour of day has the highest traffic volume?


peak_hours.head()

# - ### What is the correlation coefficient between trip duration and passenger count?


correlation = df_clean["passenger_count"].corr(df_clean["trip_duration"])
print(f"Correlation coefficient (r): {correlation:.4f}")

# ### A Correlation coefficient of 0.0134, shows that passenger count and trip duration have
# ###  a close to 0 relation. That means that our variables are reliable not to eachother. <hr>


# # IV. INTERACTIVE PYTHON DASHBOARD


# Save df_clean and df_sample to Unity Catalog 
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


df_clean_spark = spark.createDataFrame(df_clean)
df_clean_spark.write.mode("overwrite").saveAsTable("workspace.default.nyc_clean")
print(f" Saved df_clean ({len(df_clean):,}) rows")

df_sample_spark = spark.createDataFrame(df_sample)
df_sample_spark.write.mode("overwrite").saveAsTable("workspace.default.nyc_sample")
print(f" Saved df_sample ({len(df_sample):,}) rows")

print("\n Tables saved successfully")


# Save to Unity Catalog for dashboard use
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


df_most_trips = spark.createDataFrame(most_trips_location.reset_index())
df_most_trips.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("workspace.default.nyc_most_trips_location")
print(f" Saved most_trips_location")


df_longest = spark.createDataFrame(longest_trips.reset_index())
df_longest.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("workspace.default.nyc_longest_trips")
print(f"Saved longest_trips")


df_shortest = spark.createDataFrame(shortest_trips.reset_index())
df_shortest.write.mode("overwrite").option("overwriteSchema", True).saveAsTable("workspace.default.nyc_shortest_trips")
print(f" Saved shortest_trips")

print("\n All dataframes created and saved to Unity Catalog!")

#