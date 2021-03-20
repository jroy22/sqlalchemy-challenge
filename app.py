# Dependencies
import pandas as pd
import numpy as np
import datetime as dt

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Reflecting an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
measurements = Base.classes.measurement
stations = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def home():
    """List all available api routes."""
    return (
        f"Available Hawaii API Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start><br/>"
        f"/api/v1.0/<start>/<end>"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    # Create session (link) from Python to the DB
    session = Session(engine)

    # Query precipiation data
    recent_date = session.query(measurements.date).order_by(measurements.date.desc()).first()

    # Pass the integers to find the year of data
    year_from_recent = dt.date(2017,8,23) - dt.timedelta(days=365)

    sel = [measurements.date,measurements.prcp]
    dates_prcp = session.query(*sel).filter(measurements.date > '2016-08-22' ).filter(measurements.date < '2017-08-22').order_by(measurements.date).all()
    
    session.close()

    # Convert to dictionary using date ad the key and prcp as the value
    date_prcp_query = []
    for date, prcp in dates_prcp:
        query_dict = {}
        query_dict["date"] = date
        query_dict["prcp"] = prcp
        date_prcp_query.append(query_dict)

    # Return JSON representation1
    return jsonify(date_prcp_query)


@app.route("/api/v1.0/stations")
def stations():

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # List of stations from the dataset.
    active_stations = session.query(measurements.station, func.count(measurements.station)).group_by(measurements.station).order_by(func.count(measurements.station).desc()).all()

    session.close()

    station_query =[]
    for station in active_stations:
        station_dict ={}
        station_dict["station"] = station
        station_query.append(station_dict)

    return jsonify(station_query)

@app.route("/api/v1.0/tobs")
def tobs():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Get the start and 12 month dates
    recent_date = session.query(measurements.date).order_by(measurements.date.desc()).first()
    year_from_recent = dt.date(2017,8,23) - dt.timedelta(days=365)

    # Query to get active stations and the most active station
    active_stations = session.query(measurements.station, func.count(measurements.station)).group_by(measurements.station).order_by(func.count(measurements.station).desc()).all()
    most_active_station_id = active_stations[0][0]

    # Query to get the temperature athe active station
    temp_freq = session.query(measurements.date,measurements.station,measurements.tobs).filter(measurements.station == most_active_station_id).filter(measurements.date > '2016-08-22')\
            .filter(measurements.date < '2017-08-22').order_by(measurements.date).all()

    session.close()


    temp_query = []
    for date, station, tobs in temp_freq:
        temp_dict = {}
        temp_dict["date"] = date
        temp_dict["station"] = station
        temp_dict["temperature"] = tobs
        temp_query.append(temp_dict)
    
    return jsonify(temp_query)

@app.route("/api/v1.0/<start>")
def start(start):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Queries
    min_temp = session.query(func.min(measurements.tobs)).filter(measurements.date >= start).all()
    min_temp = min_temp[0][0]
    
    max_temp = session.query(func.max(measurements.tobs)).filter(measurements.date >= start).all()
    max_temp = max_temp[0][0]

    avg_temp = session.query(func.avg(measurements.tobs)).filter(measurements.date >= start).all()
    max_temp = max_temp[0][0]
    
    session.close() 

    start_query = [min_temp, max_temp,avg_temp]
    
    return jsonify(start_query)

@app.route("/api/v1.0/<start>/<end>")
def start_end(start,end):
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Queries
    min_temp = session.query(func.min(measurements.tobs)).filter(measurements.date >= start).filter(measurements.date <= end).all()
    min_temp = min_temp[0][0]
    
    max_temp = session.query(func.max(measurements.tobs)).filter(measurements.date >= start).filter(measurements.date <= end).all()
    max_temp = max_temp[0][0]
    
    avg_temp = session.query(func.avg(measurements.tobs)).filter(measurements.date >= start).filter(measurements.date <= end).all()
    max_temp = max_temp[0][0]
    
    session.close()

    start_end_query = [min_temp, max_temp,avg_temp]
    
    return jsonify(start_end_query)

if __name__ == '__main__':
    app.run(debug=True)
