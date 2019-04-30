import numpy as np
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import func
import pandas as pd
from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start<br/>"
        f"/api/v1.0/start/end"
        )





@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return a dict of all dates"""
    year_ago = dt.date(2017,8,23) - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores
    results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= year_ago).all()

    # Save the query results as a Pandas DataFrame and set the index to the date column
    prcp_df = pd.DataFrame(results, columns=['date','precipitation'])
    prcp_df.set_index('date', inplace=True)
    prcp_df
    #Sort the dataframe by date
    prcp_df = prcp_df.sort_values('date')
    

    prcp_dict = prcp_df.to_dict()
    return jsonify(prcp_dict)


@app.route("/api/v1.0/stations")
def stations():
    """Return a list of stations"""
    # Query all passengers
    results2 = session.query(Measurement.station,func.count(Measurement.station).label("Count")).\
                    group_by(Measurement.station).all()

    return jsonify(list(results2))


@app.route("/api/v1.0/tobs")
def temperatures():
    """Return a dict of all dates"""
    year_ago = dt.date(2017,8,23) - dt.timedelta(days=365)

    # Perform a query to retrieve the data and precipitation scores
    results = session.query(Measurement.date, Measurement.tobs).filter(Measurement.date >= year_ago).all()

    # Save the query results as a Pandas DataFrame and set the index to the date column
    tobs_df = pd.DataFrame(results, columns=['date','temperature'])
    tobs_df.set_index('date', inplace=True)
    tobs_df
    #Sort the dataframe by date
    tobs_df = tobs_df.sort_values('date')
    

    tobs_dict = tobs_df.to_dict()
    return jsonify(tobs_dict)


@app.route('/api/v1.0/test')
def test_func():
   '''Return key temperature observations (min, max, and average) for given start, end date range'''
   # Reutrn data from query
   obs_results = session.query(func.count(Measurement.tobs)).\
                                     all()
   # Build results into list, note position 0 = station, 1 = min temp, 2 = max temp, 3 = avg temp

   return jsonify(obs_results)

@app.route("/api/v1.0/<start>/")
@app.route("/api/v1.0/<start>/<end>")
def start_end(start,end=None):
    """When given the start and the end date, calculate the TMIN, TAVG, and TMAX for dates between the start and end date inclusive."""

    selector = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
    
    if not end:
        results = session.query(*selector)\
            .filter(Measurement.date >= start)\
            .all()
        temps = list(np.ravel(results))
        return jsonify(temps)
    
    start_date = dt.datetime.strptime(start, "%Y-%m-%d")
    end_date = dt.datetime.strptime(end, "%Y-%m-%d")
    
    results = session.query(*selector)\
        .filter(Measurement.date >= start_date)\
        .filter(Measurement.date <= end_date).all()  #.group_by(Measurement.date).all()
    print(results)
    #temps2 = [result[0] for result in results]
    temps2 = list(np.ravel(results))
    return jsonify(temps2)

if __name__ == '__main__':
    app.run(debug=True)


#@app.route("/api/v1.0/<start>")
#def min_max_avg(start):
#    """Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
#   When given the start only, calculate TMIN, TAVG, and TMAX for all dates greater than and equal to the start date."""
#
#   temps = session.query(func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
#   filter(Measurement.date >= start).all()       
#    return jsonify(list(temps))