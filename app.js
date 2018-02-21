'use strict';

const express = require('express');
let app = express();

const async = require('asyncawait/async');
const await = require('asyncawait/await');
const rp = require('request-promise');

const chalk = require( "chalk" );
const fs = require('fs');
const JSONStream = require('JSONStream');

const es = require('event-stream');

let path = require('path');

let countryGeocode = require('country-geocode');
//var localGeocodeServer = require.resolve( "../geocode/local-reverse-geocoder/app.js" );

let geocoder = require('local-reverse-geocoder');

/** Function initializations */
let forceGC = function () {
  if (global.gc) {
    global.gc();
  } else {
    console.warn('No GC hook! Start your program as `node --expose-gc app.js`.');
  }
};

let timer = function(name) {
  let start = new Date();
  return {
    stop: function() {
      var end  = new Date();
      var time = end.getTime() - start.getTime();
      console.log('Timer:', name, 'finished in', time, 'ms');
    }
  };
};

let convertToDecimal = initialNumber => initialNumber/10000000;

let convertMillisecondsToSeconds = initialNumber => Math.round(initialNumber/1000);

let isPastFortyEightHours = (initialTimeMs, newTimeMs) => {
  return Math.abs(initialTimeMs - newTimeMs) > 1.728e+8;
};

let isPastTwelveHours = (initialTimeMs, newTimeMs) => {
  return Math.abs(initialTimeMs - newTimeMs) > 4.32e+7;
};

let isPastSixHours = (initialTimeMs, newTimeMs) => {
  return Math.abs(initialTimeMs - newTimeMs) > 2.16e+7;
};

let isPastTwoHours = (initialTimeMs, newTimeMs) => {
  return Math.abs(initialTimeMs - newTimeMs) > 7.2e+6;
};

let isPastAnHour = (initialTimeMs, newTimeMs) => {
  return Math.abs(initialTimeMs - newTimeMs) > 3.6e+6;
};

let isPastHalfHour = (initialTimeMs, newTimeMs) => {
  return Math.abs(initialTimeMs - newTimeMs) > 1.8e+6;
};

let uniqueValues = arr => {
  return arr.filter((x, i, a) => a.indexOf(x) == i);
};

geocoder.init({dumpDirectory: 'geocode/geonames_dump', load:{admin1: false, admin2: false, admin3And4: false, alternateNames: false}}, function() {

    app.get(/fileparser/, function(req, res) {

    let filename = req.query.filename || false;
    let basecountry = req.query.basecountry || false;
    let starttime = req.query.starttime || false;
    let endtime = req.query.endtime || false;
    let maxResults = req.query.maxResults || 1;
    if (!filename || !basecountry || !starttime || !endtime) {
      return res.status(400).send(`Bad Request. Filename: ${filename} Country: ${basecountry} Starttime: ${starttime} EndTime: ${endtime}`);
    }

    try {

      let t = timer('Entire Run');
      let jsonPath = path.join(__dirname, '..', 'storage', 'app', decodeURI(filename));

      const configs = {
        locationFile: jsonPath,
        baseCountry: basecountry.toUpperCase(),
        startTimeMs: starttime,
        endTimeMs: endtime
      };

      let isWithinDateRange = currentTimeMs => {
        return  (currentTimeMs >= configs.startTimeMs) && (currentTimeMs <= configs.endTimeMs);
      };

      let locationData;

      try{

          let fileStream = fs.createReadStream(configs.locationFile, {
              encoding: 'utf8', bufferSize: 40960
          });

          fileStream.pipe(JSONStream.parse('locations.*')).pipe(es.through(function (data) {
              console.log(data);
              locationData = JSON.parse(data);
          }, function end() {
              console.log('stream finish');
          }));

      }catch (err){
          console.log('error', err);
      }

      let results = [];
      let currentVisit = {country: '', countryName: '', visitStart: '', visitEnd: '', cities: [] };
      let count = 0;
      let country = '';
      let lastLocation = {timestampMs: 0, latitudeE7: 0, longitudeE7: 0};
      let resultingTrips = [];
      let darkDates = [];
      let darkDateChecker = null;

      locationData.forEach(location => {

      if(isPastTwelveHours(lastLocation.timestampMs, location.timestampMs) && isWithinDateRange(location.timestampMs)) {

          country = countryGeocode([convertToDecimal(location.longitudeE7), convertToDecimal(location.latitudeE7)]);

          console.log('pegando_city',country);

          if (typeof country != 'undefined') { // Ignore oceans, etc.
            lastLocation = location;

            if (currentVisit.country !== country.alpha2Code) {
              currentVisit.visitStart = location.timestampMs;

                  if (currentVisit.country !== '') { // Ignore first row

                      // Push the previous country's results.
                      results.push({
                        country: currentVisit.country,
                        countryName: currentVisit.countryName,
                        visitStart: currentVisit.visitStart, 
                        visitEnd: currentVisit.visitEnd, 
                        cities: currentVisit.cities
                      });
                    }

                  // Initialize new country.
                  currentVisit.visitEnd = location.timestampMs;
                  currentVisit.country = country.alpha2Code;
                  currentVisit.countryName = country.name;
                  currentVisit.cities = [];

                  // Assign first city
                  if (currentVisit.country.toUpperCase() !== configs.baseCountry) {
                    currentVisit.cities.push([convertToDecimal(lastLocation.latitudeE7),convertToDecimal(lastLocation.longitudeE7)]);
                  }
                }

              // Push Next City
              if (currentVisit.country.toUpperCase() !== configs.baseCountry) {
                currentVisit.cities.push([convertToDecimal(lastLocation.latitudeE7),convertToDecimal(lastLocation.longitudeE7)]);
              }
              
              if(darkDateChecker && isPastFortyEightHours(darkDateChecker.timestampMs,location.timestampMs)){
                darkDates.push({endTime: convertMillisecondsToSeconds(darkDateChecker.timestampMs), startTime:convertMillisecondsToSeconds(location.timestampMs)});
              }

              darkDateChecker = location;

            }
          }
        });

      // unsetting the location cache 
      locationData = null;
      forceGC();

      if (currentVisit.country !== '') {
      // Ignore first row
      currentVisit.visitStart = lastLocation.timestampMs;
      currentVisit.cities.push([convertToDecimal(lastLocation.latitudeE7),convertToDecimal(lastLocation.longitudeE7)]);

      results.push({
        country: currentVisit.country,
        countryName: currentVisit.countryName,
        visitStart: currentVisit.visitStart, 
        visitEnd: currentVisit.visitEnd, 
        cities: currentVisit.cities
      });
    }

    // get results in order (oldest to newest)
    results.reverse();
    darkDates.reverse();


  //  Build Cities List
  let getCities = async (function () {

    for (var i = 0; i < results.length; i++) {
      if (results[i].cities.length > 0) {
        var points = [];
        var count2 = 0;
        // if (results[i].cities.length > 1500)
          results[i].cities.forEach( city => {
            if (count2 < 1500){
              points.push({latitude: city[0], longitude: city[1]});
            }
            count2++;
          });

        geocoder.lookUp(points, function(err, res) {
            console.log(res);
            let cityNameList = [];
            res.forEach( row => {
              row.forEach( city => cityNameList.push(city.name));
            });

            let cityNamesWithoutDuplicates = cityNameList.filter(function(item, pos, arr){
              return pos === 0 || item !== arr[pos-1];
            });

            // reassign city list, and reverse so that the cities are in order from oldest to latest visits
            results[i].cities = cityNamesWithoutDuplicates.reverse();
        });
      }
    }
  });

  let parseTrips = (results) => {
    let trips = [];
    let tripCount = 0;
    let lastCountry = '';
    let listOfCountries = uniqueValues(results.map( result => result.country));
    let totalTimeAway = 0;
    results.forEach( (result, idx, array) => {
        // if the count is 0 and it's not base country
        // or if the last country was the base and the new country is not base.
        if (result.country !== configs.baseCountry) {
          if (tripCount === 0 || lastCountry === configs.baseCountry) {
            // Initialize new trip
            tripCount++;
            trips.push({
              start: convertMillisecondsToSeconds(result.visitStart),
              end: '',
              countries: [result.countryName],
              locations: []
            }); 
          } else if (idx === array.length - 1) { 
            // Last element in the array. add cities and wrap it up.
            trips[tripCount-1].end = convertMillisecondsToSeconds(result.visitStart);
          }

          // append cities list to trip array
          // if it's the same country, append to the same array
          if(lastCountry === result.country) {
            trips[tripCount-1].locations[trips[tripCount-1].locations.length - 1] += 
            result.cities.map( city => city + ", " + result.country + ';').join(' ');
            //trips[tripCount-1].locations[trips[tripCount-1].locations.length - 1].concat(result.cities.map( city => city + ", " + result.country + '; '));
          }
          //// if it's a different country, push a new array
          else {
            trips[tripCount-1].countries.push(result.countryName);
            trips[tripCount-1].locations.push(result.cities.map( city => city + ", " + result.country + ';').join(' '));
          }
        } else {
          // If it is the base country, 
          // and it's not the first row, and the last country was not base close off the last trip.
          if (tripCount !== 0 && lastCountry !== configs.baseCountry)
            trips[tripCount-1].end = convertMillisecondsToSeconds(result.visitStart);
        }

        lastCountry = result.country;
    });

    trips.forEach( trip => {
      //totalTimeAway += Math.abs(trip.start - trip.end);
      trip.countries = uniqueValues(trip.countries).join(', ');
    });

    return trips;
  };

  // After promise completes and all cities are loaded.
  getCities().then(_ => {

      resultingTrips[0] = parseTrips(results);
      resultingTrips[1] = darkDates;

      t.stop(); // prints the time elapsed to the js console
      const used = process.memoryUsage().heapUsed / 1024 / 1024;
      console.log(`The script uses approximately ${Math.round(used * 100) / 100} MB`);

      // Remove Json file from the cache
      forceGC();
      return res.json(resultingTrips);
    });

  } 
  catch (err) {
    return res.status(500).send(err);
  }


  });

  let port = Number(process.env.PORT || 3001);
  app.listen(port, function() {
    console.log('file parser listening on port ' + port);
  });

});