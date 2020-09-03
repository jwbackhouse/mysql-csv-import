// Save connection details as object with properties:
// host, user, password, database
var { cnxDetails } = require('./connection.js')

// Establish mySQL connection
var mysql = require('mysql');
var connection = mysql.createConnection({ cnxDetails });

// Pull file names from directory in series
var fs = require('fs');
var walk = function(dir, done) {
  var results = [];
  fs.readdir(dir, function(err, list) {
    if (err) return done(err);
    var i = 0;
    (function next() {
      var file = list[i++];
      if (!file) return done(null, results);
      file = dir + '/' + file;
      fs.stat(file, function(err, stat) {
        if (stat && stat.isDirectory()) {
          walk(file, function(err, res) {
            results = results.concat(res);
            next();
          });
        } else {
          results.push(file);
          next();
        }
      });
    })();
  });
};

// Parallel version of the above
// var fs = require('fs');
// var path = require('path');
// var walk = function(dir, done) {
//   var results = [];
//   fs.readdir(dir, function(err, list) {
//     if (err) return done(err);
//     var pending = list.length;
//     if (!pending) return done(null, results);
//     list.forEach(function(file) {
//       file = path.resolve(dir, file);
//       fs.stat(file, function(err, stat) {
//         if (stat && stat.isDirectory()) {
//           walk(file, function(err, res) {
//             results = results.concat(res);
//             if (!--pending) done(null, results);
//           });
//         } else {
//           results.push(file);
//           if (!--pending) done(null, results);
//         }
//       });
//     });
//   });
// };

// Log results as array
walk("C:/ProgramData/MySQL/MySQL Server 8.0/Uploads", function(err, results) {
  if (err) throw err;
  console.log(results);

  // Extract filenames
  results.forEach(function(element) {

    // Check SQL connection
    // connection.connect(function(err) {
    //   if (err) throw err;
    //   console.log("Connected!");
    // });

    // Set escaped constants
    const enclosed = '\"';
    const terminate = '\\n';

    // Run SQl query
    connection.query("SET SESSION sql_mode = ''");
    connection.query("LOAD DATA INFILE '" + element + "' INTO TABLE darksky CHARACTER SET latin1 FIELDS OPTIONALLY ENCLOSED BY '" + enclosed + "' TERMINATED BY ',' LINES TERMINATED BY '" + terminate + "' IGNORE 1 ROWS (`latitude`,`longitude`,`day.time`,`day.summary`,`day.icon`,`day.sunriseTime`,`day.sunsetTime`,`day.moonPhase`,`day.precipIntensity`,`day.precipIntensityMax`,`day.precipProbability`,`day.temperatureHigh`,@dummy,`day.temperatureLow`,@dummy,`day.apparentTemperatureHigh`,`day.apparentTemperatureHighTime`,`day.apparentTemperatureLow`,`day.apparentTemperatureLowTime`,@dummy,@dummy,@dummy,`day.windSpeed`,`day.windGust`,@dummy,@dummy,`day.cloudCover`,`day.uvIndex`,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,@dummy,`hour.time`,`hour.summary`,`hour.icon`,`hour.precipIntensity`,`hour.precipProbability`,`hour.temperature`,`hour.apparentTemperature`,@dummy,@dummy,@dummy,`hour.windSpeed`,`hour.windGust`,@dummy,`hour.cloudCover`,`hour.uvIndex`,@dummy) SET `day.time` = from_unixtime(@daytime, '%Y-%m-%d'), `day.sunriseTime` = from_unixtime(@sunriseTime, '%Y-%m-%d %T'), `day.sunsetTime` = from_unixtime(@sunsetTime, '%Y-%m-%d %T'), `day.apparentTemperatureHighTime` = from_unixtime(@dayapparentTemperatureHighTime, '%Y-%m-%d %T'), `day.apparentTemperatureLowTime` = from_unixtime(@dayapparentTemperatureLowTime, '%Y-%m-%d %T')",
    function (error, results) {
      if (error) console.error(element + error);
      // console.log(results);
    });

  });

  // Close connection
  connection.end();
});
