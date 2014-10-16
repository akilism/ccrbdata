var fs = require('fs'),
  csv = require('csv'),
  _ = require('lodash'),
  Q = require('q'),
  concat = require('concat-stream'),
  through = require('through');

var files = {};

files['ccrb-2'] = {
  'name': 'Type of Force Allegation',
  'desc': 'Type of force allegations for 2009 - 2013 count and percentage.'
};

files['ccrb-3'] = {
  'name': 'Type of Abuse of Authority',
  'desc': 'Type of abuse of authority allegations for 2009 - 2013 count and percentage.'
};

files['ccrb-4'] = {
  'name': 'Type of Discourtesy Allegation',
  'desc': 'Type of discourtesy allegations for 2009 - 2013 count and percentage.'
};

files['ccrb-5'] = {
  'name': 'Type of Offensive Language Allegation',
  'desc': 'Type of offensive language allegations for 2009 - 2013 count and percentage.'
};

files['ccrb-13a-1'] = {
  'name': 'Where Incidents Took Place By Precinct - Manhattan',
  'desc': 'Where Incidents that Led to a Complaint Took Place by Precinct - Manhattan.'
};

files['ccrb-13a-2'] = {
  'name': 'Where Incidents Took Place By Precinct - Manhattan',
  'desc': 'Where Incidents that Led to a Complaint Took Place by Precinct - Manhattan.'
};

files['ccrb-13b'] = {
  'name': 'Where Incidents Took Place By Precinct - Bronx',
  'desc': 'Where Incidents that Led to a Complaint Took Place by Precinct - Bronx.'
};

files['ccrb-13c-1'] = {
  'name': 'Where Incidents Took Place By Precinct - Brooklyn',
  'desc': 'Where Incidents that Led to a Complaint Took Place by Precinct - Brooklyn.'
};

files['ccrb-13c-2'] = {
  'name': 'Where Incidents Took Place By Precinct - Brooklyn',
  'desc': 'Where Incidents that Led to a Complaint Took Place by Precinct - Brooklyn.'
};

files['ccrb-13d-1'] = {
  'name': 'Where Incidents Took Place By Precinct - Queens',
  'desc': 'Where Incidents that Led to a Complaint Took Place by Precinct - Queens.'
};

files['ccrb-13d-2'] = {
  'name': 'Where Incidents Took Place By Precinct - Queens',
  'desc': 'Where Incidents that Led to a Complaint Took Place by Precinct - Queens.'
};

files['ccrb-13e'] = {
  'name': 'Where Incidents Took Place By Precinct - Staten Island',
  'desc': 'Where Incidents that Led to a Complaint Took Place by Precinct - Staten Island.'
};

files['ccrb-44a-1'] = {
  'name': 'Substantiated Complaints by Precinct - Manhattan',
  'desc': 'Where Incidents that Led to a Substantiated Complaint Took Place - Manhattan.'
};

files['ccrb-44a-2'] = {
  'name': 'Substantiated Complaints by Precinct - Manhattan',
  'desc': 'Where Incidents that Led to a Substantiated Complaint Took Place - Manhattan.'
};

files['ccrb-44b'] = {
  'name': 'Substantiated Complaints by Precinct - Bronx',
  'desc': 'Where Incidents that Led to a Substantiated Complaint Took Place - Bronx.'
};

files['ccrb-44c-1'] = {
  'name': 'Substantiated Complaints by Precinct - Brooklyn',
  'desc': 'Where Incidents that Led to a Substantiated Complaint Took Place - Brooklyn.'
};

files['ccrb-44c-2'] = {
  'name': 'Substantiated Complaints by Precinct - Brooklyn',
  'desc': 'Where Incidents that Led to a Substantiated Complaint Took Place - Brooklyn.'
};

files['ccrb-44d-1'] = {
  'name': 'Substantiated Complaints by Precinct - Queens',
  'desc': 'Where Incidents that Led to a Substantiated Complaint Took Place - Queens.'
};

files['ccrb-44d-2'] = {
  'name': 'Substantiated Complaints by Precinct - Queens',
  'desc': 'Where Incidents that Led to a Substantiated Complaint Took Place - Queens.'
};

files['ccrb-44e'] = {
  'name': 'Substantiated Complaints by Precinct - Staten Island',
  'desc': 'Where Incidents that Led to a Substantiated Complaint Took Place - Staten Island.'
};

var parseCsv = function (key) {
  var deferred = Q.defer();

  var write = function (buffer) {
    //rename keys to 2009n, 2009p
    this.queue(buffer);
  };

  var end = function () {
    console.log('finished:', key);
    // process.exit();
    this.queue(null);
  };

  var tr = through(write, end);

  var readStream = fs.createReadStream(key + '.csv');
  // var writeStream = fs.createWriteStream('ccrb-2.json');
  var parseOpts = {
    'columns': true,
    'trim': true,
    'auto_parse': true
  };

  readStream.pipe(csv.parse(parseOpts)).pipe(tr).pipe(concat(function (data) {
    files[key].data = data;
    console.log('writing', key + '.json');
    fs.writeFile(key + '.json', JSON.stringify(files[key]));
    deferred.resolve(true);
  }));//.pipe(writeStream); //.pipe(process.stdout);

  return deferred.promise;
};

var main = function () {
  var keys = Object.keys(files);

  var runParser = function (name) {
    parseCsv(name).then(function (retVal) {
      if(keys.length > 0) {
        runParser(keys.shift());
      } else {
        console.log('done');
      }
    });
  };

  runParser(keys.shift());
};


main();

