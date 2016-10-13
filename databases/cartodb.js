var Promise = require('bluebird');
var tools = require('jm-tools');
var fandlebars = require('fandlebars');
var CartoDB = require('cartodb');

var format = function (output) {
  // TODO: Clean up the output from this so it's similar to all other outputs
  var result = [];
  if (output && output.rows) {
    result = output.rows;
    result.fields = output.fields;
  }
  return result;
};

var parameterize = function (param, type) {
  // CartoDB doesn't really support parameterized queries, so we'll do them our own way
  param = tools.normalizeToType(param);
  if (param === null || param === undefined) {
    return 'null';
  } else {
    param = param.toString();
  }
  var returnValue = "convert_from(decode('";
  // type = type || tools.getDataType(param)
  if (type !== 'text') {
    returnValue = "'" + param.replace(/'/g, "''") + (type ? "'::" + type : "'");
  } else {
    param = unescape(encodeURIComponent(param));
    for (var i = 0; i < param.length; i++) {
      returnValue += param.charCodeAt(i).toString(16);
    }
    returnValue += "', 'hex'), 'UTF-8')::" + type;
  }

  return returnValue;
};
var parameterizeQuery = function (query, params, columns) {
  params = JSON.parse(JSON.stringify(params || {}));
  for (var column in params) {
    var matchedColumn = columns && columns.filter(function (c) {
      return c.name === column;
    })[0];
    params[column] = parameterize(params[column], matchedColumn && !matchedColumn.transformed && matchedColumn.type);
  }
  return fandlebars(query, params);
};

var sendRequest = function (cartoDatabase, query, params, returnRaw, attempts) {
  attempts = attempts || 0;
  return new Promise(function (resolve, reject) {
    cartoDatabase.execute(parameterizeQuery(query, params), {}, {
      format: 'json'
    })
      .done(function (response) {
        resolve(returnRaw ? response : format(response));
      })
      .error(function (err) {
        if (attempts < 3) {
          console.log('trying again', attempts);
          sendRequest(cartoDatabase, query, params, returnRaw, attempts++).then(resolve).catch(reject);
        } else {
          reject(new Error(JSON.stringify(err, null, 2)));
        }
      });
    });
};

module.exports = function (connectionConfig) {
  var cartoDatabase = new CartoDB.SQL({
    user: connectionConfig.connection.account,
    api_key: connectionConfig.connection.apiKey
  });
  var returnObject = {
    query: function (query, params, returnRaw, columns) {
      return new Promise(function (resolve, reject) {

        if (query.length > 5) {
          sendRequest(cartoDatabase, query, params, returnRaw)
            .then(resolve)
            .catch(reject);
        } else {
          reject('Query Too Short: (' + query.length + ') chars');
        }
      });
    },
    close: function () {
      return new Promise(function (resolve, reject) {
        // Dummy function, cartodb connections close as soon as the query is done
        cartoDatabase = null;
        resolve(true);
      });
    }
  };
  return returnObject;
};
