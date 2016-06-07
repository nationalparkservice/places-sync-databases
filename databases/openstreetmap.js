var Promise = require('bluebird');
var superagent = require('superagent');
var OAuth = require('oauth').OAuth;
var sqlite = require('./sqlite');
var tools = require('jm-tools');

// Oauthify superagent
require('superagent-oauth')(superagent);

var loadCache = function (cacheConfig) {
  return sqlite(cacheConfig).then(function (database) {
    return {
      'database': database,
      'table': cacheConfig.table
    };
  });
};

var validateUser = function (connection, oauth) {
  return new Promise(function (resolve, reject) {
    superagent.get(connection.address + '0.6/user/details.json')
      .sign(oauth, connection.access_key, connection.access_secret)
      .end(function (err, res) {
        if (!err && res.status === 200) {
          resolve(res.body);
        } else {
          reject(new Error(err));
        }
      });
  });
};

var createOauth = function (connection) {
  return new OAuth(
    'http://' + connection.address + 'oauth/request_token',
    'http://' + connection.address + 'oauth/access_token',
    connection.consumer_key,
    connection.consumer_secret,
    '1.0',
    null,
    'HMAC-SHA1');
};

var initialize = function (source) {
  var tasks = [{
    'name': 'OAuth',
    'description': 'Creates the oauth object',
    'task': createOauth,
    'params': [source.connection]
  }, {
    'name': 'cacheDb',
    'description': 'Create and return the cache database',
    'task': loadCache,
    'params': [source.connection && source.connection.cache]
  }, {
    'name': 'userInfo',
    'description': 'Tests the OAuth by getting the user info',
    'task': validateUser,
    'params': [source.connection, '{{OAuth}}']
  }];

  tools.iterateTasks(tasks).then(function (results) {
    return {
      'oauth': results.OAuth,
      'user': results.userInfo && results.userInfo.body,
      'connection': source,
      'cache': results.cacheDb
    };
  });
};

module.exports = function (connectionConfig) {
  var initializedConnection;
  var returnObject = {
    query: function () {
      var that = this;
      var thoseArgs = arguments;
      return returnObject.verify().then(function (connection) {
        if (connection.cache) {
          return connection.cache.query.apply(that, thoseArgs);
        } else {
          return tools.dummyPromise(null, 'No cache specified for query');
        }
      });
    },
    verify: function () {
      // Tries to open the connection
      if (initializedConnection) {
        return tools.dummyPromise(initializedConnection);
      } else {
        return initialize(connectionConfig).then(function (connection) {
          initializedConnection = connection;
          return returnObject.verify();
        });
      }
    },
    close: function () {
      return returnObject.verify().then(function (connection) {
        initializedConnection = null;
        return new Promise(function (resolve, reject) {
          // Dummy function, cartodb connections close as soon as the query is done
          resolve(true);
        });
      });
    }
  };
  return returnObject;
};
