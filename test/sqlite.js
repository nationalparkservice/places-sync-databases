var databases = require('../');

var filePath = ':memory:';
var databaseExpected = {
  close: {},
  query: {},
  queryList: {}
};

module.exports = [{
  'name': 'createDbConnection',
  'description': 'Connects to the SQLite Database',
  'task': databases,
  'params': [{
    'type': 'sqlite',
    'connection': filePath
  }],
  'operator': 'structureEqual',
  'expected': databaseExpected
}, {
  'name': 'create table',
  'description': 'creates a table in the database',
  'task': '{{createDbConnection.query}}',
  'params': ['CREATE TABLE "test" (a text, b int, primary key (a));'],
  'operator': 'structureEqual',
  'expected': []
}, {
  'name': 'insert into table',
  'description': 'adds a single row to the table',
  'task': '{{createDbConnection.query}}',
  'params': ['INSERT INTO "test" values ({{a}}, {{b}});', {
    'a': 'one',
    'b': 1
  }],
  'operator': 'structureEqual',
  'expected': []
}, {
  'name': 'insert into table',
  'description': 'adds a single row to the table',
  'task': '{{createDbConnection.queryList}}',
  'params': [
    'INSERT INTO "test" values ({{a}}, {{b}});', [{
      'a': 'two',
      'b': 2
    }, {
      'a': 'three',
      'b': 3
    }, {
      'a': 'four',
      'b': 4
    }]
  ],
  'operator': 'deepEqual',
  'expected': [
    [],
    [],
    []
  ]
}, {
  'name': 'close database',
  'description': 'closes the connection',
  'task': '{{createDbConnection.query}}',
  'params': ['SELECT * FROM "test" ORDER BY "b";'],
  'operator': 'deepEqual',
  'expected': [{
    'a': 'one',
    'b': 1
  }, {
    'a': 'two',
    'b': 2
  }, {
    'a': 'three',
    'b': 3
  }, {
    'a': 'four',
    'b': 4
  }, ]
}, {
  'name': 'close database',
  'description': 'closes the connection',
  'task': '{{createDbConnection.close}}',
  'params': [],
  'operator': 'deepEqual',
  'expected': {"open": false, "filename": filePath, "mode": 65542, "_events": {}}
}];
