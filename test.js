var n1qljs = require('./n1qljs');
require('source-map-support').install();

var defaultDocs = [
    { id: 'foo', value: {n: 14} },
    { id: 'bar', value: {x: 22} }
];
var defaultKeyspace = {
    name: 'default',
    count: function() {
        return defaultDocs.length;
    },
    keys: function() {
        var keys = [];
        for (var i = 0; i < defaultDocs.length; ++i) {
            keys.push(defaultDocs[i].id);
        }
        return keys;
    },
    fetch: function(keys) {
        var docs = [];
        for (var i = 0; i < defaultDocs.length; ++i) {
            if (keys.indexOf(defaultDocs[i].id) !== -1) {
                docs.push(defaultDocs[i]);
            }
        }
        return docs;
    }
};

var handler = {
    item: function(item) {
        console.log('Item:', item);
    },
    done: function() {
        console.log('Done');
    },
    fatal: function(err) {
        console.log('Fatal:', err)
    },
    error: function(err) {
        console.log('Error:', err)
    },
    warning: function(err) {
        console.log('Warning:', err)
    },
};

n1qljs.Execute([defaultKeyspace], 'SELECT * FROM default WHERE x=22', handler);