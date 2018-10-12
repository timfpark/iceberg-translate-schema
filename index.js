const request = require('request');
const avro = require('avsc');
const snappy = require('snappy'); // Or your favorite Snappy library.

const codecs = {
    snappy: function(buf, cb) {
        // Avro appends checksums to compressed blocks, which we skip here.
        return snappy.uncompress(buf.slice(0, buf.length - 4), cb);
    }
};

request(
    {
        method: 'GET',
        url: 'http://localhost:3035/q?p=userid1&sk=95000&ek=105000',
        encoding: null
    },
    (err, response, body) => {
        let decoder = new avro.streams.BlockDecoder({ codecs })
            .on('metadata', function(type, codec, header) {
            })
            .on('data', row => {
                console.log(JSON.stringify(row));
            })
            .on('end', () => {
                console.log('finished');
            });
        decoder.end(body);
    }
);
