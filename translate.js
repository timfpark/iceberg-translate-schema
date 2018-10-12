const avro = require('avsc');
const crc32 = require('buffer-crc32');
const snappy = require('snappy');

const codecs = {
    snappy: function(buf, cb) {
        // Avro appends checksums to compressed blocks, which we skip here.
        return snappy.uncompress(buf.slice(0, buf.length - 4), cb);
    }
};

const LOCATION_SCHEMA = avro.Type.forSchema({
    type: 'record',
    name: 'Location',
    fields: [
        { name: 'accuracy', type: ['null', 'double'], default: null },
        { name: 'altitude', type: ['null', 'double'], default: null },
        { name: 'altitudeAccuracy', type: ['null', 'double'], default: null },
        { name: 'course', type: ['null', 'double'], default: null },
        {
            name: 'features',
            type: {
                type: 'array',
                items: { name: 'id', type: 'string' }
            }
        },
        { name: 'latitude', type: 'double' },
        { name: 'longitude', type: 'double' },
        { name: 'speed', type: ['null', 'double'], default: null },
        { name: 'source', type: 'string', default: 'device' },
        { name: 'timestamp', type: 'long' },
        { name: 'user_id', type: 'string' }
    ]
});

if (process.argv.length != 4) {
    console.log('usage: node translate.js <input> <output>');
    process.exit(0);
}

let inputFile = process.argv[2];
let outputFile = process.argv[3];

let outputEncoder = avro.createFileEncoder(outputFile, LOCATION_SCHEMA, {
    codec: 'snappy',
    codecs: {
        snappy: function(buf, cb) {
            // Avro requires appending checksums to compressed blocks.
            const checksum = crc32(buf);
            snappy.compress(buf, function(err, deflated) {
                if (err) {
                    cb(err);
                    return;
                }
                const block = Buffer.alloc(deflated.length + 4);
                deflated.copy(block);
                checksum.copy(block, deflated.length);
                cb(null, block);
            });
        }
    }
});

console.log(`processing ${inputFile} -> ${outputFile}`);

avro.createFileDecoder(inputFile, { codecs })
    .on('data', row => {
        outputEncoder.write(row);
    })
    .on('finish', () => {
        console.log('done!');
        outputEncoder.end();
    });
