var $levelup = require('levelup');

$levelup(
  'leveldb-places.ldb',
  {
    createIfMissing: false,
    errorIfExists: false,
    compression: true,
    cacheSize: 8 * 1024 * 1024, // the default,
    keyEncoding: 'utf8',
    valueEncoding: 'json'
  },
  function(err, db) {
    if (err) {
      console.error('LevelDB open error:', err);
      return;
    }

    console.log('leveldb output opened');

    db.createReadStream()
      .on('data', function(data) {
        console.log('\nKey:', JSON.stringify(data.key), '\n' +
                    JSON.stringify(data.value, null, 2));
      })
      .on('end', function() {
        console.log('----------- FIN -------------');
      });
  });
