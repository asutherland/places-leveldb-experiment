var $leveldown = require('leveldown');

$leveldown.repair(
  'leveldb-places.ldb',
  function() {
    console.log(
      'Repair ran, but it probably put some stuff in the lost subdir.');
    console.log(
      'Not sure what is up with that!');
  });
