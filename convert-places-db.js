/**
 * Quick experiment to map the normalized SQLite places database to what seems
 * like a reasonable denormalized LevelDB representation for sizing info.
 *
 * NOT YET IMPLEMENTED: Bookmarks.
 *
 * I am treating these as primary use-cases:
 * - Bookmarks by hierarchy: directly populate the bookmarks UI; fetch a given
 *    level of hierarchy one at a time, starting from the root.
 * - History by time: Starting from now or any timestamp, figure out what got
 *    looked at.
 * - Awesomebar.  You type stuff, you want to see stuff you care about.
 * - Info by URL: You know the site's URL, you want info on it, like its
 *    favicon.
 *
 * I am treating the following as not important to this experiment:
 * - Recent bookmarks / recent tags: I didn't know these existed, but these
 *   could arguably be handled by just maintaining a streaming cache at the
 *   application level and otherwise are not that much data.
 *
 * Important lexicographic ordering notes!
 * - When storing (numeric) values, we frequently care about the largest values
 *   and then want to see the smaller values.  Larger values by default tend to
 *   encode as lexicographically greater values, which means they come after
 *   the things we don't care about.  It's fastest in LevelDB to always
 *   iterate in the lexicographically greater direction (allegedly), so if we
 *   invert the encoding so that larger numbers are lexicographically smaller,
 *   our read order does what we want.  (And to start, we just seek to the
 *   largest value our encoding supports.)
 *
 * Accordingly, I am creating the following separate LevelDB namespaces.  It
 * might make sense to create distinct databases in cases where we want
 * to have completely separate cache pools because the awesomebar is
 * fighting link colorizing.
 *
 * - 'B', Bookmarks.  Composite key: [depth, parent id, id].
 *    Depth does not need to be zero padded because we would never scan
 *    beyond the bounds of our level of hierarchy anyways.
 *
 *    We could put the position in the key if we were concerned about having
 *    a folder with a mind-boggling number of bookmarks.
 *
 *    Note: folder_type is either null or a zero-length string, so I'm assuming
 *    it's stupid and leaving it out.
 *
 *    Values: {
 *      url,
 *      title,
 *      tags, // list of strings if present, null if no tags
 *      keyword,
 *      type,
 *      position,
 *      dateAdded,
 *      lastModified,
 *      guid
 *    }
 *
 * - 'b', Reverse bookmark mappings. Composite key: [url, id]
 *   Value: { parentId: parentId }
 *
 * - 'K', Bookmark (search in the url bar) keywords. Composite key: [keyword]
 *   Value: { key: url }
 *
 * - 'T', Bookmarked URLs by tag. Composite key: [tag, url]
 *
 *   Value: null (this just lets us then look up the url)
 *
 * - 'H', History by time.  Single key: [zero-padded inverted timestamp with
 *    uniqueifying-value appended if needed]
 *    Values: {
 *      url,
 *      prevKey: key of the visit that led to us,
 *      type,
 *      session
 *    }
 *
 * - 'h', History by place.  Composite key: [reversed-host, zero-padded
 *    inverted timestamp with uniqueifying-value appended if needed, url]
 *
 *    This approximates, moz_historyvisits_placedateindex, but note that that
 *    is an index on (place_id, visit_date).  I'm assuming that index is
 *    actually for frecency recalculation.  I'll make an assertion that this
 *    might be an interesting optimized index to have and that frecency
 *    calculation could potentially benefit from allowing a halo effect of
 *    accessing other webpages on the same site.  The argument falls down for
 *    domains that host a ridiculous amount of useful content, but in that
 *    case I... LOOK! OVER THERE!
 *
 *    Value: null (the data is boring; we can get it from the other entry)
 *
 * - 'I', Info: Composite key: [reversed-host, url]
 *   Values: {
 *     title,
 *     visitCount,
 *     typed,
 *     frecency,
 *     lastVisitDate, // since frecency will get a boost anyways...
 *     guid,
 *     favicon: { // OPTIONAL; present if not the same as the site root
 *       url,
 *       blob,
 *       mimeType,
 *       expiration,
 *     },
 *     annotations: {
 *       annotationName: {
 *         mimeType,
 *         content,
 *         flags,
 *         expiration, // so, an index wouldn't be horrible for this if the
 *                     // storage is really a concern...
 *         type,
 *         dateAdded,
 *         lastModified
 *       }
 *     }
 *   }
 *
 *
 * - 'A', Awesomebar: Composite key: [magic, zero padded inverted frecency,
 *   term, reversed-host, path]
 *
 *   My understand of the awesomebar ranking algorithm is that it cares
 *   about things in this order:
 *   * What you picked the last time you typed the substring you've typed so
 *     far.
 *   * The frecency of the URLs associated with the matching term that you've
 *     typed so far.
 *
 *   The following probably could potentially matter
 *   * Whether you've typed all of the term we can match on.  It seems like it
 *     would be rude
 *
 *   Our magic is then:
 *   * For a given term and frecency of its owner, we broadly bin the frecency;
 *     the more "frecent" the value, the more prefixes of it we generate.  So
 *     If I go to one "food" website VERY frequently, we'd generate [f, fo, foo]
 *     in addition to the data for the term "food".  But for a site I visit
 *     less frequently, we might only add "foo".
 *   * For a given input value a la moz_inputhistory, we convert the use count
 *     to a frecency so we can encode it.  We generate a row using that typed
 *     value, plus we also generate a row with the last letter of the input
 *     removed at half the frecency *if we will not already generate an entry
 *     for that string*.
 *
 *   The presence of the zero padded inverted frecency as the second component
 *   of our key lets us avoid having to fetch all rows associated with the
 *   first key component.
 *
 *   We search on [what the user typed], potentially with a limit.
 *
 *   Value: url
 *
 * - 'a', Awesomebar Input History: [typed, url]
 *   Value: { useCount }
 *
 **/
var $sqlite = require('sqlite3');
var $levelup = require('levelup');
var $Q = require('q');
var $url = require('url');

var DEBUG = false;

var ZEROES = '0000000000000000000000000000000';
var NINES =  '9999999999999999999999999999999';
/**
 * Invert and zero-pad a number so we have a lexicographic ordering over
 * numbers where larger numbers come lexically before smaller numbers.
 * You need to tell us the range, etc.
 *
 * For efficiency, it would be better to encode these in base64 or do
 * something binary, but for an example, this isn't horrible.
 */
function invertAndPadNumber(val, max, digits) {
  var unpadded = (max - val).toString();
  return ZEROES.substring(0, digits - unpadded.length) + unpadded;
}

var OLDEST_LEGAL_DATE = Date.UTC(2000, 0, 1),
    MOST_FUTURE_LEGAL_DATE = Date.UTC(2031, 0, 1),
    // sure, we could take the log10, but that's too fancy
    DATE_DIGITS =
      (MOST_FUTURE_LEGAL_DATE - OLDEST_LEGAL_DATE).toString().length;

/**
 * Invert and pad timestamps; allows only legal values between arbitrarily
 * chosen date boundaries.
 */
function lexiformTimestamp(val) {
  return invertAndPadNumber(val - OLDEST_LEGAL_DATE,
                            MOST_FUTURE_LEGAL_DATE,
                            DATE_DIGITS);
}

function openPlaces() {
  var deferred = $Q.defer();

  var db = new $sqlite.Database('places.sqlite', function(err) {
    if (err) {
      console.error('SQLite badness:', err);
      deferred.reject(err);
      return;
    }

    console.log('places SQLite DB opened');
    deferred.resolve(db);
  });
  db.on('error', function(err) {
    console.error('Database error:', err);
  });
  if (DEBUG) {
    db.on('trace', function(data) {
      console.log('sqlite:', data);
    });
  }

  return deferred.promise;
}

function openLevelStore() {
  var deferred = $Q.defer();
  $levelup(
    'leveldb-places.ldb',
    {
      createIfMissing: true,
      errorIfExists: true,
      compression: true,
      cacheSize: 8 * 1024 * 1024, // the default,
      keyEncoding: 'utf8',
      valueEncoding: 'json'
    },
    function(err, db) {
      if (err) {
        console.error('LevelDB open error:', err);
        deferred.reject(err);
        return;
      }

      console.log('leveldb output opened');
      deferred.resolve(db);
    });

  return deferred.promise;
}

function slurpAnnotationAttribs(ctx) {
  console.log('slurping annotation attributes');
  var deferred = $Q.defer();
  var annoAttribsById = ctx.annoAttribsById = {};
  ctx.sdb.each(
    'SELECT * FROM moz_anno_attributes',
    function row(err, row) {
      console.log('row!!!!');
      annoAttribsById[row.id] = row.name;
    },
    function complete(err) {
      console.log('slurped annotation attributes');
      if (err)
        deferred.reject(err);
      else
        deferred.resolve();
    });
  return deferred.promise;
}

function slurpAnnotations(ctx) {
  console.log('slurping annotations');
  var deferred = $Q.defer();
  var annotationsByPlaceId = ctx.annotationsByPlaceId = {},
      annoAttribsById = ctx.annoAttribsById;
  ctx.sdb.each(
    'SELECT * FROM moz_annos',
    function row(err, row) {
      var annos = ctx.annotationsByPlaceId[row.place_id];
      if (!annos)
        annos = ctx.annotationsByPlaceId[row.place_id] = {};
      annos[annoAttribsById[row.anno_attribute_id]] = {
        mimeType: row.mime_type,
        content: row.content,
        flags: row.flags,
        expiration: row.expiration,
        type: row.type,
        dateAdded: row.dateAdded,
        lastModified: row.lastModified
      };
    },
    function complete(err) {
      console.log('slurped annotations');
      if (err)
        deferred.reject(err);
      else
        deferred.resolve();
    });
  return deferred.promise;
}

function slurpBookmarkAnnotations(ctx) {
  console.log('slurping bookmark annotations');
  var deferred = $Q.defer();
  var annotationsByBookmarkId = ctx.annotationsByBookmarkId = {},
      annoAttribsById = ctx.annoAttribsById;
  ctx.sdb.each(
    'SELECT * FROM moz_items_annos',
    function row(err, row) {
      var annos = ctx.annotationsByPlaceId[row.item_id];
      if (!annos)
        annos = ctx.annotationsByBookmarkId[row.item_id] = {};
      annos[annoAttribsById[row.anno_attribute_id]] = {
        mimeType: row.mime_type,
        content: row.content,
        flags: row.flags,
        expiration: row.expiration,
        type: row.type,
        dateAdded: row.dateAdded,
        lastModified: row.lastModified
      };
    },
    function complete(err) {
      console.log('slurped bookmark annotations');
      if (err)
        deferred.reject(err);
      else
        deferred.resolve();
    });
  return deferred.promise;
}

function slurpInputHistory(ctx) {
  console.log('slurping input history');
  var deferred = $Q.defer();
  var inputHistoryByPlaceId = ctx.inputHistoryByPlaceId = {};
  ctx.sdb.each(
    'SELECT * FROM moz_inputhistory',
    function row(err, row) {
      var inputs = inputHistoryByPlaceId[row.place_id];
      if (!inputs)
        inputs = inputHistoryByPlaceId[row.place_id] = {};
      inputs[row.input] = {
        useCount: row.use_count
      };
    },
    function complete(err) {
      console.log('slurped input history');
      if (err)
        deferred.reject(err);
      else
        deferred.resolve();
    });
  return deferred.promise;
}


function slurpKeywords(ctx) {
  console.log('slurping keywords');
  var deferred = $Q.defer();
  var keywordsById = ctx.keywordsById = {};
  ctx.sdb.each(
    'SELECT * FROM moz_keywords',
    function row(err, row) {
      keywordsById[row.id] = row.name;
    },
    function complete(err) {
      console.log('slurped keywords');
      if (err)
        deferred.reject(err);
      else
        deferred.resolve();
    });
  return deferred.promise;
}

function slurpFavicons(ctx) {
  console.log('slurping favicons');
  var deferred = $Q.defer();
  var faviconsById = ctx.faviconsById = {};
  ctx.sdb.each(
    'SELECT * FROM moz_favicons',
    function row(err, row) {
      faviconsById[row.id] = {
        url: row.url,
        data: row.data,
        mimeType: row.mime_type,
        expiration: row.expiration,
        // dropping guid because we don't need it
      };
    },
    function complete(err) {
      console.log('slurped favicons');
      if (err)
        deferred.reject(err);
      else
        deferred.resolve();
    });
  return deferred.promise;
}

/**
 * These depend on the place id.  There are more of these than the places,
 * but smaller.  Which is why we're slurping these.
 */
function slurpHistoryVisits(ctx) {
  console.log('slurping history visits');
  var deferred = $Q.defer();
  var visitsByPlaceId = ctx.visitsByPlaceId = {};
  var keyByVisitId = {};
  var lastTS = 0, nextUnique = 0;
  ctx.sdb.each(
    // Order by the visit-date so we can generate uniqueifying values if the
    // same time-stamp comes up multiple times and so we can (because of
    // causality) generate the prevKey
    'SELECT * FROM moz_historyvisits ORDER BY visit_date ASC',
    function row(err, row) {
      var visits = visitsByPlaceId[row.place_id];

      var key = lexiformTimestamp(row.visit_date);
      if (lastTS === row.visit_date) {
        key += '-' + nextUnique++;
      }
      else {
        lastTS = row.visit_date;
        nextUnique = 0;
      }
      keyByVisitId[row.id] = key;

      var visitInfo = {
        key: key,
        prevKey: keyByVisitId[row.from_visit] || null,
        type: row.visit_type,
        session: row.session
      };
    },
    function complete(err) {
      console.log('slurped history visits');
      if (err)
        deferred.reject(err);
      else
        deferred.resolve();
    });
  return deferred.promise;
}


function slurpBookmarkRoots(ctx) {
  console.log('slurping bookmark roots');
  var deferred = $Q.defer();
  var bookmarkRoots = ctx.bookmarkRoots = {};
  ctx.sdb.each(
    'SELECT * FROM moz_bookmarks_roots',
    function row(err, row) {
      bookmarkRoots[row.root_name] = row.folder_id;
    },
    function complete(err) {
      console.log('slurped bookmark roots');
      if (err)
        deferred.reject(err);
      else
        deferred.resolve();
    });
  return deferred.promise;
}

const TYPE_BOOKMARK = 1,
      TYPE_FOLDER = 2,
      TYPE_SEPARATOR = 3,
      TYPE_DYNAMIC_CONTAINER = 4;

/**
 * We need the info on the referenced place in order to write out the
 * denormalized bookmark, so this is technically a slurping.
 *
 * Extra processing:
 * - Tags!  Tags are currently implemented by having bookmark "folders" under
 *   the 'tags' root which in turn are the parents of bookmarks that reference
 *   the places id.  (So there is no direct link between a bookmark and its
 *   tags; it's indirect through the places DB.)  We
 */
function slurpBookmarks(ctx) {
  console.log('slurping bookmarks');
  var deferred = $Q.defer();
  var annotationsByBookmarkId = ctx.annotationsByBookmarkId;
  var bookmarkHierarchy = ctx.bookmarkHierarchy = {};
  var bookmarksByPlaceId = ctx.bookmarksByPlaceId = {};
  var bookmarksById = {};
  var flatBookmarks = [];
  var keywordsById = ctx.keywordsById;
  var tagsByPlaceId = ctx.tagsByPlaceId = {};

  ctx.sdb.each(
    'SELECT * FROM moz_bookmarks',
    function row(err, row) {
      var hierInfo = {
        id: row.id,
        placeId: row.fk, // I am sending bad karma at you, maker of this choice!
        parentId: row.parent,
        position: row.position,
        title: row.title,
        keyword: row.keyword ? ctx.keywordsById[row.keyword] : null,
        dateAdded: row.dateAdded,
        lastModified: row.lastModified,
        guid: row.guid,
        annotations: annotationsByBookmarkId[row.id] || null,
        tags: null,
        kids: null,
      };

      flatBookmarks.push(hierInfo);
      bookmarksById[hierInfo.id] = hierInfo;
      // Since bookmarks are hierarchically organized, there can be more than
      // one per single url.
      var linkedBookmarks = bookmarksByPlaceId[hierInfo.placeId];
      if (!linkedBookmarks)
        linkedBookmarks = [];
       linkedBookmarks.push(hierInfo);
    },
    function complete(err) {
      if (err) {
        deferred.reject(err);
        return;
      }

      // -- establish our hierarchy!
      // - link roots
      var TAG_ROOT_ID = ctx.bookmarkRoots['tags'];
      for (var rootName in ctx.bookmarkRoots) {
        var rootId = ctx.bookmarkRoots[rootName];
        var rootBookmark = bookmarksById[rootId];
        bookmarkHierarchy[rootName] = rootBookmark;
        // propagate the title...
        rootBookmark.title = rootName;
      }

      // - link children to their parents
      flatBookmarks.forEach(function(hierInfo) {
        var parentHierInfo = bookmarksById[hierInfo.parentId];

        // Propagate tags!
        if (hierInfo.parentId === TAG_ROOT_ID) {
          var tags = tagsByPlaceId[hierInfo.placeId];
          if (!tags)
            tags = tagsByPlaceId[hierInfo.placeId] = [];
          tags.push(parentHierInfo.title);
        }
        // Not a tag!
        else if (parentHierInfo){
          if (!parentHierInfo.kids)
            parentHierInfo.kids = [];
          parentHierInfo.kids.push(hierInfo);
        }
      });

      console.log('slurped bookmarks');
      deferred.resolve();
    });

  return deferred.promise;
};

/**
 * Write out the bookmarks now that they've been normalized.
 */
function writeBookmarks(ctx) {
  console.log('writing bookmarks');
  var deferred = $Q.defer();
  var batch = ctx.ldb.batch();

  function traverseBookmark(depth, bookmark) {
    // - bookmark proper
    batch.put(
      'B\0' + depth + '\0' + bookmark.parentId + '\0' + bookmark.id,
      {
        url: bookmark.url,
        title: bookmark.title,
        tags: bookmark.tags,
        keyword: bookmark.keyword,
        type: bookmark.type,
        position: bookmark.position,
        dateAdded: bookmark.dateAdded,
        lastModified: bookmark.lastModified,
        guid: bookmark.guid
      });

    batch.put(
      'b\0' + bookmark.url + '\0' + bookmark.id,
      { parentId: bookmark.parentId });

    // - tags
    if (bookmark.tags) {
      bookmark.tags.forEach(function(tag) {
        batch.put('T\0' + tag + '\0' + bookmark.url, null);
      });
    }

    // - keyword
    if (bookmark.keyword) {
      batch.put('K\0' + bookmark.keyword, bookmark.url);
    }

    // - recurse into kids
    if (bookmark.kids) {
      bookmark.kids.forEach(
        traverseBookmark.bind(null, depth + 1));
    }
  }

  for (var rootName in ctx.bookmarkHierarchy) {
    // we are normalizing out tags, don't process!
    if (rootName === 'tags')
      continue;

    var rootBookmark = ctx.bookmarkHierarchy[rootName];
    traverseBookmark(0, rootBookmark);
  }

  batch.write(function() {
    console.log('wrote bookmarks');
    deferred.resolve();
  });
  return deferred.promise();
}

var BATCH_LIMIT = 1000;

/**
 * Extract reasonable searchable terms
 */
function extractTermsForPlace(url, title, bookmarks, tags) {
  var terms = [];
  function maybeAddTerm(term) {
    term = term.toLowerCase();
    if (term.length < 3)
      return;
    // stop-words for babies
    switch (term) {
      case 'the':
      case 'www':
      case 'com':
      case 'org':
      case 'net':
        return;
    }
    if (terms.indexOf(term) !== -1)
      terms.push(term);
  }

  if (url && url.hostname)
    url.hostname.split('.').slice(0, -1).forEach(maybeAddTerm);
  if (title)
    title.split(/\W+/g).forEach(maybeAddTerm);
  bookmarks.forEach(function(bookmark) {
    bookmarks.title.split(/\W+/g).forEach(maybeAddTerm);
  });
  tags.forEach(maybeAddTerm);
  // note: we don't add the keyword as a term because that would defeat the
  // point of the keyword.

  return terms;
}

/**
 * What's the shortest prefix we should emit for a given frecency and a
 * given term length?
 *
 * Since this is not really the point of the prototype, the answer is
 * that we emit *ALL* the prefixes if the frecency is above 10,000.
 */
function lowestPrefixToEmitGivenFrecency(frecency, length) {
  if (frecency > 10000)
    return 1;
  return length;
}

var MAX_FRECENCY = 1000000;
var FRECENCY_DIGITS = 6;

function transformPlaceRecords(ctx) {
  var deferred = $Q.defer();
  var batch = ctx.ldb.batch(), batchCount = 0;
  var annotationsByPlaceId = ctx.annotationsByPlaceId;
  var bookmarksByPlaceId = ctx.bookmarksByPlaceId;
  var tagsByPlaceId = ctx.tagsByPlaceId;
  var faviconsById = ctx.faviconsById;

  var faviconPersistedForReverseHost = {};

  var emptyArray = [];

  ctx.sdb.each(
    // Because of our favicon normalization logic, we want to traverse URLs
    // within an origin from shortest to longer
    'SELECT * FROM moz_places ORDER BY url',
    function row(err, row) {
      var url = row.url,
          reversedHost = row.rev_host,
          placeId = row.id;

      var infoValue = {
        title: row.title,
        visitCount: row.visit_count,
        typed: row.typed,
        frecency: row.frecency,
        lastVisitDate: row.last_visit_date,
        guid: row.guid,
        favicon: null,
        annotations: annotationsByPlaceId[row.id] || null,
      };

      if (!faviconPersistedForReverseHost[reversedHost]) {
        infoValue.favicon = faviconsById[row.favicon_id];
        faviconPersistedForReverseHost[reversedHost] = true;
      }

      batch.put('I\0' + reversedHost + '\0' + url, infoValue);

      // -- fix-up bookmarks
      var linkedBookmarks = bookmarksByPlaceId[placeId] || emptyArray;
      linkedBookmarks.forEach(function(bookmark) {
        bookmark.url = url;
        bookmark.tags = tagsByPlaceId[placeId] || null;
      });

      // -- history visits
      var visits = ctx.visitsByPlaceId[placeId] || emptyArray;
      visits.forEach(function(visit) {
        batch.put(
          'H\0' + visit.key,
          {
            url: url,
            prevKey: visit.prevKey,
            type: visit.type,
            session: visit.session,
          });
        batch.put(
          'h\0' + reversedHost + '\0' + url,
          null);
      });

      // -- emit awesomebar stuff
      var tags = tagsByPlaceId[placeId] || emptyArray;
      var parsedUrl = $url.parse(url);
      var terms =
            extractTermsForPlace(parsedUrl, row.title, linkedBookmarks, tags);
      function emitAwesome(magic, term) {
        var awesomeKey =
              'A\0' +
              magic + '\0' +
              invertAndPadNumber(row.frecency, MAX_FRECENCY, FRECENCY_DIGITS) +
              '\0' +
              term + '\0' +
              reversedHost + '\0' +
              parsedUrl.pathname;
        batch.put(awesomeKey, url);
      }
      var awesomeCommon =
      terms.forEach(function(term) {
        var magic = term;
        var lowestPrefix = lowestPrefixToEmitGivenFrecency(row.frecency);
        while (magic.length >= lowestPrefix) {
          emitAwesome(magic, term);
          magic = magic.slice(0, -1);
        }
      });

      // - input history
      var inputs = ctx.inputHistoryByPlaceId[placeId];
      for (var typed in inputs) {
        var countObj = inputs[typed];
        batch.put('a\0' + typed + '\0' + url, countObj);
        emitAwesome(typed, typed);
        if (typed.length >= 2) {
          var typedPrefix = typed.slice(0, -1);
          if (!inputs.hasOwnProperty(typedPrefix))
            emitAwesome(typedPrefix, typed);
        }
      }

      if (batchCount++ >= BATCH_LIMIT) {
        batch.write(function() {
          console.log('transformed', BATCH_LIMIT, 'places');
        });
        batchCount = 0;
        batch = ctx.ldb.batch();
      }
    },
    function complete(err) {
      batch.write(function() {
        console.log('transformed', batchCount, 'places');
        console.log('wrote places');
        if (err)
          deferred.reject(err);
        else
          deferred.resolve();
      });
    });

  return deferred.promise;
};


function kickoffConversions(results) {
  var sqlDb = results[0], levelDb = results[1];
  var context = {
    sdb: sqlDb,
    ldb: levelDb,

    /** Maps id to name */
    annoAttribsById: null,
    /** Maps place id to object dict of anno name to anno value obj */
    annotationsByPlaceId: null,
    /** Maps bookmark id to object dict of anno name to anno value obj */
    annotationsByBookmarkId: null,
    /** Maps place id to input history obj */
    inputHistoryByPlaceId: null,
    /** Maps keyword ids to their string values */
    keywordsById: null,

    bookmarkHierarchy: null,
    bookmarksByPlaceId: null,
    tagsByPlaceId: null,

    visitsByPlaceId: null,

    faviconsById: null,
  };

  var theGreatPromiseChain  =
    slurpAnnotationAttribs(context)
    .then(slurpAnnotations.bind(null, context))
    .then(slurpBookmarkAnnotations.bind(null, context))
    .then(slurpInputHistory.bind(null, context))
    .then(slurpKeywords.bind(null, context))
    .then(slurpFavicons.bind(null, context))
    .then(slurpHistoryVisits.bind(null, context))
    .then(slurpBookmarkRoots.bind(null, context))
    .then(slurpBookmarks.bind(null, context))
    .then(transformPlaceRecords.bind(null, context))
    .then(allDone.bind(null, context))
    .fail(fatalError);

  return theGreatPromiseChain;
}

function allDone(ctx) {
  console.log('All done!');
  ctx.sdb.close();
  ctx.ldb.close();
}

function fatalError(err) {
  console.error('The following fatal thing has ended us:', err);
}

$Q.all([openPlaces(),
        openLevelStore()])
  .then(kickoffConversions, fatalError);

// So, sqlite and leveldb are failing to keep the event loop alive.  Which is
// awkward.
function uhKeepEventLoopGoing() {
  var $timers = require('timers');
  $timers.setInterval(function() {}, 1000);
}
//uhKeepEventLoopGoing();
